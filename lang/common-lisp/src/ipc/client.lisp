;;; Licensed to the Apache Software Foundation (ASF) under one
;;; or more contributor license agreements.  See the NOTICE file
;;; distributed with this work for additional information
;;; regarding copyright ownership.  The ASF licenses this file
;;; to you under the Apache License, Version 2.0 (the
;;; "License"); you may not use this file except in compliance
;;; with the License.  You may obtain a copy of the License at
;;;
;;;   http://www.apache.org/licenses/LICENSE-2.0
;;;
;;; Unless required by applicable law or agreed to in writing,
;;; software distributed under the License is distributed on an
;;; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
;;; KIND, either express or implied.  See the License for the
;;; specific language governing permissions and limitations
;;; under the License.

(cl:in-package #:cl-user)
(defpackage #:org.apache.avro.internal.ipc.client
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:framing #:org.apache.avro.internal.ipc.framing)
   (#:record #:org.apache.avro.internal.record)))
(in-package #:org.apache.avro.internal.ipc.client)

;;; client

;; caching as slots requires each client to talk to only one server
(defclass internal:client ()
  ((server-hash
    :type internal:md5
    :accessor internal:server-hash)
   (server-protocol
    :type api:protocol
    :accessor internal:server-protocol)))

(defgeneric api:send (client buffers)
  (:documentation
   "Send a one-way message.

This is will be called with stateful-clients when sending a one-way
message over connections that have already performed a handshake."))

(defgeneric api:send-and-receive (client buffers)
  (:documentation
   "Send a message and receive a response.

This is called when sending two-way messages, or when a handshake
needs to be performed."))

(defgeneric api:sent-handshake-p (client)
  (:documentation
   "Determine if a stateful-client needs to perform a handshake."))

(defgeneric (setf api:sent-handshake-p) (boolean client)
  (:documentation
   "Called when a stateful-client performs a handshake, or fails to
perform one successfully."))

(defgeneric internal:add-methods (protocol client messages)
  (:method (protocol transceiver (messages null))
    (values))

  (:documentation
   "Add client methods to MESSAGES."))

;;; stateless-client

(defclass api:stateless-client (internal:client)
  ()
  (:documentation
   "Base class of stateless avro ipc clients."))

(defmethod internal:add-methods
    ((protocol api:protocol)
     (client api:stateless-client)
     (messages simple-array))
  (declare ((simple-array api:message (*)) messages))
  (flet ((add-stateless-method (message)
           (add-stateless-method protocol client message)))
    (map nil #'add-stateless-method messages))
  (values))

;;; stateful-client

(defclass api:stateful-client (internal:client)
  ()
  (:documentation
   "Base class of stateful avro ipc clients."))

(defmethod internal:add-methods
    ((protocol api:protocol)
     (client api:stateful-client)
     (messages simple-array))
  (declare ((simple-array api:message (*)) messages))
  (flet ((add-stateful-method (message)
           (add-stateful-method protocol client message)))
    (map nil #'add-stateful-method messages))
  (values))

;;; add-stateless-method

(declaim
 (ftype (function (api:protocol api:stateless-client api:message)
                  (values &optional))
        add-stateless-method))
(defun add-stateless-method (protocol client message)
  (let ((message-name (symbol-name (closer-mop:generic-function-name message)))
        (lambda-list (closer-mop:generic-function-lambda-list message))
        (request (api:request message))
        (response (api:response message)))
    (multiple-value-bind (conditions errors-union)
        (api:errors message)
      (let* ((lambda-list-with-default
               (nconc (butlast lambda-list)
                      (list '(api:metadata
                              (make-instance 'api:map<bytes>)))))
             (lambda-list-without-optional
               (nbutlast (butlast lambda-list)))
             (body
               `(lambda ,lambda-list-with-default
                  (declare (api:map<bytes> api:metadata))
                  (let* ((metadata api:metadata)
                         (parameters (parse-parameters
                                      ,request
                                      (list ,@lambda-list-without-optional)))
                         (response-stream
                           (perform-handshake
                            ,protocol ,client ,message-name
                            parameters metadata)))
                    ,(if (api:one-way message)
                         `(declare (ignore response-stream))
                         `(process-response
                           response-stream
                           (find-server-message ,message-name ,client)
                           ,response
                           ,conditions
                           ,errors-union)))))
             (method-lambda
               (closer-mop:make-method-lambda
                message (closer-mop:class-prototype
                         (find-class 'standard-method))
                body nil))
             (documentation
               "Some auto-generated documentation would be nice.")
             (specializers
               (map 'list
                    (lambda (field)
                      (let ((schema (api:type field)))
                        (if (symbolp schema)
                            (primitive->class schema)
                            schema)))
                    (api:fields request)))
             (method
               (make-instance
                'standard-method
                :lambda-list lambda-list-with-default
                :specializers specializers
                :function (compile nil method-lambda)
                :documentation documentation)))
        (add-method message method))))
  (values))

;;; add-stateful-method

(declaim
 (ftype (function (api:protocol api:stateful-client api:message)
                  (values &optional))
        add-stateful-method))
(defun add-stateful-method (protocol client message)
  (let ((message-name (symbol-name (closer-mop:generic-function-name message)))
        (lambda-list (closer-mop:generic-function-lambda-list message))
        (request (api:request message))
        (response (api:response message)))
    (multiple-value-bind (conditions errors-union)
        (api:errors message)
      (let* ((lambda-list-with-default
               (nconc (butlast lambda-list)
                      (list '(api:metadata
                              (make-instance 'api:map<bytes>)))))
             (lambda-list-without-optional
               (nbutlast (butlast lambda-list)))
             (body
               `(lambda ,lambda-list-with-default
                  (declare (api:map<bytes> api:metadata))
                  (let* ((metadata api:metadata)
                         (parameters (parse-parameters
                                      ,request
                                      (list ,@lambda-list-without-optional)))
                         ,@(unless (api:one-way message)
                             `((response-stream
                                (if (api:sent-handshake-p ,client)
                                    (framing:to-input-stream
                                     (api:send-and-receive
                                      ,client (framing:frame
                                               metadata
                                               ,message-name parameters)))
                                    (prog1 (perform-handshake
                                            ,protocol ,client ,message-name
                                            parameters metadata)
                                      (setf (api:sent-handshake-p ,client)
                                            t)))))))
                    ,@(if (api:one-way message)
                          `((if (api:sent-handshake-p ,client)
                                (api:send
                                 ,client (framing:frame
                                          metadata ,message-name parameters))
                                (progn
                                  (perform-handshake
                                   ,protocol ,client ,message-name
                                   parameters metadata)
                                  (setf (api:sent-handshake-p ,client) t)))
                            nil)
                          `((process-response
                             response-stream
                             (find-server-message ,message-name ,client)
                             ,response
                             ,conditions
                             ,errors-union))))))
             (method-lambda
               (closer-mop:make-method-lambda
                message (closer-mop:class-prototype
                         (find-class 'standard-method))
                body nil))
             (documentation
               "Some auto-generated documentation would be nice.")
             (specializers
               (map 'list
                    (lambda (field)
                      (let ((schema (api:type field)))
                        (if (symbolp schema)
                            (primitive->class schema)
                            schema)))
                    (api:fields request)))
             (method
               (make-instance
                'standard-method
                :lambda-list lambda-list-with-default
                :specializers specializers
                :function (compile nil method-lambda)
                :documentation documentation)))
        (add-method message method))))
  (values))

;;; primitive->class

(declaim (ftype (function (symbol) (values class &optional)) primitive->class))
(defun primitive->class (schema)
  (find-class
   (ecase schema
     (api:null 'null)
     (api:boolean 'symbol)   ; TODO this sucks, use eql api:true/false
     ((api:int api:long) 'integer)
     (api:float 'single-float)
     (api:double 'double-float)
     (api:bytes 'vector)
     (api:string 'string))))

;;; parse-parameters

(declaim
 (ftype (function (api:record list) (values api:record-object &optional))
        parse-parameters))
(defun parse-parameters (request lambda-list)
  (record:make-object
   request
   (api:fields request)
   (make-array (length lambda-list)
               :element-type 'api:object :initial-contents lambda-list)))

;;; find-server-message

(declaim
 (ftype (function (string internal:client) (values api:message &optional))
        find-server-message))
(defun find-server-message (message-name client)
  (let ((messages (api:messages (internal:server-protocol client))))
    (find message-name messages
          :test #'string= :key #'closer-mop:generic-function-name)))

;;; process-response

(deftype classes ()
  '(simple-array class (*)))

(deftype classes? ()
  '(or null classes))

(declaim
 (ftype (function
         (framing:input-stream api:message api:schema classes? api:union)
         (values api:object api:map<bytes> &optional))
        process-response))
(defun process-response
    (response-stream server-message response-schema conditions errors-union)
  (let ((metadata (api:deserialize 'api:map<bytes> response-stream))
        (errorp (eq 'api:true (api:deserialize 'api:boolean response-stream))))
    (if errorp
        (error (make-error
                conditions
                (api:coerce
                 (api:deserialize (nth-value 1 (api:errors server-message))
                                  response-stream)
                 errors-union)
                metadata))
        (values (api:coerce
                 (api:deserialize
                  (api:response server-message) response-stream)
                 response-schema)
                metadata))))

(declaim
 (ftype (function (classes? api:union-object api:map<bytes>)
                  (values api:rpc-error &optional))
        make-error))
(defun make-error (conditions error metadata)
  (let ((position (nth-value 1 (api:which-one error)))
        (object (api:object error)))
    (if (zerop position)                ; string is the first schema
        (make-condition
         'api:undeclared-rpc-error :message object :metadata metadata)
        (let ((condition-name (class-name (elt conditions (1- position)))))
          (internal:make-declared-rpc-error condition-name object metadata)))))

;;; perform-handshake

(defmacro handshake-match (handshake &body cases)
  (declare (symbol handshake))
  (let ((known-cases
          (map 'list #'intern (api:symbols (find-class 'internal:match)))))
    (map nil
         (lambda (case)
           (check-type case cons)
           (let ((position (position (first case) known-cases)))
             (assert position ()
                     "Unknown case ~S, expected one of ~S"
                     (first case) known-cases)
             (rplaca case position)))
         cases))
  `(ecase (nth-value 1 (api:which-one (internal:match ,handshake)))
     ,@cases))

(defmacro update-client-cache (client handshake checkp)
  (declare (symbol client handshake)
           (boolean checkp))
  (let* ((server-hash (gensym))
         (server-protocol-json (gensym))
         (server-protocol `(api:deserialize
                            `api:protocol ,server-protocol-json))
         (server-hash-accessor `(internal:server-hash ,client))
         (server-protocol-accessor `(internal:server-protocol ,client)))
    `(let ((,server-hash (api:object (internal:server-hash ,handshake)))
           (,server-protocol-json (api:object
                                   (internal:server-protocol ,handshake))))
       ,@(if checkp
             `((when ,server-hash
                 (setf ,server-hash-accessor ,server-hash))
               (when ,server-protocol-json
                 (setf ,server-protocol-accessor ,server-protocol)))
             `((setf ,server-hash-accessor ,server-hash
                     ,server-protocol-accessor ,server-protocol))))))

(declaim
 (ftype (function
         (api:protocol internal:client string api:record-object api:map<bytes>)
         (values framing:input-stream &optional))
        perform-handshake))
(defun perform-handshake (protocol client message-name parameters metadata)
  (let* ((request-handshake (initial-handshake protocol client))
         (buffers (framing:frame
                   request-handshake metadata message-name parameters))
         (response-stream (framing:to-input-stream
                           (api:send-and-receive client buffers)))
         (response-handshake (api:deserialize
                              'internal:response response-stream)))
    (handshake-match response-handshake
      (BOTH)
      (CLIENT
       (update-client-cache client response-handshake nil))
      (NONE
       (update-client-cache client response-handshake t)
       (setf (elt buffers 0) (framing:buffer-object
                              (subsequent-handshake protocol client))
             response-stream (framing:to-input-stream
                              (api:send-and-receive client buffers))
             response-handshake (api:deserialize
                                 'internal:response response-stream))
       (handshake-match response-handshake
         (BOTH))))
    response-stream))

(declaim
 (ftype (function (api:protocol internal:client)
                  (values internal:request &optional))
        initial-handshake))
(defun initial-handshake (protocol client)
  (make-instance
   'internal:request
   :client-hash (internal:md5 protocol)
   :server-hash (internal:server-hash client)))

(declaim
 (ftype (function (api:protocol internal:client)
                  (values internal:request &optional))
        subsequent-handshake))
(defun subsequent-handshake (protocol client)
  (make-instance
   'internal:request
   :client-hash (internal:md5 protocol)
   :client-protocol (make-instance 'internal:union<null-string>
                                   :object (api:serialize protocol))
   :server-hash (internal:server-hash client)))
