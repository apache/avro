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
(defpackage #:org.apache.avro/test/ipc/stateful/server
  (:use #:cl)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/ipc/common
                #:flatten)
  (:export #:service-endpoint
           #:client-connected-p
           #:*one-way*
           #:*metadata*))
(in-package #:org.apache.avro/test/ipc/stateful/server)

(defclass |Greeting| ()
  ((|message| :type avro:string :initarg :message))
  (:metaclass avro:record)
  (:enclosing-namespace "com.acme"))

(avro:define-error |Curse|
    ((|message| :type avro:string :initarg :message)))

(defclass |HelloWorld| ()
  ()
  (:metaclass avro:protocol)
  (:documentation "Protocol Greetings")
  (:namespace "com.acme")
  (:types
   |Greeting|
   |Curse|)
  (:messages
   (:name |hello|
    :documentation "Say hello."
    :request ((:name #:|greeting| :type |Greeting|))
    :response |Greeting|
    :errors (|Curse|))
   (:name one-way
    :request ((:name #:foo :type avro:string))
    :response avro:null
    :one-way t)))

(defclass server (avro:server)
  ((cache
    :initarg :cache
    :type hash-table)
   (connected-clients
    :initarg :connected-clients
    :type hash-table
    :reader connected-clients))
  (:default-initargs
   :cache (make-hash-table :test #'equalp)
   :connected-clients (make-hash-table :test #'eql)))

(defmethod avro:client-protocol
    ((server server) client-hash)
  (with-slots (cache) server
    (gethash client-hash cache)))

(defmethod (setf avro:client-protocol)
    (client-protocol (server server) client-hash)
  (with-slots (cache) server
    (setf (gethash client-hash cache) client-protocol)))

(defparameter +server+ (make-instance 'server))

(defparameter +service+
  (make-instance '|HelloWorld| :transceiver +server+))

(defparameter *metadata* nil)

(defmethod |hello| ((greeting |Greeting|) &optional metadata)
  (check-type metadata avro:map<bytes>)
  (setf *metadata* metadata)
  (let ((message (slot-value greeting '|message|)))
    (cond
      ((string= message "foo")
       (error '|Curse| :message "throw foo"))
      ((string= message "bar") (error "meow"))
      ((string= message "baz")
       (values (make-instance '|Greeting| :message "baz with metadata")
               (let ((map (make-instance 'avro:map<bytes>)))
                 (setf (avro:gethash "baz" map)
                       (babel:string-to-octets message :encoding :utf-8))
                 map)))
      (t (make-instance
          '|Greeting| :message (format nil "Hello ~A!" message))))))

(declaim
 (ftype (function (fixnum org.apache.avro.internal.ipc.framing:buffers)
                  (values (or null
                              (simple-array (unsigned-byte 8) (*))) &optional))
        service-endpoint))
(defun service-endpoint (client-id request)
  (let* ((request (flatten request))
         (client-protocol (gethash client-id (connected-clients +server+)))
         (response
           (if client-protocol
               (avro:receive-from-connected-client
                +service+ request client-protocol)
               (multiple-value-bind (response client-protocol)
                   (avro:receive-from-unconnected-client +service+ request)
                 (setf (gethash client-id (connected-clients +server+))
                       client-protocol)
                 response))))
    (when response
      (flatten response))))

(declaim
 (ftype (function (fixnum) (values boolean &optional)) client-connected-p))
(defun client-connected-p (client-id)
  (not (null (gethash client-id (connected-clients +server+)))))

(defparameter *one-way* nil)

(defmethod one-way ((string string) &optional metadata)
  (check-type metadata avro:map<bytes>)
  (setf *one-way* string
        *metadata* metadata))

(defpackage #:org.apache.avro/test/ipc/stateful/client
  (:use #:cl)
  (:local-nicknames
   (#:avro #:org.apache.avro)
   (#:server #:org.apache.avro/test/ipc/stateful/server))
  (:export #:|Greeting|
           #:|Curse|
           #:|message|
           #:|hello|
           #:reset
           #:id
           #:sent-handshake-p
           #:one-way))
(in-package #:org.apache.avro/test/ipc/stateful/client)

(defclass |Greeting| ()
  ((|message| :type avro:string :initarg :message))
  (:metaclass avro:record)
  (:enclosing-namespace "com.acme"))

(avro:define-error |Curse|
    ((|message| :type avro:string :reader |message|)))

(defclass |HelloWorld| ()
  ()
  (:metaclass avro:protocol)
  (:documentation "Protocol Greetings")
  (:namespace "com.acme")
  (:types
   |Greeting|
   |Curse|)
  (:messages
   (:name |hello|
    :documentation "Say hello."
    :request ((:name #:|greeting| :type |Greeting|))
    :response |Greeting|
    :errors (|Curse|))
   (:name one-way
    :request ((:name #:foo :type avro:string))
    :response avro:null
    :one-way t)))

(defclass client (avro:stateful-client)
  ((sent-handshake-p
    :type boolean
    :accessor %sent-handshake-p
    :initform nil)
   (client-id
    :type fixnum
    :accessor client-id
    :initform (random most-positive-fixnum))))

(defmethod avro:sent-handshake-p
    ((client client))
  (%sent-handshake-p client))

(defmethod (setf avro:sent-handshake-p)
    ((boolean symbol) (client client))
  (setf (%sent-handshake-p client) boolean))

(defmethod avro:send-and-receive
    ((client client) buffers)
  (server:service-endpoint (client-id client) buffers))

(defmethod avro:send
    ((client client) buffers)
  (server:service-endpoint (client-id client) buffers))

(defparameter +client+ (make-instance 'client))

(defparameter +service+
  (make-instance '|HelloWorld| :transceiver +client+))

(declaim (ftype (function () (values &optional)) reset))
(defun reset ()
  (setf (%sent-handshake-p +client+) nil
        (client-id +client+) (random most-positive-fixnum))
  (values))

(declaim (ftype (function () (values fixnum &optional)) id))
(defun id ()
  (client-id +client+))

(declaim (ftype (function () (values boolean &optional)) sent-handshake-p))
(defun sent-handshake-p ()
  (avro:sent-handshake-p +client+))

(defpackage #:org.apache.avro/test/ipc/stateful
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro)
   (#:client #:org.apache.avro/test/ipc/stateful/client)
   (#:server #:org.apache.avro/test/ipc/stateful/server)))
(in-package #:org.apache.avro/test/ipc/stateful)

(test greeting
  (setf server:*metadata* nil)
  (let ((request (make-instance 'client:|Greeting| :message "foobar")))
    (multiple-value-bind (response response-metadata)
        (client:|hello| request)
      (is (zerop (avro:hash-table-count server:*metadata*)))
      (is (zerop (avro:hash-table-count response-metadata)))
      (is (typep response 'client:|Greeting|))
      (is (string= "Hello foobar!" (slot-value response 'client:|message|))))))

(test greeting-metadata
  (setf server:*metadata* nil)
  (let ((request (make-instance 'client:|Greeting| :message "foobar"))
        (request-metadata (make-instance 'avro:map<bytes>)))
    (setf (avro:gethash "foo" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(2 4 6))
          (avro:gethash "bar" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(8 10 12)))
    (multiple-value-bind (response response-metadata)
        (client:|hello| request request-metadata)
      (is (equalp (avro:raw request-metadata) (avro:raw server:*metadata*)))
      (is (zerop (avro:hash-table-count response-metadata)))
      (is (typep response 'client:|Greeting|))
      (is (string= "Hello foobar!" (slot-value response 'client:|message|))))))

(test metadata-response
  (setf server:*metadata* nil)
  (let ((request (make-instance 'client:|Greeting| :message "baz"))
        (expected-response-metadata (make-instance 'avro:map<bytes>)))
    (setf (avro:gethash "baz" expected-response-metadata)
          (babel:string-to-octets "baz" :encoding :utf-8))
    (multiple-value-bind (response response-metadata)
        (client:|hello| request)
      (is (zerop (avro:hash-table-count server:*metadata*)))
      (is (equalp (avro:raw expected-response-metadata)
                  (avro:raw response-metadata)))
      (is (typep response 'client:|Greeting|))
      (is (string=
           "baz with metadata" (slot-value response 'client:|message|))))))

(test metadata-response-metadata
  (setf server:*metadata* nil)
  (let ((request (make-instance 'client:|Greeting| :message "baz"))
        (request-metadata (make-instance 'avro:map<bytes>))
        (expected-response-metadata (make-instance 'avro:map<bytes>)))
    (setf (avro:gethash "foo" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(2 4 6))
          (avro:gethash "bar" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(8 10 12))
          (avro:gethash "baz" expected-response-metadata)
          (babel:string-to-octets "baz" :encoding :utf-8))
    (multiple-value-bind (response response-metadata)
        (client:|hello| request request-metadata)
      (is (equalp (avro:raw request-metadata) (avro:raw server:*metadata*)))
      (is (equalp (avro:raw expected-response-metadata)
                  (avro:raw response-metadata)))
      (is (typep response 'client:|Greeting|))
      (is (string=
           "baz with metadata" (slot-value response 'client:|message|))))))

(test declared-error
  (setf server:*metadata* nil)
  (handler-case
      (let ((request (make-instance 'client:|Greeting| :message "foo")))
        (client:|hello| request))
    (client:|Curse| (curse)
      (is (zerop (avro:hash-table-count server:*metadata*)))
      (is (typep curse 'client:|Curse|))
      (is (string= "throw foo" (client:|message| curse))))))

(test declared-error-metadata
  (setf server:*metadata* nil)
  (let ((request (make-instance 'client:|Greeting| :message "foo"))
        (request-metadata (make-instance 'avro:map<bytes>)))
    (setf (avro:gethash "foo" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(2 4 6))
          (avro:gethash "bar" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(8 10 12)))
    (handler-case
        (client:|hello| request request-metadata)
      (client:|Curse| (curse)
        (is (equalp (avro:raw request-metadata) (avro:raw server:*metadata*)))
        (is (typep curse 'client:|Curse|))
        (is (string= "throw foo" (client:|message| curse)))))))

(test undeclared-error
  (setf server:*metadata* nil)
  (handler-case
      (let ((request (make-instance 'client:|Greeting| :message "bar")))
        (client:|hello| request))
    (avro:undeclared-rpc-error (condition)
      (is (zerop (avro:hash-table-count server:*metadata*)))
      (is (typep condition 'avro:undeclared-rpc-error))
      (is (string= "oh no, an error occurred" (avro:message condition))))))

(test undeclared-error-metadata
  (setf server:*metadata* nil)
  (let ((request (make-instance 'client:|Greeting| :message "bar"))
        (request-metadata (make-instance 'avro:map<bytes>)))
    (setf (avro:gethash "foo" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(2 4 6))
          (avro:gethash "bar" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(8 10 12)))
    (handler-case
        (client:|hello| request request-metadata)
      (avro:undeclared-rpc-error (condition)
        (is (equalp (avro:raw request-metadata) (avro:raw server:*metadata*)))
        (is (typep condition 'avro:undeclared-rpc-error))
        (is (string= "oh no, an error occurred" (avro:message condition)))))))

(test connection
  (client:reset)
  (is (not (client:sent-handshake-p)))
  (is (not (server:client-connected-p (client:id))))
  (greeting)
  (is (client:sent-handshake-p))
  (is (server:client-connected-p (client:id))))

(test one-way
  (setf server:*one-way* nil
        server:*metadata* nil)
  (let ((request "foobar"))
    (multiple-value-bind (response response-metadata)
        (client:one-way request)
      (is (zerop (avro:hash-table-count server:*metadata*)))
      (is (null response))
      (is (null response-metadata))
      (is (string= request server:*one-way*)))))

(test one-way-metadata
  (setf server:*one-way* nil
        server:*metadata* nil)
  (let ((request "foobar")
        (request-metadata (make-instance 'avro:map<bytes>)))
    (setf (avro:gethash "foo" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(2 4 6))
          (avro:gethash "bar" request-metadata)
          (make-array 3 :element-type '(unsigned-byte 8)
                        :initial-contents '(8 10 12)))
    (multiple-value-bind (response response-metadata)
        (client:one-way request request-metadata)
      (is (equalp (avro:raw request-metadata) (avro:raw server:*metadata*)))
      (is (null response))
      (is (null response-metadata))
      (is (string= request server:*one-way*)))))