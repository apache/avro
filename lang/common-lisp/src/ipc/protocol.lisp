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
(defpackage #:org.apache.avro.internal.ipc.protocol
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:mop #:org.apache.avro.internal.mop)
   (#:name #:org.apache.avro.internal.name)
   (#:parse #:org.apache.avro.internal.ipc.parse)
   (#:intern #:org.apache.avro.internal.intern))
  (:import-from #:org.apache.avro.internal.record
                #:read-field))
(in-package #:org.apache.avro.internal.ipc.protocol)

;;; protocol

(defclass api:protocol (name:named-class)
  ((types
    :initarg :types
    :reader api:types
    :late-type parse:classes?
    :early-type parse:symbol/classes?
    :documentation "A vector of named types if provided, otherwise nil.")
   (messages
    :initarg :messages
    :reader api:messages
    :late-type parse:messages?
    :early-type parse:conses?
    :documentation "A vector of messages if provided, otherwise nil.")
   (md5
    :reader internal:md5
    :type internal:md5
    :documentation "MD5 hash of protocol."))
  (:metaclass mop:schema-class)
  (:object-class api:protocol-object)
  (:documentation
   "Metaclass of avro protocols."))

(defmethod closer-mop:validate-superclass
    ((class api:protocol) (superclass name:named-class))
  t)

(mop:definit
    ((instance api:protocol) :around
     &rest initargs &key (types nil typesp) (messages nil messagesp))
  (setf (getf initargs :types) (parse:early-types types typesp)
        (getf initargs :messages) (parse:early-messages messages messagesp))
  (apply #'call-next-method instance initargs))

(defmethod mop:early->late
    ((class api:protocol) (name (eql 'types)) type value)
  ;; this executes before the messages slot
  (with-slots (types) class
    (setf types (parse:late-types types))))

(defmethod mop:early->late
    ((class api:protocol) (name (eql 'messages)) type value)
  (with-slots (types messages) class
    (setf messages (parse:late-messages types messages))))

(defmethod closer-mop:finalize-inheritance :after
    ((instance api:protocol))
  (with-slots (md5) instance
    (let* ((json (api:serialize instance))
           (bytes (babel:string-to-octets json :encoding :utf-8))
           (checksum (md5:md5sum-sequence bytes)))
      (setf md5 (make-instance 'internal:md5 :initial-contents checksum)))))

;;; serialize

(defmethod api:serialize
    ((protocol api:protocol)
     &key
       (into (make-array
              0 :element-type 'character :adjustable t :fill-pointer t))
       (start 0)
       (canonical-form-p nil))
  (let ((jso-object (internal:write-jso
                     protocol (make-hash-table :test #'eq) canonical-form-p)))
    (internal:write-json jso-object into :start start)
    into))

(defmethod internal:write-jso
    ((protocol api:protocol) seen canonical-form-p)
  (flet ((write-jso (schema)
           (internal:write-jso schema seen canonical-form-p)))
    (let ((initargs (list "protocol" (nth-value 1 (api:name protocol))))
          (documentation (documentation protocol t))
          (types (api:types protocol))
          (messages (api:messages protocol)))
      (multiple-value-bind (deduced namespace namespacep)
          (api:namespace protocol)
        (declare (ignore deduced))
        (when namespacep
          (setf initargs (nconc initargs (list "namespace" namespace)))))
      (when documentation
        (setf initargs (nconc initargs (list "doc" documentation))))
      (when types
        (let ((types (map 'list #'write-jso types)))
          (setf initargs (nconc initargs (list "types" types)))))
      (when messages
        (loop
          for message across messages
          for name = (symbol-name (closer-mop:generic-function-name message))
          for jso = (write-jso message)

          collect name into message-initargs
          collect jso into message-initargs

          finally
             (let ((messages (apply #'st-json:jso message-initargs)))
               (setf initargs (nconc initargs (list "messages" messages))))))
      (apply #'st-json:jso initargs))))

;;; deserialize

(defmethod api:deserialize
    ((protocol (eql 'api:protocol)) (input stream) &key)
  (jso->protocol (st-json:read-json input t)))

(defmethod api:deserialize
    ((protocol (eql 'api:protocol)) (input string) &key (start 0))
  (jso->protocol
   (st-json:read-json-from-string input :start start :junk-allowed-p t)))

(declaim
 (ftype (function (st-json:jso) (values api:protocol &optional))
        jso->protocol))
(defun jso->protocol (jso)
  (let ((fullname->schema (make-hash-table :test #'equal))
        initargs name enclosing-namespace)
    (multiple-value-bind (protocol protocolp)
        (st-json:getjso "protocol" jso)
      (when protocolp
        (setf name protocol)
        (push protocol initargs)
        (push :name initargs)))
    (multiple-value-bind (namespace namespacep)
        (st-json:getjso "namespace" jso)
      (when namespacep
        (push namespace initargs)
        (push :namespace initargs))
      (setf enclosing-namespace (name:deduce-namespace name namespace nil)))
    (multiple-value-bind (doc docp)
        (st-json:getjso "doc" jso)
      (when docp
        (push doc initargs)
        (push :documentation initargs)))
    (multiple-value-bind (types typesp)
        (st-json:getjso "types" jso)
      (when typesp
        (push (parse-types types fullname->schema enclosing-namespace)
              initargs)
        (push :types initargs)))
    (multiple-value-bind (messages messagesp)
        (st-json:getjso "messages" jso)
      (when messagesp
        (push (parse-messages messages fullname->schema enclosing-namespace)
              initargs)
        (push :messages initargs)))
    (let ((protocol (apply #'make-instance 'api:protocol initargs)))
      (closer-mop:finalize-inheritance protocol)
      protocol)))

;;; parse-types

(deftype class/schema ()
  '(or class api:schema))

(deftype class/schemas ()
  '(simple-array class/schema (*)))

(declaim
 (ftype (function (t hash-table name:namespace)
                  (values class/schemas &optional))
        parse-types))
(defun parse-types (types fullname->schema enclosing-namespace)
  (flet ((read-jso (jso)
           (internal:read-jso jso fullname->schema enclosing-namespace)))
    (map 'class/schemas #'read-jso types)))

;;; parse-messages

(declaim
 (ftype (function (t hash-table name:namespace)
                  (values (vector cons) &optional))
        parse-messages))
(defun parse-messages (messages fullname->schema enclosing-namespace)
  (check-type messages st-json:jso)
  (let ((vector (make-array 0 :element-type 'cons :adjustable t
                              :fill-pointer t)))
    (flet ((fill-vector (key value)
             (let ((initargs (list :name (make-symbol key))))
               (multiple-value-bind (one-way one-way-p)
                   (st-json:getjso "one-way" value)
                 (when one-way-p
                   (push (eq one-way 'api:true) initargs)
                   (push :one-way initargs)))
               (multiple-value-bind (request requestp)
                   (st-json:getjso "request" value)
                 (when requestp
                   (push (parse-request
                          request fullname->schema enclosing-namespace)
                         initargs)
                   (push :request initargs)))
               (multiple-value-bind (response responsep)
                   (st-json:getjso "response" value)
                 (when responsep
                   (push (internal:read-jso
                          response fullname->schema enclosing-namespace)
                         initargs)
                   (push :response initargs)))
               (multiple-value-bind (errors errorsp)
                   (st-json:getjso "errors" value)
                 (when errorsp
                   (push (parse-errors
                          errors fullname->schema enclosing-namespace)
                         initargs)
                   (push :errors initargs)))
               (vector-push-extend initargs vector))))
      (st-json:mapjso #'fill-vector messages))
    vector))

(declaim
 (ftype (function (t hash-table name:namespace) (values list &optional))
        parse-request))
(defun parse-request (request fullname->schema enclosing-namespace)
  (flet ((read-field (request)
           (read-field request fullname->schema enclosing-namespace)))
    (map 'list #'read-field request)))

(declaim
 (ftype (function (t hash-table name:namespace) (values list &optional))
        parse-errors))
(defun parse-errors (errors fullname->schema enclosing-namespace)
  (flet ((read-jso (error)
           (internal:read-jso error fullname->schema enclosing-namespace)))
    (map 'list #'read-jso errors)))

;;; intern

(defmethod api:intern ((instance api:protocol) &key null-namespace)
  (flet ((intern-type (type)
           (api:intern type :null-namespace null-namespace)))
    (map nil #'intern-type (api:types instance)))
  (flet ((intern-message (message)
           (let* ((name (symbol-name
                         (closer-mop:generic-function-name message)))
                  (symbol (intern name intern:*intern-package*)))
             (export symbol intern:*intern-package*)
             (setf (fdefinition symbol) message))))
    (map nil #'intern-message (api:messages instance))))
