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
(defpackage #:org.apache.avro.internal.ipc.message
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:mop #:org.apache.avro.internal.mop)))
(in-package #:org.apache.avro.internal.ipc.message)

(deftype errors ()
  '(simple-array class (*)))

(deftype errors? ()
  '(or null errors))

(defclass api:message
    (mop:all-or-nothing-reinitialization closer-mop:standard-generic-function)
  ((request
    :initarg :request
    :reader api:request
    :type api:record
    :documentation "Request params.")
   (response
    :initarg :response
    :reader api:response
    :type api:schema
    :documentation "Response schema.")
   (errors
    :initarg :errors
    :type errors?
    :documentation "Error condition classes if provided, otherwise nil.")
   (effective-errors
    :type api:union
    :documentation "Effective error union.")
   (one-way
    :initarg :one-way
    :type boolean
    :documentation "Boolean indicating if message is one-way."))
  (:metaclass closer-mop:funcallable-standard-class)
  (:default-initargs
   :request (error "Must supply REQUEST")
   :response (error "Must supply RESPONSE")
   :errors nil)
  (:documentation
   "Metaclass of avro ipc messages."))

(defmethod closer-mop:validate-superclass
    ((class api:message) (superclass mop:all-or-nothing-reinitialization))
  t)

(defmethod closer-mop:validate-superclass
    ((class api:message) (superclass closer-mop:standard-generic-function))
  t)

(defmethod api:errors
    ((message api:message))
  "Return (values errors effective-errors)."
  (with-slots (errors effective-errors) message
    (values errors effective-errors)))

(defmethod api:one-way
    ((message api:message))
  "Return (values one-way one-way-provided-p)."
  (if (slot-boundp message 'one-way)
      (values (slot-value message 'one-way) t)
      (values nil nil)))

(declaim
 (ftype (function (api:record) (values list &optional)) deduce-lambda-list))
(defun deduce-lambda-list (request)
  (flet ((name (field)
           (nth-value 1 (api:name field))))
    (nconc (map 'list #'name (api:fields request))
           (list '&optional 'api:metadata))))

(defmethod initialize-instance :around
    ((instance api:message) &rest initargs &key request)
  (setf (getf initargs :lambda-list) (deduce-lambda-list request))
  (apply #'call-next-method instance initargs))

(defmethod reinitialize-instance :around
    ((instance api:message) &rest initargs)
  (if initargs
      (call-next-method)
      instance))

(declaim
 (ftype (function (errors?) (values api:union &optional))
        deduce-effective-errors))
(defun deduce-effective-errors (errors)
  (flet ((schema (error-class)
           (closer-mop:ensure-finalized error-class)
           (internal:schema (closer-mop:class-prototype error-class))))
    (let ((error-schemas (map 'list #'schema errors)))
      (make-instance 'api:union :schemas (cons 'api:string error-schemas)))))

(mop:definit ((instance api:message) :after &key)
  (with-slots (errors effective-errors response) instance
    ;; the errors json field represents a union and this
    ;; implementation doesn't allow empty unions, at least for the
    ;; time being
    (assert (if errors (plusp (length errors)) t) (errors)
            "Errors cannot be an empty array")
    (when (api:one-way instance)
      (unless (eq response 'api:null)
        (error "Message is one-way but response is not null: ~S" response))
      (when errors
        ;; union doesn't allow empty schemas so we don't have to check
        ;; length...that would be a problem only if we allow an empty
        ;; json array for errors
        (error "Message is one-way but there are errors declared: ~S" errors)))
    (setf effective-errors (deduce-effective-errors errors))))

;;; jso

(defmethod internal:write-jso
    ((message api:message) seen canonical-form-p)
  (flet ((write-jso (schema)
           (internal:write-jso schema seen canonical-form-p)))
    (let ((initargs
            (list
             "request" (map 'list #'write-jso (api:fields (api:request message)))
             "response" (write-jso (api:response message))))
          (documentation (documentation message t))
          (errors (api:errors message)))
      (when errors
        (let ((errors (map 'list #'write-jso errors)))
          (setf initargs (nconc initargs (list "errors" errors)))))
      (multiple-value-bind (one-way one-way-p)
          (api:one-way message)
        (when one-way-p
          (let ((one-way (st-json:as-json-bool one-way)))
            (setf initargs (nconc initargs (list "one-way" one-way))))))
      (when documentation
        (setf initargs (nconc initargs (list "doc" documentation))))
      (apply #'st-json:jso initargs))))
