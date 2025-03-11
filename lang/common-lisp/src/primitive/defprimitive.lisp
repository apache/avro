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
(defpackage #:org.apache.avro.internal.defprimitive
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:little-endian #:org.apache.avro.internal.little-endian))
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:uint64
                #:array<uint8>)
  (:import-from #:org.apache.avro.internal.crc-64-avro
                #:crc-64-avro)
  (:import-from #:org.apache.avro.internal.recursive-descent.pattern
                #:define-pattern-method)
  (:import-from #:org.apache.avro.internal
                #:read-jso
                #:write-jso
                #:crc-64-avro-little-endian)
  (:import-from #:alexandria
                #:define-constant)
  (:export #:defprimitive
           #:*primitives*))
(in-package #:org.apache.avro.internal.defprimitive)

(defgeneric write-json-string
    (string into &key &allow-other-keys))

(defmethod write-json-string
    ((string simple-string) (into stream) &key)
  (write-string string into)
  into)

(defmethod write-json-string
    ((string simple-string) (into string) &key (start 0))
  (replace into string :start1 start)
  into)

(defmethod write-json-string
    ((string simple-string) (into null) &key)
  (declare (ignore into))
  string)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defparameter *primitives* nil
    "List of primitive schemas."))

(defmacro defprimitive (name base &body docstring)
  "Define a primitive schema NAME based on BASE."
  (declare (symbol name)
           ((or symbol cons) base))
  (pushnew name *primitives* :test #'eq)
  (let ((+jso+ (intern "+JSO+"))
        (+json+ (intern "+JSON+"))
        (+json-non-canonical+ (intern "+JSON-NON-CANONICAL+"))
        (+crc-64-avro+ (intern "+CRC-64-AVRO+"))
        (+crc-64-avro-little-endian+ (intern "+CRC-64-AVRO-LITTLE-ENDIAN+"))
        (string (string-downcase (string name))))
    `(progn
       (deftype ,name ()
         ,@docstring
         ',base)

       (declaim ((eql ,name) ,name))
       (defconstant ,name ',name
         ,@docstring)

       (declaim (simple-string ,+jso+))
       (define-constant ,+jso+ ,string :test #'string=)

       (declaim (simple-string ,+json+))
       (define-constant ,+json+
           (st-json:write-json-to-string ,+jso+)
         :test #'string=)

       (declaim (simple-string ,+json-non-canonical+))
       (define-constant ,+json-non-canonical+
           (st-json:write-json-to-string
            (let ((hash-table (make-hash-table)))
              (setf (gethash "type" hash-table) ,+jso+)
              hash-table))
         :test #'string=)

       (declaim (uint64 ,+crc-64-avro+))
       (defconstant ,+crc-64-avro+
         (crc-64-avro (babel:string-to-octets ,+json+ :encoding :utf-8)))

       (declaim ((array<uint8> 8) ,+crc-64-avro-little-endian+))
       (define-constant ,+crc-64-avro-little-endian+
           (let ((vector (make-array 8 :element-type 'uint8)))
             (little-endian:uint64->vector ,+crc-64-avro+ vector 0)
             vector)
         :test #'equalp)

       (defmethod crc-64-avro-little-endian
           ((schema (eql ',name)))
         (declare (ignore schema))
         ,+crc-64-avro-little-endian+)

       (define-pattern-method 'read-jso
           '(lambda ((jso ("type" ,string)) fullname->schema enclosing-namespace)
             (declare (ignore jso fullname->schema enclosing-namespace))
             ,name))

       (define-pattern-method 'read-jso
           '(lambda ((jso ,string) fullname->schema enclosing-namespace)
             (declare (ignore jso fullname->schema enclosing-namespace))
             ,name))

       (defmethod write-jso
           ((schema (eql ',name)) seen canonical-form-p)
         (declare (ignore schema seen canonical-form-p))
         ,+jso+)

       (defmethod api:serialize
           ((schema (eql ',name)) &key into (start 0) (canonical-form-p t))
         (declare (ignore schema))
         (let ((string (if canonical-form-p ,+json+ ,+json-non-canonical+)))
           (write-json-string string into :start start))))))
