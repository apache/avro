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
(defpackage #:org.apache.avro.internal.schema
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:mop #:org.apache.avro.internal.mop)
   (#:little-endian #:org.apache.avro.internal.little-endian))
  (:import-from #:org.apache.avro.internal.defprimitive
                #:*primitives*)
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:uint64
                #:ufixnum
                #:array<uint8>
                #:vector<uint8>)
  (:import-from #:org.apache.avro.internal.crc-64-avro
                #:crc-64-avro))
(in-package #:org.apache.avro.internal.schema)

;;; objects

(macrolet
    ((defobject ()
       `(deftype api:primitive-object ()
          "The set of objects adhering to a primitive avro schema."
          '(or ,@*primitives*))))
  (defobject))

(defclass api:complex-object ()
  ()
  (:documentation
   "Superclass of avro complex schemas."))

(defmethod api:schema-of
    ((object api:complex-object))
  (class-of object))

(defclass api:logical-object ()
  ()
  (:documentation
   "Superclass of avro logical schemas."))

(defmethod api:schema-of
    ((object api:logical-object))
  (class-of object))

(deftype api:object ()
  "An object adhering to an avro schema."
  '(or api:primitive-object api:complex-object api:logical-object))

;;; single-object-encoding

(defclass crc-64-avro-little-endian ()
  ((crc-64-avro-little-endian
    :type (array<uint8> 8)
    :initform (make-array 8 :element-type 'uint8)
    :reader internal:crc-64-avro-little-endian)))

(defmethod closer-mop:finalize-inheritance :around
    ((instance crc-64-avro-little-endian))
  (prog1 (call-next-method)
    (let ((bytes (internal:crc-64-avro-little-endian instance))
          (crc-64-avro (crc-64-avro
                        (babel:string-to-octets
                         (api:serialize instance :canonical-form-p t)
                         :encoding :utf-8))))
      (little-endian:uint64->vector crc-64-avro bytes 0))))

(defmethod internal:crc-64-avro-little-endian :before
    ((schema crc-64-avro-little-endian))
  (closer-mop:ensure-finalized schema))

(declaim
 (ftype (function (uint8 uint8) (values &optional))
        assert-single-object-encoding))
(defun assert-single-object-encoding (byte1 byte2)
  (unless (and (= byte1 #xc3)
               (= byte2 #x01))
    (error "Input does not adhere to Single Object Encoding"))
  (values))

(defmethod api:deserialize
    ((schema (eql 'api:fingerprint)) (input vector) &key (start 0))
  (declare (vector<uint8> input)
           (ufixnum start)
           (ignore schema))
  (assert-single-object-encoding (elt input start) (elt input (1+ start)))
  (values (little-endian:vector->uint64 input (+ start 2)) 10))

(defmethod api:deserialize
    ((schema (eql 'api:fingerprint)) (input stream) &key)
  (declare (ignore schema))
  (assert-single-object-encoding (read-byte input) (read-byte input))
  (values (little-endian:stream->uint64 input) 10))

;;; schemas

(macrolet
    ((defschema ()
       `(deftype api:primitive-schema ()
          "The set of avro primitive schemas."
          '(member ,@*primitives*))))
  (defschema))

(defclass api:complex-schema (crc-64-avro-little-endian
                              mop:all-or-nothing-reinitialization
                              standard-class)
  ()
  (:documentation
   "Base metaclass of avro complex schemas."))

(defmethod closer-mop:validate-superclass
    ((class api:complex-schema) (superclass crc-64-avro-little-endian))
  t)

(defmethod closer-mop:validate-superclass
    ((class api:complex-schema)
     (superclass mop:all-or-nothing-reinitialization))
  t)

(defmethod closer-mop:validate-superclass
    ((class api:complex-schema) (superclass standard-class))
  t)

(defclass api:logical-schema (crc-64-avro-little-endian
                              mop:all-or-nothing-reinitialization
                              standard-class)
  ()
  (:documentation
   "Base metaclass of avro logical schemas."))

(defmethod closer-mop:validate-superclass
    ((class api:logical-schema) (superclass crc-64-avro-little-endian))
  t)

(defmethod closer-mop:validate-superclass
    ((class api:logical-schema)
     (superclass mop:all-or-nothing-reinitialization))
  t)

(defmethod closer-mop:validate-superclass
    ((class api:logical-schema) (superclass standard-class))
  t)

(deftype api:schema ()
  "An avro schema."
  '(or api:primitive-schema api:complex-schema api:logical-schema))

;;; fingerprint

(deftype long/bytes ()
  '(or uint64 vector<uint8>))

(deftype fingerprint-function ()
  '(function (vector<uint8>) (values long/bytes &optional)))

(declaim (fingerprint-function api:*default-fingerprint-algorithm*))
(defparameter api:*default-fingerprint-algorithm* #'crc-64-avro
  "Default function used for FINGERPRINT.")

(declaim
 (ftype (function (api:schema &optional fingerprint-function)
                  (values long/bytes &optional))
        api:fingerprint))
(defun api:fingerprint
    (schema &optional (algorithm api:*default-fingerprint-algorithm*))
  "Return the fingerprint of avro SCHEMA under ALGORITHM."
  (if (eq algorithm #'crc-64-avro)
      (little-endian:vector->uint64
       (internal:crc-64-avro-little-endian schema) 0)
      (let* ((string (api:serialize schema))
             (bytes (babel:string-to-octets string :encoding :utf-8)))
        (funcall algorithm bytes))))

;;; single-object-encoding

(declaim
 (ftype (function (api:object t) (values (array<uint8> 8) &optional))
        fingerprint))
(defun fingerprint (object single-object-encoding-p)
  (let ((schema (api:schema-of object)))
    (declare (api:schema schema))
    (if (and (eq schema 'api:int)
             (eq single-object-encoding-p 'api:long))
        (internal:crc-64-avro-little-endian 'api:long)
        (internal:crc-64-avro-little-endian schema))))

(defmethod internal:serialize :around
    (object (into vector) &key single-object-encoding-p (start 0))
  (if (not single-object-encoding-p)
      (call-next-method)
      (let ((fingerprint (fingerprint object single-object-encoding-p)))
        (declare (vector<uint8> into)
                 (ufixnum start))
        (setf (elt into start) #xc3
              (elt into (1+ start)) #x01)
        (replace into fingerprint :start1 (+ start 2))
        (+ 10 (call-next-method object into :start (+ start 10))))))

(defmethod internal:serialize :around
    (object (into stream) &key single-object-encoding-p)
  (if (not single-object-encoding-p)
      (call-next-method)
      (let ((fingerprint (fingerprint object single-object-encoding-p)))
        (write-byte #xc3 into)
        (write-byte #x01 into)
        (write-sequence fingerprint into)
        (+ 10 (call-next-method object into)))))

;;; serialize

(defmethod internal:logical-name
    ((schema api:logical-schema))
  (internal:downcase-symbol (class-name schema)))

(defmethod internal:write-jso :around
    ((schema api:logical-schema) seen canonical-form-p)
  (let ((underlying (internal:write-jso
                     (internal:underlying schema) seen canonical-form-p)))
    (apply #'st-json:jso
           (list* "type" underlying
                  "logicalType" (internal:logical-name schema)
                  (call-next-method)))))

(defmethod internal:write-jso
    ((schema api:logical-schema) seen canonical-form-p)
  (declare (ignore schema seen canonical-form-p))
  nil)

(defmethod internal:write-json
    (object (into stream) &key)
  (st-json:write-json object into))

(defmethod internal:write-json
    (object (into string) &key (start 0))
  (declare (ufixnum start))
  (with-output-to-string
      (into (cond
              ((zerop start)
               into)
              ((array-has-fill-pointer-p into)
               (prog1 into
                 (setf (fill-pointer into) start)))
              (t
               (make-array (- (length into) start)
                           :element-type 'character
                           :fill-pointer 0
                           :displaced-to into
                           :displaced-index-offset start))))
    (st-json:write-json object into)))

(macrolet
    ((define-serialize (object-specializer)
       (declare (symbol object-specializer))
       `(defmethod api:serialize
            ((object ,object-specializer)
             &rest initargs
             &key
               (into (make-array 0 :element-type 'character :adjustable t
                                   :fill-pointer t))
               (start 0)
               (canonical-form-p nil))
          (declare (ignore start))
          (let* ((seen (make-hash-table :test #'eq))
                 (st-json:*output-literal-unicode*
                   (or canonical-form-p
                       st-json:*output-literal-unicode*))
                 (jso (internal:write-jso object seen canonical-form-p)))
            (apply #'internal:write-json jso into initargs))
          into)))
  (define-serialize api:complex-schema)
  (define-serialize api:logical-schema))

(defmethod api:serialize
    ((object symbol) &rest initargs)
  "Serialize the class named OBJECT."
  (apply #'api:serialize (find-class object) initargs))

;;; coerce

(defmethod api:coerce
    (object (schema symbol))
  (api:coerce object (find-class schema)))
