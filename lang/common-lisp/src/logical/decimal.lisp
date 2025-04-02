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
(defpackage #:org.apache.avro.internal.decimal
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:mop #:org.apache.avro.internal.mop)
   (#:bytes #:org.apache.avro.internal.bytes)
   (#:long #:org.apache.avro.internal.long)
   (#:big-endian #:org.apache.avro.internal.logical.big-endian))
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>)
  (:import-from #:org.apache.avro.internal.recursive-descent.pattern
                #:define-pattern-method))
(in-package #:org.apache.avro.internal.decimal)

;;; decimal

(deftype size ()
  '(and bytes:size (integer 1)))

(deftype precision ()
  '(integer 1))

(deftype scale ()
  '(integer 0))

(defclass api:decimal (api:logical-schema)
  ((underlying
    :initarg :underlying
    :reader internal:underlying
    :late-type (or (eql api:bytes) api:fixed))
   (scale
    :initarg :scale
    :type scale
    :documentation "Decimal scale.")
   (precision
    :initarg :precision
    :reader api:precision
    :type precision
    :documentation "Decimal precision."))
  (:metaclass mop:schema-class)
  (:scalars :scale :precision :underlying)
  (:object-class api:decimal-object)
  (:default-initargs
   :precision (error "Must supply PRECISION")
   :underlying (error "Must supply UNDERLYING"))
  (:documentation
   "Metaclass of avro decimal schemas."))

(defmethod closer-mop:validate-superclass
    ((class api:decimal) (superclass api:logical-schema))
  t)

(declaim (ftype (function (size) (values precision &optional)) max-precision))
(defun max-precision (size)
  (let ((integer (1- (expt 2 (1- (* 8 size))))))
    (nth-value 0 (truncate (log integer 10)))))

(declaim
 (ftype (function (precision size) (values &optional))
        %assert-decent-precision))
(defun %assert-decent-precision (precision size)
  (let ((max-precision (max-precision size)))
    (assert (<= precision max-precision) ()
            "Fixed schema with size ~S can store up to ~S digits, not ~S"
            size max-precision precision))
  (values))

(declaim
 (ftype (function (precision bytes:size) (values &optional))
        assert-decent-precision))
(defun assert-decent-precision (precision size)
  (assert (plusp size) () "Fixed schema has size 0")
  (%assert-decent-precision precision size)
  (values))

(mop:definit ((instance api:decimal) :after &key)
  (let ((precision (slot-value instance 'precision))
        (scale (if (slot-boundp instance 'scale)
                   (slot-value instance 'scale)
                   0)))
    (assert (<= scale precision) ()
            "Scale ~S cannot be greater than precision ~S" scale precision)))

(defmethod mop:early->late
    ((class api:decimal) (name (eql 'underlying)) type value)
  (let ((value (call-next-method)))
    (when (typep value 'api:fixed)
      (assert-decent-precision (slot-value class 'precision) (api:size value)))
    value))

(defmethod api:scale
    ((schema api:decimal))
  "Return (values scale scale-provided-p)."
  (closer-mop:ensure-finalized schema)
  (let* ((scalep (slot-boundp schema 'scale))
         (scale (if scalep
                    (slot-value schema 'scale)
                    0)))
    (values scale scalep)))

;;; %decimal-object

(defclass %decimal-object ()
  ((unscaled
    :accessor unscaled
    :type integer)))

;;; decimal-object

(defclass api:decimal-object (%decimal-object api:logical-object)
  ((unscaled
    :initarg :unscaled
    :reader api:unscaled
    :documentation "Unscaled integer for decimal object."))
  (:default-initargs
   :unscaled (error "Must supply UNSCALED"))
  (:documentation
   "Base class for instances of an avro decimal schema."))

(declaim
 (ftype (function (integer) (values (integer 1) &optional)) number-of-digits))
(defun number-of-digits (integer)
  (if (zerop integer)
      1
      (let ((abs (abs integer)))
        (nth-value 0 (ceiling (log (1+ abs) 10))))))

(defmethod initialize-instance :after
    ((instance api:decimal-object) &key)
  (with-slots (unscaled) instance
    (let ((max-precision (api:precision (class-of instance))))
      (assert (<= (number-of-digits unscaled) max-precision) (unscaled)
              "Decimal schema with precision ~S cannot represent ~S digits"
              max-precision (number-of-digits unscaled)))))

;;; serialized-size

(defmethod internal:fixed-size
    ((schema api:decimal))
  (internal:fixed-size (internal:underlying schema)))

(declaim
 (ftype (function (integer (or (eql api:bytes) api:fixed))
                  (values (bytes:size) &optional))
        min-buf-length))
(defun min-buf-length (unscaled schema)
  (if (eq schema 'api:bytes)
      (nth-value 0 (ceiling (1+ (integer-length unscaled)) 8))
      (api:size schema)))

(defmethod api:serialized-size
    ((object api:decimal-object))
  (let* ((underlying (internal:underlying (class-of object)))
         (buf-length (min-buf-length (api:unscaled object) underlying)))
    (if (eq underlying 'api:bytes)
        (+ (long:serialized-size buf-length) buf-length)
        buf-length)))

;;; twos-complement

(deftype bits ()
  `(integer 8 ,(* 8 bytes:+max-size+)))

(declaim
 (ftype (function ((integer 0) bits) (values integer &optional))
        from-twos-complement))
(defun from-twos-complement (integer bits)
  (let ((mask (ash 1 (1- bits))))
    (if (zerop (logand integer mask))
        integer
        (- integer (expt 2 bits)))))

(declaim
 (ftype (function (integer bits) (values (integer 0) &optional))
        to-twos-complement))
(defun to-twos-complement (integer bits)
  (if (minusp integer)
      (+ integer (expt 2 bits))
      integer))

;;; serialize

(defmethod internal:serialize
    ((object api:decimal-object) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (let* ((underlying (internal:underlying (class-of object)))
         (unscaled (api:unscaled object))
         (min-length (min-buf-length unscaled underlying))
         (twos-complement (to-twos-complement unscaled (* 8 min-length)))
         (end (+ start min-length)))
    (if (eq underlying 'api:bytes)
        (let ((bytes-written (long:serialize-into-vector
                              min-length into start)))
          (+ bytes-written
             (big-endian:to-vector
              twos-complement into (+ start bytes-written) (1+ end))))
        (big-endian:to-vector twos-complement into start end))))

(defmethod internal:serialize
    ((object api:decimal-object) (into stream) &key)
  (let* ((underlying (internal:underlying (class-of object)))
         (unscaled (api:unscaled object))
         (min-length (min-buf-length unscaled underlying))
         (twos-complement (to-twos-complement unscaled (* 8 min-length))))
    (if (eq underlying 'api:bytes)
        (+ (long:serialize-into-stream min-length into)
           (big-endian:to-stream twos-complement into min-length))
        (big-endian:to-stream twos-complement into min-length))))

(defmethod api:serialize
    ((object api:decimal-object)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (+ (if sp 10 0) (api:serialized-size object))
                         :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

;;; deserialize

(defmethod api:deserialize
    ((schema api:decimal) (input vector) &key (start 0))
  (declare (vector<uint8> input)
           (ufixnum start))
  (let ((underlying (internal:underlying schema)))
    (declare ((or (eql api:bytes) api:fixed) underlying))
    (if (eq underlying 'api:bytes)
        (multiple-value-bind (length bytes-read)
            (long:deserialize-from-vector input start)
          (let* ((start (+ start bytes-read))
                 (end (+ start length)))
            (declare (ufixnum start end))
            (multiple-value-bind (twos-complement more-bytes-read)
                (big-endian:from-vector input start end)
              (let ((unscaled (from-twos-complement
                               twos-complement (* 8 length)))
                    (bytes-read (+ bytes-read more-bytes-read))
                    (%decimal-object (make-instance '%decimal-object)))
                (setf (unscaled %decimal-object) unscaled)
                (values (change-class %decimal-object schema) bytes-read)))))
        (let* ((length (api:size underlying))
               (end (+ start length)))
          (declare (bytes:size length))
          (multiple-value-bind (twos-complement bytes-read)
              (big-endian:from-vector input start end)
            (let ((unscaled (from-twos-complement
                             twos-complement (* 8 length)))
                  (%decimal-object (make-instance '%decimal-object)))
              (setf (unscaled %decimal-object) unscaled)
              (values (change-class %decimal-object schema) bytes-read)))))))

(defmethod api:deserialize
    ((schema api:decimal) (input stream) &key)
  (let ((underlying (internal:underlying schema)))
    (declare ((or (eql api:bytes) api:fixed) underlying))
    (if (eq underlying 'api:bytes)
        (multiple-value-bind (length bytes-read)
            (long:deserialize-from-stream input)
          (multiple-value-bind (twos-complement more-bytes-read)
              (big-endian:from-stream input length)
            (let ((unscaled (from-twos-complement
                             twos-complement (* 8 length)))
                  (bytes-read (+ bytes-read more-bytes-read))
                  (%decimal-object (make-instance '%decimal-object)))
              (setf (unscaled %decimal-object) unscaled)
              (values (change-class %decimal-object schema) bytes-read))))
        (let ((length (api:size underlying)))
          (declare (bytes:size length))
          (multiple-value-bind (twos-complement bytes-read)
              (big-endian:from-stream input length)
            (let ((unscaled (from-twos-complement
                             twos-complement (* 8 length)))
                  (%decimal-object (make-instance '%decimal-object)))
              (setf (unscaled %decimal-object) unscaled)
              (values (change-class %decimal-object schema) bytes-read)))))))

;;; compare

(defmethod internal:skip
    ((schema api:decimal) (input vector) &optional (start 0))
  (internal:skip (internal:underlying schema) input start))

(defmethod internal:skip
    ((schema api:decimal) (input stream) &optional start)
  (declare (ignore start))
  (internal:skip (internal:underlying schema) input))

(defmethod api:compare
    ((schema api:decimal) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (api:compare
   (internal:underlying schema) left right
   :left-start left-start :right-start right-start))

(defmethod api:compare
    ((schema api:decimal) (left stream) (right stream) &key)
  (api:compare (internal:underlying schema) left right))

;;; coerce

(defmethod api:coerce
    ((object api:decimal-object) (schema api:decimal))
  (if (eq (class-of object) schema)
      object
      (let* ((writer (class-of object))
             (writer-scale (api:scale writer))
             (writer-precision (api:precision writer))
             (reader-scale (api:scale schema))
             (reader-precision (api:precision schema)))
        (declare (scale reader-scale writer-scale)
                 (precision reader-precision writer-precision))
        (assert (= reader-scale writer-scale) ()
                "Reader's decimal scale of ~S does not match writer's ~S"
                reader-scale writer-scale)
        (assert (= reader-precision writer-precision) ()
                "Reader's decimal precision of ~S does not match writer's ~S"
                reader-precision writer-precision)
        (change-class object schema))))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:decimal-object))
  (let* ((underlying (internal:underlying (class-of default)))
         (unscaled (api:unscaled default))
         (min-length (min-buf-length unscaled underlying))
         (twos-complement (to-twos-complement unscaled (* 8 min-length)))
         (buffer (make-array min-length :element-type 'uint8)))
    (big-endian:to-vector twos-complement buffer 0 min-length)
    (babel:octets-to-string buffer :encoding :latin-1)))

(defmethod internal:deserialize-field-default
    ((schema api:decimal) (default string))
  (let* ((buffer (babel:string-to-octets default :encoding :latin-1))
         (twos-complement (big-endian:from-vector buffer 0 (length buffer)))
         (unscaled
           (from-twos-complement twos-complement (* 8 (length buffer))))
         (%decimal-object (make-instance '%decimal-object)))
    (declare ((simple-array uint8 (*)) buffer))
    (setf (unscaled %decimal-object) unscaled)
    (change-class %decimal-object schema)))

;;; jso

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" nil
                    "logicalType" "decimal"))
              fullname->schema
              enclosing-namespace)
      (let ((underlying (internal:read-jso (st-json:getjso "type" jso)
                                           fullname->schema
                                           enclosing-namespace)))
        (handler-case
            (internal:with-initargs (precision scale) jso
              (push underlying initargs)
              (push :underlying initargs)
              (apply #'make-instance 'api:decimal initargs))
          (error ()
            underlying)))))

(defmethod internal:logical-name
    ((schema api:decimal))
  (declare (ignore schema))
  "decimal")

(defmethod internal:write-jso
    ((schema api:decimal) seen canonical-form-p)
  (declare (ignore seen canonical-form-p))
  (let (initargs)
    (multiple-value-bind (scale scalep)
        (api:scale schema)
      (when scalep
        (push scale initargs)
        (push "scale" initargs)))
    (list* "precision" (api:precision schema) initargs)))
