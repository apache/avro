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
(defpackage #:org.apache.avro.internal.fixed
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:mop #:org.apache.avro.internal.mop)
   (#:name #:org.apache.avro.internal.name)
   (#:bytes #:org.apache.avro.internal.bytes)
   (#:sequences #:org.shirakumo.trivial-extensible-sequences)
   (#:intern #:org.apache.avro.internal.intern))
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>
                #:comparison)
  (:import-from #:org.apache.avro.internal.compare
                #:compare-byte-vectors
                #:compare-byte-streams)
  (:import-from #:org.apache.avro.internal.recursive-descent.pattern
                #:define-pattern-method))
(in-package #:org.apache.avro.internal.fixed)

;;; fixed

(defclass api:fixed (name:named-schema)
  ((size
    :initarg :size
    :reader api:size
    :type bytes:size
    :documentation "Fixed schema size."))
  (:metaclass mop:schema-class)
  (:scalars :size)
  (:object-class api:fixed-object)
  (:default-initargs
   :size (error "Must supply SIZE"))
  (:documentation
   "Metaclass of avro fixed schemas."))

(defmethod closer-mop:validate-superclass
    ((class api:fixed) (superclass name:named-schema))
  t)

(declaim
 (ftype (function (bytes:size) (values cons &optional)) make-buffer-slot))
(defun make-buffer-slot (size)
  (list :name 'buffer
        :type `(vector uint8 ,size)))

(mop:definit ((instance api:fixed) :around &rest initargs &key size)
  (let ((buffer-slot (make-buffer-slot (mop:scalarize size))))
    (push buffer-slot (getf initargs :direct-slots)))
  (apply #'call-next-method instance initargs))

;;; buffered

(defclass buffered ()
  ((buffer
    :accessor buffer
    :type (vector uint8))))

;;; fixed-object

(defclass api:fixed-object
    (buffered api:complex-object #+sbcl sequence #-sbcl sequences:sequence)
  ((buffer :reader api:raw :documentation "The underlying octet vector."))
  (:documentation
   "Base class for instances of an avro fixed schema."))

(defmethod initialize-instance :after
    ((instance api:fixed-object)
     &key
       (initial-element nil initial-element-p)
       (initial-contents nil initial-contents-p))
  (let ((size (api:size (class-of instance)))
        (keyword-args (list :element-type 'uint8)))
    (when initial-element-p
      (push initial-element keyword-args)
      (push :initial-element keyword-args))
    (when initial-contents-p
      (push initial-contents keyword-args)
      (push :initial-contents keyword-args))
    (setf (buffer instance) (apply #'make-array size keyword-args))))

(defmethod sequences:length
    ((instance api:fixed-object))
  (length (buffer instance)))

(defmethod sequences:elt
    ((instance api:fixed-object) (index fixnum))
  (elt (buffer instance) index))

(defmethod (setf sequences:elt)
    (value (instance api:fixed-object) (index fixnum))
  (setf (elt (buffer instance) index) value))

(defmethod sequences:adjust-sequence
    ((instance api:fixed-object)
     (length fixnum)
     &key
       (initial-element nil initial-element-p)
       (initial-contents nil initial-contents-p))
  (let ((size (api:size (class-of instance)))
        (keyword-args (list :element-type 'uint8)))
    (unless (= length size)
      (error "Cannot change size of fixed object from ~S to ~S" size length))
    (when initial-element-p
      (push initial-element keyword-args)
      (push :initial-element keyword-args))
    (when initial-contents-p
      (push initial-contents keyword-args)
      (push :initial-contents keyword-args))
    (setf (buffer instance)
          (apply #'adjust-array (buffer instance) length keyword-args)))
  instance)

(defmethod sequences:make-sequence-like
    ((instance api:fixed-object)
     (length fixnum)
     &rest keyword-args
     &key
       (initial-element nil initial-element-p)
       (initial-contents nil initial-contents-p))
  (declare (ignore initial-element initial-contents))
  (if (or initial-element-p initial-contents-p)
      (apply #'make-instance (class-of instance) keyword-args)
      (if (not (slot-boundp instance 'buffer))
          (make-instance (class-of instance))
          (let ((buffered (make-instance 'buffered)))
            (setf (buffer buffered) (buffer instance))
            (change-class buffered (class-of instance))))))

(defmethod sequences:make-sequence-iterator
    ((instance api:fixed-object)
     &key (start 0) (end (length instance)) from-end)
  (let* ((end (or end (length instance)))
         (iterator (if from-end (1- end) start))
         (limit (if from-end (1- start) end)))
    (values
     iterator
     limit
     from-end
     (if from-end
         (lambda (sequence iterator from-end)
           (declare (ignore sequence from-end)
                    (ufixnum iterator))
           (the ufixnum (1- iterator)))
         (lambda (sequence iterator from-end)
           (declare (ignore sequence from-end)
                    (ufixnum iterator))
           (the ufixnum (1+ iterator))))
     (lambda (sequence iterator limit from-end)
       (declare (ignore sequence from-end)
                (ufixnum iterator)
                ((or ufixnum null) limit))
       (= iterator limit))
     (lambda (sequence iterator)
       (declare (api:fixed-object sequence)
                (ufixnum iterator))
       (elt sequence iterator))
     (lambda (value sequence iterator)
       (declare (api:fixed-object sequence)
                (ufixnum iterator))
       (setf (elt sequence iterator) value))
     (lambda (sequence iterator)
       (declare (ignore sequence)
                (ufixnum iterator))
       iterator)
     (lambda (sequence iterator)
       (declare (ignore sequence)
                (ufixnum iterator))
       iterator))))

;;; serialized-size

(defmethod internal:fixed-size
    ((schema api:fixed))
  (api:size schema))

(defmethod api:serialized-size
    ((object api:fixed-object))
  (length object))

;;; serialize

(defmethod internal:serialize
    ((object api:fixed-object) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (let ((buffer (buffer object)))
    (declare (vector<uint8> buffer))
    (assert (>= (- (length into) start)
                (length buffer))
            (into)
            "Not enough room in vector, need ~S more byte~:P"
            (- (length buffer)
               (- (length into) start)))
    (replace into buffer :start1 start)
    (length buffer)))

(defmethod internal:serialize
    ((object api:fixed-object) (into stream) &key)
  (let ((buffer (buffer object)))
    (declare (vector<uint8> buffer))
    (length (write-sequence buffer into))))

(defmethod api:serialize
    ((object api:fixed-object)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (when sp (make-array (+ 10 (api:serialized-size object))
                                  :element-type 'uint8)))
       (start 0))
  (declare (ignore start))
  (if into
      (values into (apply #'internal:serialize object into initargs))
      (let ((buffer (buffer object)))
        (declare (vector<uint8> buffer))
        (values buffer (length buffer)))))

;;; deserialize

(defmethod api:deserialize
    ((schema api:fixed) (input vector) &key (start 0))
  (declare (vector<uint8> input)
           (ufixnum start))
  (let ((size (api:size schema))
        (buffered (make-instance 'buffered)))
    (setf (buffer buffered) (make-array size
                                        :element-type 'uint8
                                        :displaced-to input
                                        :displaced-index-offset start))
    (values (change-class buffered schema) size)))

(defmethod api:deserialize
    ((schema api:fixed) (input stream) &key)
  (let* ((size (api:size schema))
         (buffered (make-instance 'buffered))
         (buffer (make-array size :element-type 'uint8)))
    (unless (= (read-sequence buffer input) size)
      (error 'end-of-file :stream *error-output*))
    (setf (buffer buffered) buffer)
    (values (change-class buffered schema) size)))

;;; compare

(defmethod internal:skip
    ((schema api:fixed) (input vector) &optional start)
  (declare (ignore input start))
  (the (values bytes:size &optional) (api:size schema)))

(defmethod internal:skip
    ((schema api:fixed) (input stream) &optional start)
  (declare (ignore start))
  (let ((size (api:size schema)))
    (declare (bytes:size size))
    (loop repeat size do (read-byte input))
    size))

(defmethod api:compare
    ((schema api:fixed) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (vector<uint8> left right)
           (ufixnum left-start right-start))
  (let* ((size (api:size schema))
         (left-end (+ left-start size))
         (right-end (+ right-start size)))
    (declare (bytes:size size))
    (compare-byte-vectors
     left right left-start right-start left-end right-end)))

(defmethod api:compare
    ((schema api:fixed) (left stream) (right stream) &key)
  (let ((size (api:size schema)))
    (compare-byte-streams left right size size)))

;;; coerce

(defmethod api:coerce
    ((object api:fixed-object) (schema api:fixed))
  (if (eq (class-of object) schema)
      object
      (let* ((writer (class-of object))
             (writer-size (api:size writer))
             (reader-size (api:size schema)))
        (declare (api:fixed writer)
                 (bytes:size writer-size reader-size))
        (name:assert-matching-names schema writer)
        (unless (= reader-size writer-size)
          (error
           "Reader and writer fixed schemas have different sizes: ~S and ~S"
           reader-size writer-size))
        (change-class object schema))))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:fixed-object))
  (babel:octets-to-string default :encoding :latin-1))

(defmethod internal:deserialize-field-default
    ((schema api:fixed) (default string))
  (let ((bytes (babel:string-to-octets default :encoding :latin-1)))
    (make-instance schema :bytes bytes)))

;;; jso

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" "fixed")) fullname->schema enclosing-namespace)
      (internal:with-initargs (name namespace aliases size) jso
        (push enclosing-namespace initargs)
        (push :enclosing-namespace initargs)
        (push (make-symbol (st-json:getjso "name" jso)) initargs)
        (push :name initargs)
        (let* ((schema (apply #'make-instance 'api:fixed initargs))
               (fullname (api:fullname schema)))
          (assert (not (gethash fullname fullname->schema)) ()
                  "Name ~S is already taken" fullname)
          (setf (gethash fullname fullname->schema) schema)))))

(defmethod internal:write-jso
    ((schema api:fixed) seen canonical-form-p)
  (declare (ignore seen canonical-form-p))
  (list "size" (api:size schema)))

;;; intern

(defmethod api:intern ((instance api:fixed) &key null-namespace)
  (declare (ignore instance null-namespace)))
