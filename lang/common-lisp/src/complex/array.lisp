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
(defpackage #:org.apache.avro.internal.array
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:mop #:org.apache.avro.internal.mop)
   (#:bytes #:org.apache.avro.internal.bytes)
   (#:long #:org.apache.avro.internal.long)
   (#:count-and-size #:org.apache.avro.internal.count-and-size)
   (#:sequences #:org.shirakumo.trivial-extensible-sequences))
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>
                #:comparison)
  (:import-from #:org.apache.avro.internal.recursive-descent.pattern
                #:define-pattern-method)
  (:export #:buffered
           #:buffer
           #:%serialized-size))
(in-package #:org.apache.avro.internal.array)

;;; array

(defclass api:array (api:complex-schema)
  ((items
    :initarg :items
    :reader api:items
    :late-type api:schema
    :documentation "Array schema element type."))
  (:metaclass mop:schema-class)
  (:scalars :items)
  (:object-class api:array-object)
  (:default-initargs
   :items (error "Must supply ITEMS"))
  (:documentation
   "Metaclass of avro array schemas."))

(defmethod closer-mop:validate-superclass
    ((class api:array) (superclass api:complex-schema))
  t)

(declaim
 (ftype (function ((or api:schema symbol)) (values cons &optional))
        make-buffer-slot))
(defun make-buffer-slot (items)
  (list :name 'buffer
        :type `(vector ,items)))

(mop:definit ((instance api:array) :around &rest initargs &key items)
  (let ((buffer-slot (make-buffer-slot (mop:scalarize items))))
    (push buffer-slot (getf initargs :direct-slots)))
  (apply #'call-next-method instance initargs))

;;; buffered

(defclass buffered ()
  ((buffer
    :accessor buffer
    :type vector)))

;;; array-object

(defclass api:array-object
    (buffered api:complex-object #+sbcl sequence #-sbcl sequences:sequence)
  ((buffer :reader api:raw :documentation "The underlying vector."))
  (:documentation
   "Base class for instances of an avro array schema."))

(defmethod initialize-instance :after
    ((instance api:array-object)
     &key
       (initial-element nil initial-element-p)
       (initial-contents nil initial-contents-p)
       (length (length initial-contents)))
  (let* ((schema (api:items (class-of instance)))
         (keyword-args `(:element-type ,schema :fill-pointer t :adjustable t)))
    (when initial-element-p
      (assert (typep initial-element schema))
      (push initial-element keyword-args)
      (push :initial-element keyword-args))
    (when initial-contents-p
      (flet ((assert-type (object)
               (assert (typep object schema))))
        (map nil #'assert-type initial-contents))
      (push initial-contents keyword-args)
      (push :initial-contents keyword-args))
    (setf (buffer instance) (apply #'make-array length keyword-args))))

(defmethod api:push
    (element (instance api:array-object))
  "Push ELEMENT onto end of INSTANCE."
  (assert (typep element (api:items (class-of instance))))
  (vector-push-extend element (buffer instance)))

(defmethod api:pop
    ((instance api:array-object))
  "Pop the last element from INSTANCE."
  (vector-pop (buffer instance)))

(defmethod sequences:length
    ((instance api:array-object))
  (length (buffer instance)))

(defmethod sequences:elt
    ((instance api:array-object) (index fixnum))
  (elt (buffer instance) index))

(defmethod (setf sequences:elt)
    (value (instance api:array-object) (index fixnum))
  (assert (typep value (api:items (class-of instance))))
  (setf (elt (buffer instance) index) value))

(defmethod sequences:adjust-sequence
    ((instance api:array-object)
     (length fixnum)
     &key
       (initial-element nil initial-element-p)
       (initial-contents nil initial-contents-p))
  (let* ((schema (api:items (class-of instance)))
         (keyword-args (list :element-type schema :fill-pointer t)))
    (when initial-element-p
      (assert (typep initial-element schema))
      (cl:push initial-element keyword-args)
      (cl:push :initial-element keyword-args))
    (when initial-contents-p
      (flet ((assert-type (object)
               (assert (typep object schema))))
        (map nil #'assert-type initial-contents))
      (cl:push initial-contents keyword-args)
      (cl:push :initial-contents keyword-args))
    (setf (buffer instance)
          (apply #'adjust-array (buffer instance) length keyword-args)))
  instance)

(defmethod sequences:make-sequence-like
    ((instance api:array-object)
     (length fixnum)
     &rest keyword-args
     &key
       (initial-element nil initial-element-p)
       (initial-contents nil initial-contents-p))
  (declare (ignore initial-element initial-contents))
  (if (or initial-element-p initial-contents-p)
      (apply #'make-instance (class-of instance)
             (list* :length length keyword-args))
      (if (slot-boundp instance 'buffer)
          (make-instance (class-of instance)
                         :length length :initial-contents (buffer instance))
          (make-instance (class-of instance) :length length))))

(defmethod sequences:make-sequence-iterator
    ((instance api:array-object)
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
       (declare (api:array-object sequence)
                (ufixnum iterator))
       (elt sequence iterator))
     (lambda (value sequence iterator)
       (declare (api:array-object sequence)
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
    ((schema api:array))
  (declare (ignore schema))
  nil)

(declaim
 (ftype (function (bytes:size api:object) (values bytes:size &optional)) sum))
(defun sum (running-total object)
  (let ((serialized-size (api:serialized-size object)))
    (declare (bytes:size serialized-size))
    (+ running-total serialized-size)))

(declaim
 (ftype (function (vector api:schema) (values bytes:size &optional))
        %serialized-size))
(defun %serialized-size (buffer items)
  (let ((fixed-size (internal:fixed-size items)))
    (declare ((or null bytes:size) fixed-size))
    (if fixed-size
        (* fixed-size (length buffer))
        (reduce #'sum buffer :initial-value 0))))

(declaim
 (ftype (function (vector api:schema) (values bytes:size &optional))
        serialized-size))
(defun serialized-size (buffer items)
  (let ((count (length buffer)))
    (if (zerop count)
        1
        (let* ((count (long:serialized-size (- count)))
               (serialized-size (%serialized-size buffer items))
               (size (long:serialized-size serialized-size)))
          (+ count size serialized-size 1)))))

(defmethod api:serialized-size
    ((object api:array-object))
  (serialized-size (buffer object) (api:items (class-of object))))

;;; serialize-into-vector

(declaim
 (ftype (function (api:schema vector vector<uint8> ufixnum)
                  (values ufixnum &optional))
        serialize-into-vector))
(defun serialize-into-vector (items buffer into start)
  (let ((count (length buffer))
        (bytes-written 0))
    (declare (ufixnum bytes-written))
    (unless (zerop count)
      (loop
        initially
           (incf bytes-written
                 (long:serialize-into-vector (- count) into start))
           (incf bytes-written
                 (long:serialize-into-vector
                  (%serialized-size buffer items)
                  into
                  (+ start bytes-written)))

        for object of-type api:object across buffer
        for new-start of-type ufixnum = (+ start bytes-written)
        for new-bytes-written of-type ufixnum = (internal:serialize
                                                 object into :start new-start)

        do (incf bytes-written new-bytes-written)))
    (+ bytes-written
       (long:serialize-into-vector 0 into (+ start bytes-written)))))

(defmethod internal:serialize
    ((object api:array-object) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (serialize-into-vector
   (api:items (class-of object)) (buffer object) into start))

;;; serialize-into-stream

(declaim
 (ftype (function (api:schema vector stream) (values ufixnum &optional))
        serialize-into-stream))
(defun serialize-into-stream (items buffer into)
  (let ((count (length buffer))
        (bytes-written 0))
    (declare (ufixnum bytes-written))
    (unless (zerop count)
      (loop
        initially
           (incf bytes-written (long:serialize-into-stream (- count) into))
           (incf bytes-written (long:serialize-into-stream
                                (%serialized-size buffer items) into))

        for object of-type api:object across buffer
        for new-bytes-written of-type ufixnum = (internal:serialize
                                                 object into)

        do (incf bytes-written new-bytes-written)))
    (+ bytes-written (long:serialize-into-stream 0 into))))

(defmethod internal:serialize
    ((object api:array-object) (into stream) &key)
  (serialize-into-stream
   (api:items (class-of object)) (buffer object) into))

;;; serialize

(defmethod api:serialize
    ((object api:array-object)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (+ (if sp 10 0) (api:serialized-size object))
                         :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

;;; deserialize-from-vector

(declaim
 (ftype (function (api:schema vector<uint8> ufixnum)
                  (values vector ufixnum &optional))
        deserialize-from-vector))
(defun deserialize-from-vector (schema input start)
  (loop
    with total-bytes-read of-type ufixnum = 0
    and output = (make-array 0 :element-type schema
                               :adjustable t :fill-pointer t)

    for count of-type fixnum
      = (multiple-value-bind (count bytes-read)
            (long:deserialize-from-vector input (+ start total-bytes-read))
          (incf total-bytes-read bytes-read)
          count)
    until (zerop count)

    when (minusp count) do
      (setf count (abs count))
      (incf total-bytes-read
            (nth-value 1 (long:deserialize-from-vector
                          input (+ start total-bytes-read))))

    do (loop
         repeat count
         for item of-type api:object
           = (multiple-value-bind (item bytes-read)
                 (api:deserialize
                  schema input :start (+ start total-bytes-read))
               (declare (ufixnum bytes-read))
               (incf total-bytes-read bytes-read)
               item)
         do (vector-push-extend item output))

    finally
       (return
         (values output total-bytes-read))))

(defmethod api:deserialize
    ((schema api:array) (input vector) &key (start 0))
  (declare (vector<uint8> input)
           (ufixnum start))
  (multiple-value-bind (vector bytes-read)
      (deserialize-from-vector (api:items schema) input start)
    (let ((buffered (make-instance 'buffered)))
      (setf (buffer buffered) vector)
      (values (change-class buffered schema) bytes-read))))

;;; deserialize-from-stream

(declaim
 (ftype (function (api:schema stream) (values vector ufixnum &optional))
        deserialize-from-stream))
(defun deserialize-from-stream (schema input)
  (loop
    with total-bytes-read of-type ufixnum = 0
    and output = (make-array 0 :element-type schema
                               :adjustable t :fill-pointer t)

    for count of-type fixnum
      = (multiple-value-bind (count bytes-read)
            (long:deserialize-from-stream input)
          (incf total-bytes-read bytes-read)
          count)
    until (zerop count)

    when (minusp count) do
      (setf count (abs count))
      (incf total-bytes-read
            (nth-value 1 (long:deserialize-from-stream input)))

    do (loop
         repeat count
         for item of-type api:object
           = (multiple-value-bind (item bytes-read)
                 (api:deserialize schema input)
               (declare (ufixnum bytes-read))
               (incf total-bytes-read bytes-read)
               item)
         do (vector-push-extend item output))

    finally
       (return
         (values output total-bytes-read))))

(defmethod api:deserialize
    ((schema api:array) (input stream) &key)
  (multiple-value-bind (vector bytes-read)
      (deserialize-from-stream (api:items schema) input)
    (let ((buffered (make-instance 'buffered)))
      (setf (buffer buffered) vector)
      (values (change-class buffered schema) bytes-read))))

;;; compare vectors

(defmethod internal:skip
    ((schema api:array) (input vector) &optional (start 0))
  (declare (vector<uint8> input)
           (ufixnum start))
  (loop
    with items of-type api:schema = (api:items schema)
    and total-bytes-read of-type ufixnum = 0

    for count of-type fixnum
      = (multiple-value-bind (count bytes-read)
            (long:deserialize-from-vector input (+ start total-bytes-read))
          (incf total-bytes-read bytes-read)
          count)
    until (zerop count)

    if (minusp count) do
      (multiple-value-bind (size bytes-read)
          (long:deserialize-from-vector input (+ start total-bytes-read))
        (incf total-bytes-read (+ size bytes-read)))
    else do
      (loop
        repeat count
        do (incf total-bytes-read
                 (internal:skip items input (+ start total-bytes-read))))

    finally
       (return
         total-bytes-read)))

(declaim
 (ftype (function (api:schema vector<uint8> vector<uint8> ufixnum ufixnum)
                  (values comparison ufixnum ufixnum &optional))
        compare-vectors))
(defun compare-vectors (schema left right left-start right-start)
  (loop
    with left-bytes-read of-type ufixnum = 0
    and right-bytes-read of-type ufixnum = 0

    for left-count of-type ufixnum
      = (multiple-value-bind (count size bytes-read)
            (count-and-size:from-vector left left-start)
          (declare (ignore size))
          (incf left-bytes-read bytes-read)
          count)
        then (if (zerop (decf left-count min-count))
                 (multiple-value-bind (count size bytes-read)
                     (count-and-size:from-vector
                      left (+ left-start left-bytes-read))
                   (declare (ignore size))
                   (incf left-bytes-read bytes-read)
                   count)
                 left-count)
    for right-count of-type ufixnum
      = (multiple-value-bind (count size bytes-read)
            (count-and-size:from-vector right right-start)
          (declare (ignore size))
          (incf right-bytes-read bytes-read)
          count)
        then (if (zerop (decf right-count min-count))
                 (multiple-value-bind (count size bytes-read)
                     (count-and-size:from-vector
                      right (+ right-start right-bytes-read))
                   (declare (ignore size))
                   (incf right-bytes-read bytes-read)
                   count)
                 right-count)
    for min-count = (min left-count right-count)

    until (or (zerop left-count)
              (zerop right-count))

    do (loop
         repeat min-count
         for comparison of-type comparison
           = (multiple-value-bind (comparison left right)
                 (api:compare
                  schema left right
                  :left-start (+ left-start left-bytes-read)
                  :right-start (+ right-start right-bytes-read))
               (declare (ufixnum left right))
               (incf left-bytes-read left)
               (incf right-bytes-read right)
               comparison)
         unless (zerop comparison) do
           (return-from compare-vectors
             (values comparison left-bytes-read right-bytes-read)))

    finally
       (return
         (values (cond
                   ((= 0 left-count right-count) 0)
                   ((= 0 left-count) -1)
                   (t 1))
                 left-bytes-read
                 right-bytes-read))))

(defmethod api:compare
    ((schema api:array) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (vector<uint8> left right)
           (ufixnum left-start right-start))
  (compare-vectors (api:items schema) left right left-start right-start))

;;; compare streams

(defmethod internal:skip
    ((schema api:array) (input stream) &optional start)
  (declare (ignore start))
  (loop
    with items of-type api:schema = (api:items schema)
    and total-bytes-read of-type ufixnum = 0

    for count of-type fixnum
      = (multiple-value-bind (count bytes-read)
            (long:deserialize-from-stream input)
          (incf total-bytes-read bytes-read)
          count)
    until (zerop count)

    if (minusp count) do
      (multiple-value-bind (size bytes-read)
          (long:deserialize-from-stream input)
        (incf total-bytes-read (+ size bytes-read)))
    else do
      (loop
        repeat count
        do (incf total-bytes-read (internal:skip items input)))

    finally
       (return
         total-bytes-read)))

(declaim
 (ftype (function (api:schema stream stream)
                  (values comparison ufixnum ufixnum &optional))
        compare-streams))
(defun compare-streams (schema left right)
  (loop
    with left-bytes-read of-type ufixnum = 0
    and right-bytes-read of-type ufixnum = 0

    for left-count of-type ufixnum
      = (multiple-value-bind (count size bytes-read)
            (count-and-size:from-stream left)
          (declare (ignore size))
          (incf left-bytes-read bytes-read)
          count)
        then (if (zerop (decf left-count min-count))
                 (multiple-value-bind (count size bytes-read)
                     (count-and-size:from-stream left)
                   (declare (ignore size))
                   (incf left-bytes-read bytes-read)
                   count)
                 left-count)
    for right-count of-type ufixnum
      = (multiple-value-bind (count size bytes-read)
            (count-and-size:from-stream right)
          (declare (ignore size))
          (incf right-bytes-read bytes-read)
          count)
        then (if (zerop (decf right-count min-count))
                 (multiple-value-bind (count size bytes-read)
                     (count-and-size:from-stream right)
                   (declare (ignore size))
                   (incf right-bytes-read bytes-read)
                   count)
                 right-count)
    for min-count = (min left-count right-count)

    until (or (zerop left-count)
              (zerop right-count))

    do (loop
         repeat min-count
         for comparison of-type comparison
           = (multiple-value-bind (comparison left right)
                 (api:compare schema left right)
               (declare (ufixnum left right))
               (incf left-bytes-read left)
               (incf right-bytes-read right)
               comparison)
         unless (zerop comparison) do
           (return-from compare-streams
             (values comparison left-bytes-read right-bytes-read)))

    finally
       (return
         (values (cond 
                   ((= 0 left-count right-count) 0)
                   ((= 0 left-count) -1)
                   (t 1))
                 left-bytes-read
                 right-bytes-read))))

(defmethod api:compare
    ((schema api:array) (left stream) (right stream) &key)
  (compare-streams (api:items schema) left right))

;;; coerce

(declaim
 (ftype (function (vector api:schema) (values vector &optional))
        coerce-buffer))
(defun coerce-buffer (buffer items)
  (loop
    with buffer = (if (subtypep (array-element-type buffer)
                                (upgraded-array-element-type items))
                      buffer
                      (coerce buffer '(vector t)))

    for i below (length buffer)
    for object across buffer

    do (setf (elt buffer i) (api:coerce object items))

    finally
       (return (coerce buffer `(vector ,items)))))

(declaim
 (ftype (function (vector api:schema) (values buffered &optional)) %coerce))
(defun %coerce (buffer items)
  (let ((buffered (make-instance 'buffered)))
    (setf (buffer buffered) (coerce-buffer buffer items))
    buffered))

(defmethod api:coerce
    ((object api:array-object) (schema api:array))
  (if (eq (class-of object) schema)
      object
      (let ((items (api:items schema)))
        (if (eq (api:items (class-of object)) items)
            (change-class object schema)
            (change-class (%coerce (buffer object) items) schema)))))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:array-object))
  (map 'list #'internal:serialize-field-default default))

(defmethod internal:deserialize-field-default
    ((schema api:array) (default list))
  (flet ((deserialize-elt (elt)
           (internal:deserialize-field-default (api:items schema) elt)))
    (make-instance
     schema :initial-contents (mapcar #'deserialize-elt default))))

;;; jso

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" "array")) fullname->schema enclosing-namespace)
      (multiple-value-bind (items itemsp)
          (st-json:getjso "items" jso)
        (if itemsp
            (let ((items (internal:read-jso
                          items fullname->schema enclosing-namespace)))
              (make-instance 'api:array :items items))
            (make-instance 'api:array)))))

(defmethod internal:write-jso
    ((schema api:array) seen canonical-form-p)
  (st-json:jso
   "type" "array"
   "items" (internal:write-jso (api:items schema) seen canonical-form-p)))
