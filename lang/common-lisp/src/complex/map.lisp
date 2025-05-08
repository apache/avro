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
(defpackage #:org.apache.avro.internal.map
  (:use #:cl)
  (:shadow #:hash-table)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:mop #:org.apache.avro.internal.mop)
   (#:bytes #:org.apache.avro.internal.bytes)
   (#:long #:org.apache.avro.internal.long))
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>)
  (:import-from #:org.apache.avro.internal.recursive-descent.pattern
                #:define-pattern-method)
  (:export #:backed-by-hash-table
           #:hash-table))
(in-package #:org.apache.avro.internal.map)

;;; map

(defclass api:map (api:complex-schema)
  ((values
    :initarg :values
    :reader api:values
    :late-type api:schema
    :documentation "Map schema values type."))
  (:metaclass mop:schema-class)
  (:scalars :values)
  (:object-class api:map-object)
  (:default-initargs
   :values (error "Must supply VALUES"))
  (:documentation
   "Metaclass of avro map schemas."))

(defmethod closer-mop:validate-superclass
    ((class api:map) (superclass api:complex-schema))
  t)

;;; backed-by-hash-table

(defclass backed-by-hash-table ()
  ((hash-table
    :accessor hash-table
    :type cl:hash-table)))

;;; map-object

(defclass api:map-object (backed-by-hash-table api:complex-object)
  ((hash-table :reader api:raw :documentation "The underlying hash-table."))
  (:documentation
   "Base class for instances of an avro map schema."))

(defmethod initialize-instance :after
    ((instance api:map-object) &key size rehash-size rehash-threshold)
  (let ((keyword-args (list :test #'equal)))
    (when size
      (push size keyword-args)
      (push :size keyword-args))
    (when rehash-size
      (push rehash-size keyword-args)
      (push :rehash-size keyword-args))
    (when rehash-threshold
      (push rehash-threshold keyword-args)
      (push :rehash-threshold keyword-args))
    (setf (hash-table instance) (apply #'make-hash-table keyword-args))))

(defmethod api:hash-table-count
    ((object api:map-object))
  "Return the number of entries in OBJECT, like hash-table-count."
  (hash-table-count (hash-table object)))

(defmethod api:hash-table-size
    ((object api:map-object))
  "Return the underlying size of OBJECT, like hash-table-size."
  (hash-table-size (hash-table object)))

(defmethod api:clrhash
    ((object api:map-object))
  "Clear the entries from OBJECT before returning OBJECT, like clrhash."
  (prog1 object
    (clrhash (hash-table object))))

(defmethod api:maphash
    ((function function) (object api:map-object))
  "Call FUNCTION for each entry in OBJECT, like maphash."
  (maphash function (hash-table object)))

(defmethod api:gethash
    ((key string) (object api:map-object) &optional default)
  "Return the value associated with KEY or DEFAULT if not found, like gethash."
  (gethash key (hash-table object) default))

(defmethod (setf api:gethash)
    (value (key string) (object api:map-object))
  "Assign the value associated with KEY, like (setf gethash)."
  (assert (typep value (api:values (class-of object))))
  (setf (gethash key (hash-table object)) value))

(defmethod api:remhash
    ((key string) (object api:map-object))
  "Remove the entry associated with KEY, returning t if found or nil otherwise."
  (remhash key (hash-table object)))

;;; serialized-size

(defmethod internal:fixed-size
    ((schema api:map))
  (declare (ignore schema))
  nil)

(declaim
 (ftype (function (cl:hash-table api:schema) (values bytes:size &optional))
        %serialized-size))
(defun %serialized-size (hash-table values)
  (let ((fixed-size (internal:fixed-size values)))
    (declare ((or null bytes:size) fixed-size))
    (if fixed-size
        (loop
          with serialized-size-of-values of-type bytes:size
            = (* fixed-size (hash-table-count hash-table))

          for key of-type api:string being the hash-keys of hash-table
          for key-size of-type bytes:size = (api:serialized-size key)
          sum key-size into serialized-size-of-keys of-type bytes:size

          finally
             (return
               (+ serialized-size-of-keys serialized-size-of-values)))
        (loop
          for key of-type api:string being the hash-keys of hash-table
          for value of-type api:object being the hash-values of hash-table

          for key-size of-type bytes:size = (api:serialized-size key)
          for value-size of-type bytes:size = (api:serialized-size value)

          sum key-size into sum of-type bytes:size
          sum value-size into sum of-type bytes:size

          finally
             (return
               sum)))))

(declaim
 (ftype (function (cl:hash-table api:schema) (values bytes:size &optional))
        serialized-size))
(defun serialized-size (hash-table values)
  (let ((count (hash-table-count hash-table)))
    (if (zerop count)
        1
        (let* ((count (long:serialized-size (- count)))
               (serialized-size (%serialized-size hash-table values))
               (size (long:serialized-size serialized-size)))
          (+ count size serialized-size 1)))))

(defmethod api:serialized-size
    ((object api:map-object))
  (serialized-size (hash-table object) (api:values (class-of object))))

;;; serialize-into-vector

(declaim
 (ftype (function (api:schema cl:hash-table vector<uint8> ufixnum)
                  (values ufixnum &optional))
        serialize-into-vector))
(defun serialize-into-vector (values hash-table into start)
  (let ((count (hash-table-count hash-table))
        (bytes-written 0))
    (declare (ufixnum bytes-written))
    (unless (zerop count)
      (loop
        initially
           (incf bytes-written
                 (long:serialize-into-vector (- count) into start))
           (incf bytes-written
                 (long:serialize-into-vector
                  (%serialized-size hash-table values)
                  into
                  (+ start bytes-written)))

        for key of-type api:string being the hash-keys of hash-table
        for value of-type api:object being the hash-values of hash-table

        do
           (incf bytes-written
                 (internal:serialize key into :start (+ start bytes-written)))
           (incf bytes-written
                 (internal:serialize
                  value into :start (+ start bytes-written)))))
    (+ bytes-written
       (long:serialize-into-vector 0 into (+ start bytes-written)))))

(defmethod internal:serialize
    ((object api:map-object) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (serialize-into-vector
   (api:values (class-of object)) (hash-table object) into start))

;;; serialize-into-stream

(declaim
 (ftype (function (api:schema cl:hash-table stream) (values ufixnum &optional))
        serialize-into-stream))
(defun serialize-into-stream (values hash-table into)
  (let ((count (hash-table-count hash-table))
        (bytes-written 0))
    (declare (ufixnum bytes-written))
    (unless (zerop count)
      (loop
        initially
           (incf bytes-written (long:serialize-into-stream (- count) into))
           (incf bytes-written (long:serialize-into-stream
                                (%serialized-size hash-table values) into))

        for key of-type api:string being the hash-keys of hash-table
        for value of-type api:object being the hash-values of hash-table

        do
           (incf bytes-written (internal:serialize key into))
           (incf bytes-written (internal:serialize value into))))
    (+ bytes-written (long:serialize-into-stream 0 into))))

(defmethod internal:serialize
    ((object api:map-object) (into stream) &key)
  (serialize-into-stream
   (api:values (class-of object)) (hash-table object) into))

;;; serialize

(defmethod api:serialize
    ((object api:map-object)
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
                  (values cl:hash-table ufixnum &optional))
        deserialize-from-vector))
(defun deserialize-from-vector (schema input start)
  (loop
    with total-bytes-read of-type ufixnum = 0
    and output = (make-hash-table :test #'equal)

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
         for key of-type api:string
           = (multiple-value-bind (key bytes-read)
                 (api:deserialize
                  'api:string input :start (+ start total-bytes-read))
               (declare (ufixnum bytes-read))
               (incf total-bytes-read bytes-read)
               key)
         for value of-type api:object
           = (multiple-value-bind (value bytes-read)
                 (api:deserialize
                  schema input :start (+ start total-bytes-read))
               (declare (ufixnum bytes-read))
               (incf total-bytes-read bytes-read)
               value)
         do (setf (gethash key output) value))

    finally
       (return
         (values output total-bytes-read))))

(defmethod api:deserialize
    ((schema api:map) (input vector) &key (start 0))
  (declare (vector<uint8> input)
           (ufixnum start))
  (multiple-value-bind (hash-table bytes-read)
      (deserialize-from-vector (api:values schema) input start)
    (let ((backed-by-hash-table (make-instance 'backed-by-hash-table)))
      (setf (hash-table backed-by-hash-table) hash-table)
      (values (change-class backed-by-hash-table schema) bytes-read))))

;;; deserialize-from-stream

(declaim
 (ftype (function (api:schema stream) (values cl:hash-table ufixnum &optional))
        deserialize-from-stream))
(defun deserialize-from-stream (schema input)
  (loop
    with total-bytes-read of-type ufixnum = 0
    and output = (make-hash-table :test #'equal)

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
         for key of-type api:string
           = (multiple-value-bind (key bytes-read)
                 (api:deserialize 'api:string input)
               (declare (ufixnum bytes-read))
               (incf total-bytes-read bytes-read)
               key)
         for value of-type api:object
           = (multiple-value-bind (value bytes-read)
                 (api:deserialize schema input)
               (declare (ufixnum bytes-read))
               (incf total-bytes-read bytes-read)
               value)
         do (setf (gethash key output) value))

    finally
       (return
         (values output total-bytes-read))))

(defmethod api:deserialize
    ((schema api:map) (input stream) &key)
  (multiple-value-bind (hash-table bytes-read)
      (deserialize-from-stream (api:values schema) input)
    (let ((backed-by-hash-table (make-instance 'backed-by-hash-table)))
      (setf (hash-table backed-by-hash-table) hash-table)
      (values (change-class backed-by-hash-table schema) bytes-read))))

;;; compare

;; maps cannot be compared, but they can be skipped in records

(defmethod internal:skip
    ((schema api:map) (input vector) &optional (start 0))
  (declare (vector<uint8> input)
           (ufixnum start))
  (loop
    with values of-type api:schema = (api:values schema)
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
        do
           (incf total-bytes-read
                 (internal:skip 'api:string input (+ start total-bytes-read)))
           (incf total-bytes-read
                 (internal:skip values input (+ start total-bytes-read))))

    finally
       (return
         total-bytes-read)))

(defmethod internal:skip
    ((schema api:map) (input stream) &optional start)
  (declare (ignore start))
  (loop
    with values of-type api:schema = (api:values schema)
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
        do
           (incf total-bytes-read (internal:skip 'api:string input))
           (incf total-bytes-read (internal:skip values input)))

    finally
       (return
         total-bytes-read)))

;;; coerce

(declaim
 (ftype (function (cl:hash-table api:schema) (values &optional))
        coerce-hash-table))
(defun coerce-hash-table (hash-table values)
  (loop
    for key of-type api:string being the hash-keys of hash-table
    for value of-type api:object being the hash-values of hash-table

    do (setf (gethash key hash-table) (api:coerce value values)))
  (values))

(defmethod api:coerce
    ((object api:map-object) (schema api:map))
  (if (eq (class-of object) schema)
      object
      (let ((values (api:values schema)))
        (unless (eq (api:values (class-of object)) values)
          (coerce-hash-table (hash-table object) values))
        (change-class object schema))))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:map-object))
  (let ((jso (st-json:jso)))
    (flet ((fill-jso (key value)
             (setf (st-json:getjso key jso)
                   (internal:serialize-field-default value))))
      (api:maphash #'fill-jso default))
    jso))

(defmethod internal:deserialize-field-default
    ((schema api:map) (default st-json:jso))
  (let ((map (make-instance schema)))
    (flet ((fill-map (key value)
             (setf (api:gethash key map)
                   (internal:deserialize-field-default
                    (api:values schema) value))))
      (st-json:mapjso #'fill-map default))
    map))

;;; jso

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" "map")) fullname->schema enclosing-namespace)
      (multiple-value-bind (values valuesp)
          (st-json:getjso "values" jso)
        (if valuesp
            (let ((values (internal:read-jso
                           values fullname->schema enclosing-namespace)))
              (make-instance 'api:map :values values))
            (make-instance 'api:map)))))

(defmethod internal:write-jso
    ((schema api:map) seen canonical-form-p)
  (st-json:jso
   "type" "map"
   "values" (internal:write-jso (api:values schema) seen canonical-form-p)))
