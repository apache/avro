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
(defpackage #:org.apache.avro.internal.union
  (:use #:cl)
  (:shadow #:union
           #:position)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:mop #:org.apache.avro.internal.mop)
   (#:name #:org.apache.avro.internal.name)
   (#:int #:org.apache.avro.internal.int))
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>
                #:comparison)
  (:import-from #:org.apache.avro.internal.compare
                #:compare-reals))
(in-package #:org.apache.avro.internal.union)

;;; wrapper

(deftype position ()
  '(and api:int (integer 0)))

(defclass wrapper-class (standard-class)
  ((position
    :initarg :position
    :reader position
    :type position
    :documentation "Position of chosen union schema."))
  (:metaclass mop:schema-class)
  (:scalars :position)
  (:object-class wrapper-object)
  (:default-initargs
   :position (error "Must supply POSITION")))

(defmethod closer-mop:validate-superclass
    ((class wrapper-class) (superclass standard-class))
  t)

(defclass wrapper-object ()
  ((wrapped-object
    :initarg :wrap
    :accessor unwrap
    :documentation "Wrapped union object."))
  (:default-initargs
   :wrap (error "Must supply WRAP")))

;;; union

(deftype array<schema> ()
  '(simple-array api:schema (*)))

(deftype schema? ()
  '(or api:schema symbol))

(deftype array<schema?> ()
  '(simple-array schema? (*)))

(deftype array<wrapper-class> ()
  '(simple-array wrapper-class (*)))

(defclass api:union (api:complex-schema)
  ((schemas
    :initarg :schemas
    :reader api:schemas
    :late-type array<schema>
    :early-type array<schema?>
    :documentation "Schemas for union.")
   (wrapper-classes
    :reader wrapper-classes
    :type array<wrapper-class>))
  (:metaclass mop:schema-class)
  (:object-class api:union-object)
  (:default-initargs
   :schemas (error "Must supply SCHEMAS"))
  (:documentation
   "Metaclass of avro union schemas."))

(defmethod closer-mop:validate-superclass
    ((class api:union) (superclass api:complex-schema))
  t)

(declaim (ftype (function (t) (values schema? &optional)) parse-early-schema))
(defun parse-early-schema (schema?)
  (check-type schema? schema?)
  schema?)

(declaim
 (ftype (function (sequence) (values array<schema?> &optional))
        parse-early-schemas))
(defun parse-early-schemas (early-schemas?)
  (assert (plusp (length early-schemas?))
          (early-schemas?)
          "Schemas cannot be empty: ~S"
          early-schemas?)
  (map 'array<schema?> #'parse-early-schema early-schemas?))

(mop:definit ((instance api:union) :around &rest initargs &key schemas)
  (setf (getf initargs :schemas) (parse-early-schemas schemas))
  (apply #'call-next-method instance initargs))

(deftype schema-key ()
  '(or symbol name:valid-fullname))

(declaim
 (ftype (function (api:schema) (values schema-key &optional)) schema-key))
(defun schema-key (schema)
  (if (symbolp schema)
      schema
      (if (subtypep (class-of schema) 'name:named-schema)
          (api:fullname schema)
          (type-of schema))))

(declaim
 (ftype (function (array<schema>) (values &optional)) assert-valid-schemas))
(defun assert-valid-schemas (schemas)
  (let ((seen (make-hash-table :test #'equal :size (length schemas))))
    (labels
        ((assert-unique (schema)
           (let ((key (schema-key schema)))
             (if (gethash key seen)
                 (error "Duplicate ~S schema in union" key)
                 (setf (gethash key seen) t))))
         (assert-valid (schema)
           (if (subtypep (class-of schema) 'api:union)
               (error "Nested union schema: ~S" schema)
               (assert-unique schema))))
      (map nil #'assert-valid schemas)))
  (values))

(declaim
 (ftype (function (schema?) (values api:schema &optional)) parse-schema))
(defun parse-schema (schema?)
  (let ((schema
          (if (and (symbolp schema?)
                   (not (typep schema? 'api:schema)))
              (find-class schema?)
              schema?)))
    (check-type schema api:schema)
    schema))

(declaim
 (ftype (function (array<schema?>) (values array<schema> &optional))
        parse-schemas))
(defun parse-schemas (schemas?)
  (let ((schemas (map 'array<schema> #'parse-schema schemas?)))
    (assert-valid-schemas schemas)
    schemas))

(declaim
 (ftype (function (array<schema>) (values array<wrapper-class> &optional))
        make-wrapper-classes))
(defun make-wrapper-classes (schemas)
  (loop
    with wrapper-classes = (make-array (length schemas)
                                       :element-type 'wrapper-class)

    for schema across schemas
    for i from 0
    for wrapper-class = (make-instance 'wrapper-class :position i)
    do (setf (elt wrapper-classes i) wrapper-class)

    finally
       (return
         wrapper-classes)))

(defmethod mop:early->late
    ((class api:union) (name (eql 'schemas)) type value)
  (with-slots (schemas wrapper-classes) class
    (setf schemas (parse-schemas schemas)
          wrapper-classes (make-wrapper-classes schemas))
    schemas))

;;; %union-object

(defclass %union-object ()
  ((wrapped-object
    :accessor wrapped-object
    :type wrapper-object)))

;;; union-object

(defclass api:union-object (%union-object api:complex-object)
  ()
  (:documentation
   "Base class for instances of an avro union schema."))

(defgeneric api:object (union-object)
  (:method ((instance api:union-object))
    "Return the chosen union object."
    (unwrap (wrapped-object instance))))

(defmethod api:which-one
    ((instance api:union-object))
  "Return (values schema-name position schema)."
  (let* ((position (position (class-of (wrapped-object instance))))
         (schemas (api:schemas (class-of instance)))
         (schema (elt schemas position))
         (schema-name (schema-key schema)))
    (declare (array<schema> schemas))
    (values schema-name position schema)))

(declaim
 (ftype (function (api:union t) (values (or null wrapper-class) &optional))
        find-wrapper-class))
(defun find-wrapper-class (union object)
  (let* ((schemas (api:schemas union))
         (wrapper-classes (wrapper-classes union))
         (position (cl:position object schemas :test #'typep)))
    (declare (array<schema> schemas)
             (array<wrapper-class> wrapper-classes))
    (when position
      (elt wrapper-classes position))))

(declaim
 (ftype (function (api:union t) (values wrapper-object &optional)) wrap))
(defun wrap (union object)
  (let ((wrapper-class (find-wrapper-class union object)))
    (unless wrapper-class
      (error "Object ~S must be one of ~S" object (api:schemas union)))
    (make-instance wrapper-class :wrap object)))

(defmethod initialize-instance :after
    ((instance api:union-object) &key (object (error "Must supply OBJECT")))
  (setf (wrapped-object instance) (wrap (class-of instance) object)))

;;; serialized-size

(defmethod internal:fixed-size
    ((schema api:union))
  (let ((sizes (map 'list #'internal:fixed-size (api:schemas schema))))
    (when (and (every #'identity sizes)
               (apply #'= sizes))
      (first sizes))))

(defmethod api:serialized-size
    ((object api:union-object))
  (let ((position (nth-value 1 (api:which-one object))))
    (+ (api:serialized-size position)
       (api:serialized-size (api:object object)))))

;;; serialize

(defmethod internal:serialize
    ((object api:union-object) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (let* ((position (nth-value 1 (api:which-one object)))
         (bytes-written (internal:serialize position into :start start))
         (object (api:object object)))
    (declare (ufixnum bytes-written))
    (+ bytes-written
       (internal:serialize object into :start (+ start bytes-written)))))

(defmethod internal:serialize
    ((object api:union-object) (into stream) &key)
  (let* ((position (nth-value 1 (api:which-one object)))
         (bytes-written (internal:serialize position into))
         (object (api:object object)))
    (declare (ufixnum bytes-written))
    (+ bytes-written (internal:serialize object into))))

(defmethod api:serialize
    ((object api:union-object)
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
    ((schema api:union) (input vector) &key (start 0))
  (declare (vector<uint8> input)
           (ufixnum start))
  (let ((schemas (api:schemas schema))
        (wrapper-classes (wrapper-classes schema)))
    (declare (array<schema> schemas)
             (array<wrapper-class> wrapper-classes))
    (multiple-value-bind (position bytes-read)
        (api:deserialize 'api:int input :start start)
      (declare (api:int position))
      (let ((chosen-schema (elt schemas position))
            (chosen-wrapper-class (elt wrapper-classes position)))
        (multiple-value-bind (object more-bytes-read)
            (api:deserialize chosen-schema input :start (+ start bytes-read))
          (let ((%union-object (make-instance '%union-object)))
            (setf (wrapped-object %union-object)
                  (make-instance chosen-wrapper-class :wrap object))
            (values (change-class %union-object schema)
                    (+ bytes-read more-bytes-read))))))))

(defmethod api:deserialize
    ((schema api:union) (input stream) &key)
  (let ((schemas (api:schemas schema))
        (wrapper-classes (wrapper-classes schema)))
    (declare (array<schema> schemas)
             (array<wrapper-class> wrapper-classes))
    (multiple-value-bind (position bytes-read)
        (api:deserialize 'api:int input)
      (declare (api:int position))
      (let ((chosen-schema (elt schemas position))
            (chosen-wrapper-class (elt wrapper-classes position)))
        (multiple-value-bind (object more-bytes-read)
            (api:deserialize chosen-schema input)
          (let ((%union-object (make-instance '%union-object)))
            (setf (wrapped-object %union-object)
                  (make-instance chosen-wrapper-class :wrap object))
            (values (change-class %union-object schema)
                    (+ bytes-read more-bytes-read))))))))

;;; compare

(defmethod internal:skip
    ((schema api:union) (input vector) &optional (start 0))
  (declare (vector<uint8> input)
           (ufixnum start))
  (multiple-value-bind (position bytes-read)
      (int:deserialize-from-vector input start)
    (let* ((schemas (api:schemas schema))
           (chosen-schema (elt schemas position))
           (start (+ start bytes-read)))
      (declare (array<schema> schemas)
               (ufixnum start))
      (+ bytes-read (internal:skip chosen-schema input start)))))

(defmethod internal:skip
    ((schema api:union) (input stream) &optional start)
  (declare (ignore start))
  (multiple-value-bind (position bytes-read)
      (int:deserialize-from-stream input)
    (let* ((schemas (api:schemas schema))
           (chosen-schema (elt schemas position)))
      (declare (array<schema> schemas))
      (+ bytes-read (internal:skip chosen-schema input)))))

(defmethod api:compare
    ((schema api:union) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (vector<uint8> left right)
           (ufixnum left-start right-start))
  (multiple-value-bind (left-position left-bytes-read)
      (api:deserialize 'api:int left :start left-start)
    (declare (position left-position)
             (ufixnum left-bytes-read))
    (multiple-value-bind (right-position right-bytes-read)
        (api:deserialize 'api:int right :start right-start)
      (declare (position right-position)
               (ufixnum right-bytes-read))
      (let ((comparison (compare-reals left-position right-position)))
        (if (not (zerop comparison))
            (values comparison left-bytes-read right-bytes-read)
            (let* ((schemas (api:schemas schema))
                   (chosen-schema (elt schemas left-position)))
              (declare (array<schema> schemas))
              (multiple-value-bind
                    (comparison more-left-bytes-read more-right-bytes-read)
                  (api:compare chosen-schema left right
                               :left-start (+ left-start left-bytes-read)
                               :right-start (+ right-start right-bytes-read))
                (declare (comparison comparison)
                         (ufixnum more-left-bytes-read more-right-bytes-read))
                (values comparison
                        (+ left-bytes-read more-left-bytes-read)
                        (+ right-bytes-read more-right-bytes-read)))))))))

(defmethod api:compare
    ((schema api:union) (left stream) (right stream) &key)
  (multiple-value-bind (left-position left-bytes-read)
      (api:deserialize 'api:int left)
    (declare (position left-position)
             (ufixnum left-bytes-read))
    (multiple-value-bind (right-position right-bytes-read)
        (api:deserialize 'api:int right)
      (declare (position right-position)
               (ufixnum right-bytes-read))
      (let ((comparison (compare-reals left-position right-position)))
        (if (not (zerop comparison))
            (values comparison left-bytes-read right-bytes-read)
            (let* ((schemas (api:schemas schema))
                   (chosen-schema (elt schemas left-position)))
              (declare (array<schema> schemas))
              (multiple-value-bind
                    (comparison more-left-bytes-read more-right-bytes-read)
                  (api:compare chosen-schema left right)
                (declare (comparison comparison)
                         (ufixnum more-left-bytes-read more-right-bytes-read))
                (values comparison
                        (+ left-bytes-read more-left-bytes-read)
                        (+ right-bytes-read more-right-bytes-read)))))))))

;;; coerce

(declaim
 (ftype (function (api:object array<schema>)
                  (values api:object (or null position) &optional))
        first-coercion))
(defun first-coercion (object schemas)
  (loop
    for i below (length schemas)
    for schema = (elt schemas i)

    do
       (handler-case
           (api:coerce object schema)
         (error ())
         (:no-error (coerced)
           (return-from first-coercion
             (values coerced i))))

    finally
       (return
         (values nil nil))))

(defmethod api:coerce
    ((object api:union-object) (schema api:union))
  (if (eq (class-of object) schema)
      object
      (multiple-value-bind (coerced position)
          (first-coercion (api:object object) (api:schemas schema))
        (unless position
          (error "None of the reader union's schemas match the writer schema"))
        (let* ((wrapper-classes (wrapper-classes schema))
               (chosen-wrapper-class (elt wrapper-classes position)))
          (change-class (wrapped-object object) chosen-wrapper-class)
          (setf (unwrap (wrapped-object object)) coerced)
          (change-class object schema)))))

(defmethod api:coerce
    (object (schema api:union))
  (multiple-value-bind (coerced position)
      (first-coercion object (api:schemas schema))
    (unless position
      (error "None of the reader union's schemas match the writer schema"))
    (let* ((wrapper-classes (wrapper-classes schema))
           (chosen-wrapper-class (elt wrapper-classes position))
           (%union-object (make-instance '%union-object)))
      (setf (wrapped-object %union-object)
            (make-instance chosen-wrapper-class :wrap coerced))
      (change-class %union-object schema))))

(defmethod api:coerce
    ((object api:union-object) schema)
  (api:coerce (api:object object) schema))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:union-object))
  (internal:serialize-field-default (api:object default)))

(defmethod internal:deserialize-field-default
    ((schema api:union) default)
  (let* ((first-schema (elt (api:schemas schema) 0))
         (default (internal:deserialize-field-default first-schema default)))
    (make-instance schema :object default)))

;;; jso

(defmethod internal:read-jso
    ((jso list) fullname->schema enclosing-namespace)
  (flet ((read-jso (jso)
           (internal:read-jso jso fullname->schema enclosing-namespace)))
    (make-instance `api:union :schemas (mapcar #'read-jso jso))))

(defmethod internal:write-jso
    ((schema api:union) seen canonical-form-p)
  (flet ((write-jso (schema)
           (internal:write-jso schema seen canonical-form-p)))
    (map '(simple-array simple-string (*)) #'write-jso (api:schemas schema))))
