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
(defpackage #:org.apache.avro.internal.enum
  (:use #:cl)
  (:shadow #:position)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:mop #:org.apache.avro.internal.mop)
   (#:name #:org.apache.avro.internal.name)
   (#:intern #:org.apache.avro.internal.intern))
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>
                #:comparison)
  (:import-from #:org.apache.avro.internal.recursive-descent.pattern
                #:define-pattern-method))
(in-package #:org.apache.avro.internal.enum)

;;; enum

(deftype position ()
  '(and api:int (integer 0)))

(deftype position? ()
  '(or null position))

(deftype array<name> ()
  '(simple-array name:name (*)))

(defclass api:enum (name:named-schema)
  ((symbols
    :initarg :symbols
    :reader api:symbols
    :type array<name>
    :documentation "Symbols for enum.")
   (default
    :initarg :default
    :type position?
    :documentation "Position of enum default."))
  (:metaclass mop:schema-class)
  (:scalars :default)
  (:object-class api:enum-object)
  (:default-initargs
   :symbols (error "Must supply SYMBOLS")
   :default nil)
  (:documentation
   "Metaclass of avro enum schemas."))

(defmethod closer-mop:validate-superclass
    ((class api:enum) (superclass name:named-schema))
  t)

(declaim (ftype (function (t) (values name:name &optional)) parse-symbol))
(defun parse-symbol (symbol)
  (check-type symbol name:name)
  symbol)

(declaim
 (ftype (function (sequence) (values array<name> &optional)) parse-symbols))
(defun parse-symbols (symbols)
  (assert (plusp (length symbols)) (symbols) "Symbols cannot be empty")
  (assert (null (internal:duplicates symbols)) (symbols))
  (map 'array<name> #'parse-symbol symbols))

(declaim
 (ftype (function (array<name> name:name) (values position? &optional))
        default-position))
(defun default-position (symbols default)
  (cl:position default symbols :test #'string=))

(declaim
 (ftype (function (array<name> (or null name:name position))
                  (values position? &optional))
        parse-default))
(defun parse-default (symbols default)
  (when default
    (etypecase default
      (name:name
       (assert (default-position symbols default) (default)
               "Default ~S not found in symbols ~S" default symbols)
       (default-position symbols default))
      (position
       (assert (< default (length symbols)) (default)
               "Default position ~S overindexes ~S" default symbols)
       default))))

(mop:definit ((instance api:enum) :around &rest initargs &key symbols default)
  (let* ((symbols (parse-symbols symbols))
         (default (parse-default symbols (mop:scalarize default))))
    (setf (getf initargs :symbols) symbols
          (getf initargs :default) default))
  (apply #'call-next-method instance initargs))

(defmethod api:default
    ((instance api:enum))
  "Return (values default position)."
  (with-slots (default symbols) instance
    (if default
        (values (elt symbols default) default)
        (values nil nil))))

;;; %enum-object

(defclass %enum-object ()
  ((position
    :accessor position
    :type position
    :documentation "Position of chosen enum.")))

;;; enum-object

(defclass api:enum-object (%enum-object api:complex-object)
  ()
  (:documentation
   "Base class for instances of an avro enum schema."))

(defmethod initialize-instance :after
    ((instance api:enum-object) &key (enum (error "Must supply ENUM")))
  (let* ((symbols (api:symbols (class-of instance)))
         position)
    (declare (array<name> symbols)
             (position? position))
    (assert (setf position (cl:position enum symbols :test #'string=)) (enum)
            "Enum ~S must be one of ~S" enum symbols)
    (setf (slot-value instance 'position) position)))

(defmethod api:which-one
    ((instance api:enum-object))
  "Return (values enum-string position)"
  (let* ((position (position instance))
         (symbols (api:symbols (class-of instance)))
         (symbol (elt symbols position)))
    (declare (position position)
             (array<name> symbols))
    (values symbol position)))

;;; serialized-size

(defmethod internal:fixed-size
    ((schema api:enum))
  (let ((symbols (api:symbols schema)))
    (declare (array<name> symbols))
    (when (<= (length symbols) 64)
      1)))

(defmethod api:serialized-size
    ((object api:enum-object))
  (let ((position (position object)))
    (declare (position position))
    (api:serialized-size position)))

;;; serialize

(defmethod internal:serialize
    ((object api:enum-object) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (let ((position (position object)))
    (declare (position position))
    (internal:serialize position into :start start)))

(defmethod internal:serialize
    ((object api:enum-object) (into stream) &key)
  (let ((position (position object)))
    (declare (position position))
    (internal:serialize position into)))

(defmethod api:serialize
    ((object api:enum-object)
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
    ((schema api:enum) (input vector) &key (start 0))
  (declare (vector<uint8> input)
           (ufixnum start))
  (multiple-value-bind (position bytes-read)
      (api:deserialize 'api:int input :start start)
    (declare (position position)
             (ufixnum bytes-read))
    (let ((%enum-object (make-instance '%enum-object)))
      (setf (position %enum-object) position)
      (values (change-class %enum-object schema) bytes-read))))

(defmethod api:deserialize
    ((schema api:enum) (input stream) &key)
  (multiple-value-bind (position bytes-read)
      (api:deserialize 'api:int input)
    (declare (position position)
             (ufixnum bytes-read))
    (let ((%enum-object (make-instance '%enum-object)))
      (setf (position %enum-object) position)
      (values (change-class %enum-object schema) bytes-read))))

;;; compare

(defmethod internal:skip
    ((schema api:enum) (input vector) &optional (start 0))
  (declare (ignore schema))
  (internal:skip 'api:int input start))

(defmethod internal:skip
    ((schema api:enum) (input stream) &optional start)
  (declare (ignore schema start))
  (internal:skip 'api:int input))

(defmethod api:compare
    ((schema api:enum) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (ignore schema)
           (vector<uint8> left right)
           (ufixnum left-start right-start))
  (the (values comparison ufixnum ufixnum &optional)
       (api:compare
        'api:int left right :left-start left-start :right-start right-start)))

(defmethod api:compare
    ((schema api:enum) (left stream) (right stream) &key)
  (declare (ignore schema))
  (the (values comparison ufixnum ufixnum &optional)
       (api:compare 'api:int left right)))

;;; coerce

(defmethod api:coerce
    ((object api:enum-object) (schema api:enum))
  (if (eq (class-of object) schema)
      object
      (progn
        (name:assert-matching-names schema (class-of object))
        (let* ((chosen (api:which-one object))
               (reader-symbols (api:symbols schema))
               (position
                 (or (cl:position chosen reader-symbols :test #'string=)
                     (nth-value 1 (api:default schema)))))
          (declare (name:name chosen)
                   (array<name> reader-symbols)
                   (position? position))
          (assert position nil
                  "Reader enum has no default for unknown writer symbol ~S"
                  chosen)
          (setf (position object) position)
          (change-class object schema)))))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:enum-object))
  (api:which-one default))

(defmethod internal:deserialize-field-default
    ((schema api:enum) (default string))
  (make-instance schema :enum default))

;;; jso

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" "enum")) fullname->schema enclosing-namespace)
      (internal:with-initargs
          (name namespace aliases (doc :documentation) symbols default) jso
        (push enclosing-namespace initargs)
        (push :enclosing-namespace initargs)
        (push (make-symbol (st-json:getjso "name" jso)) initargs)
        (push :name initargs)
        (let* ((schema (apply #'make-instance 'api:enum initargs))
               (fullname (api:fullname schema)))
          (assert (not (gethash fullname fullname->schema)) ()
                  "Name ~S is already taken" fullname)
          (setf (gethash fullname fullname->schema) schema)))))

(defmethod internal:write-jso
    ((schema api:enum) seen canonical-form-p)
  (declare (ignore seen))
  (let ((initargs (list "symbols" (api:symbols schema)))
        (documentation (documentation schema t))
        (default (api:default schema)))
    (unless canonical-form-p
      (when documentation
        (push documentation initargs)
        (push "doc" initargs))
      (when default
        (push default initargs)
        (push "default" initargs)))
    initargs))

;;; intern

(defmethod api:intern ((instance api:enum) &key null-namespace)
  (declare (ignore null-namespace))
  (let* ((namespace (concatenate
                     'string
                     (package-name intern:*intern-package*)
                     "." (api:name instance)))
         (package (or (find-package namespace) (make-package namespace))))
    (loop
      for symbol-name across (api:symbols instance)
      for symbol = (intern symbol-name package)
      for object = (make-instance instance :enum symbol-name)
      do
         (export symbol package)
         (setf (symbol-value symbol) object))))
