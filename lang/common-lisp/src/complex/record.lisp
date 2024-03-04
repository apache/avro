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
(defpackage #:org.apache.avro.internal.record
  (:use #:cl)
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
                #:define-pattern-method)
  (:export #:read-field
           #:fields
           #:objects
           #:booleans
           #:make-object))
(in-package #:org.apache.avro.internal.record)

;;; field

;; TODO export this
(deftype order ()
  '(member api:ascending api:descending api:ignore))

(deftype alias ()
  'name:name)

(deftype array<alias> ()
  '(simple-array alias (*)))

(deftype array<alias>? ()
  '(or null array<alias>))

(defclass api:field (closer-mop:standard-direct-slot-definition)
  ((aliases
    :initarg :aliases
    :reader api:aliases
    :type array<alias>?
    :documentation "A vector of aliases if provided, otherwise nil.")
   (order
    :initarg :order
    :type order
    :documentation "Field ordering used during sorting.")
   (default
    :initarg :default
    :type t
    :documentation "Field default."))
  (:default-initargs
   :name (error "Must supply NAME")
   :type (error "Must supply TYPE"))
  (:documentation
   "Slot class for an avro record field."))

(defmethod api:order
    ((instance api:field))
  "Return (values order provided-p)."
  (let* ((orderp (slot-boundp instance 'order))
         (order (if orderp
                    (slot-value instance 'order)
                    'api:ascending)))
    (values order orderp)))

(defmethod api:default
    ((instance api:field))
  "Return (values default provided-p)."
  (let* ((defaultp (slot-boundp instance 'default))
         (default (when defaultp
                    (slot-value instance 'default))))
    (values default defaultp)))

(defmethod api:name
    ((instance api:field))
  "Return (values name-string slot-symbol)."
  (let ((name (closer-mop:slot-definition-name instance)))
    (declare (symbol name))
    (values (string name) name)))

(defmethod api:type
    ((instance api:field))
  "Field type."
  (closer-mop:slot-definition-type instance))

(declaim (ftype (function (t) (values alias &optional)) parse-alias))
(defun parse-alias (alias)
  (check-type alias alias)
  alias)

(declaim
 (ftype (function (sequence boolean) (values array<alias>? &optional))
        parse-aliases))
(defun parse-aliases (aliases aliasesp)
  (when aliasesp
    (assert (null (internal:duplicates aliases)) (aliases))
    (map 'array<alias> #'parse-alias aliases)))

(declaim
 (ftype (function (string) (values order &optional)) %parse-order))
(defun %parse-order (order)
  (let ((expected '("ascending" "descending" "ignore")))
    (assert (member order expected :test #'string=) (expected)))
  (nth-value 0 (find-symbol (string-upcase order) 'org.apache.avro)))

(declaim (ftype (function (t) (values order &optional)) parse-order))
(defun parse-order (order)
  (etypecase order
    (order order)
    (string (%parse-order order))))

(defmethod initialize-instance :around
    ((instance api:field)
     &rest initargs &key (aliases nil aliasesp) (order 'api:ascending orderp))
  (setf (getf initargs :aliases) (parse-aliases aliases aliasesp))
  (when orderp
    (setf (getf initargs :order) (parse-order order)))
  (apply #'call-next-method instance initargs))

(defmethod initialize-instance :after
    ((instance api:field) &key)
  (with-accessors
        ((name closer-mop:slot-definition-name)
         (initfunction closer-mop:slot-definition-initfunction)
         (initform closer-mop:slot-definition-initform)
         (allocation closer-mop:slot-definition-allocation)
         (type closer-mop:slot-definition-type))
      instance
    (check-type type (or api:schema symbol))
    (assert (typep (string name) 'name:name))
    (assert (null initfunction) ()
            "Did not expect an initform for slot ~S: ~S" name initform)
    (assert (eq allocation :instance) ()
            "Expected :INSTANCE allocation for slot ~S, not ~S"
            name allocation)))

;;; effective-field

(defclass effective-field
    (api:field closer-mop:standard-effective-slot-definition)
  ((default
    :type api:object)
   (readers
    :type list
    :accessor readers
    :reader internal:readers)
   (writers
    :type list
    :accessor writers
    :reader internal:writers)))

(defmethod initialize-instance :around
    ((instance effective-field)
     &rest initargs &key type (default nil defaultp))
  (when (and (symbolp type)
             (not (typep type 'api:schema)))
    (let ((schema (find-class type)))
      (setf type schema
            (getf initargs :type) schema)))
  (when defaultp
    (setf (getf initargs :default)
          (internal:deserialize-field-default type default)))
  (apply #'call-next-method instance initargs))

(defmethod initialize-instance :after
    ((instance effective-field) &key)
  (let ((type (api:type instance)))
    (check-type type api:schema)))

(declaim
 (ftype (function (effective-field t) (values &optional)) set-default-once))
(defun set-default-once (field default)
  (unless (slot-boundp field 'default)
    (setf (slot-value field 'default)
          (internal:deserialize-field-default (api:type field) default)))
  (values))

(declaim
 (ftype (function (effective-field order) (values &optional)) set-order-once))
(defun set-order-once (field order)
  (unless (slot-boundp field 'order)
    (setf (slot-value field 'order) order))
  (values))

(declaim
 (ftype (function (effective-field array<alias>) (values &optional))
        set-aliases-once))
(defun set-aliases-once (field aliases)
  ;; aliases slot is always bound
  (unless (slot-value field 'aliases)
    (setf (slot-value field 'aliases) aliases))
  (values))

;;; record

(deftype fields ()
  '(simple-array api:field (*)))

(defclass api:record (name:named-schema)
  ((fields
    :type fields
    :documentation "Record fields."))
  (:metaclass mop:schema-class)
  (:object-class api:record-object)
  (:documentation
   "Metaclass of avro record schemas."))

(defmethod closer-mop:validate-superclass
    ((class api:record) (superclass name:named-schema))
  t)

(defmethod closer-mop:direct-slot-definition-class
    ((class api:record) &key)
  (find-class 'api:field))

(defmethod closer-mop:effective-slot-definition-class
    ((class api:record) &key)
  (find-class 'effective-field))

(defmethod closer-mop:compute-effective-slot-definition
    ((class api:record) name slots)
  (let ((effective-slot (call-next-method))
        readers
        writers)
    (dolist (slot slots)
      (multiple-value-bind (default defaultp)
          (api:default slot)
        (when defaultp
          (set-default-once effective-slot default)))
      (multiple-value-bind (order orderp)
          (api:order slot)
        (when orderp
          (set-order-once effective-slot order)))
      (let ((aliases (api:aliases slot)))
        (when aliases
          (set-aliases-once effective-slot aliases)))
      (dolist (reader (closer-mop:slot-definition-readers slot))
        (push reader readers))
      (dolist (writer (closer-mop:slot-definition-writers slot))
        (push writer writers)))
    ;; delete-duplicates will discard the earlier duplicate, which
    ;; works fine because we're reversing afterwards
    (setf (readers effective-slot) (nreverse
                                    (delete-duplicates readers :test #'eq))
          (writers effective-slot) (nreverse
                                    (delete-duplicates writers :test #'equal)))
    effective-slot))

(defmethod (setf closer-mop:slot-value-using-class)
    (new-value (class api:record) object (slot api:field))
  (let ((type (api:type slot))
        (name (api:name slot)))
    (assert (typep new-value type) (new-value)
            "Expected type ~S for field ~S, but got type ~S instead: ~S"
            type name (type-of new-value) new-value))
  (call-next-method))

(defmethod api:fields
    ((instance api:record))
  "Return fields from INSTANCE."
  (closer-mop:ensure-finalized instance)
  (slot-value instance 'fields))

(deftype slot-names ()
  '(simple-array symbol (*)))

(declaim
 (ftype (function (api:record) (values slot-names &optional))
        ordered-slot-names))
(defun ordered-slot-names (instance)
  (let* ((slots (closer-mop:class-direct-slots instance))
         (names (mapcar #'closer-mop:slot-definition-name slots)))
    (make-array (length slots) :element-type 'symbol :initial-contents names)))

(declaim
 (ftype (function (list slot-names) (values fields &optional)) sort-slots))
(defun sort-slots (slots ordered-slot-names)
  (flet ((slot-position (slot)
           (let ((name (closer-mop:slot-definition-name slot)))
             (position name ordered-slot-names))))
    (let ((slots (make-array (length slots)
                             :element-type 'effective-field
                             :initial-contents slots)))
      (sort slots #'< :key #'slot-position))))

(defmethod closer-mop:finalize-inheritance :after
    ((instance api:record))
  (with-slots (fields) instance
    (let ((ordered-slot-names (ordered-slot-names instance))
          (slots (closer-mop:compute-slots instance)))
      (setf fields (sort-slots slots ordered-slot-names)))))

;;; record-object

(defclass api:record-object (api:complex-object)
  ()
  (:documentation
   "Base class for instances of an avro record schema."))

;;; serialized-size

(deftype ufixnum? ()
  '(or null ufixnum))

(defmethod internal:fixed-size
    ((schema api:record))
  (let* ((fields (api:fields schema))
         (types (map '(simple-array api:schema (*)) #'api:type fields))
         (sizes
           (map '(simple-array ufixnum? (*)) #'internal:fixed-size types)))
    (declare (fields fields))
    (when (every #'identity sizes)
      (reduce #'+ sizes :initial-value 0))))

(defmethod api:serialized-size
    ((object api:record-object))
  (flet ((add-size (total field)
           (let ((value (slot-value object (nth-value 1 (api:name field)))))
             (+ total (api:serialized-size value)))))
    (let ((fields (api:fields (class-of object))))
      (declare (fields fields))
      (reduce #'add-size fields :initial-value 0))))

;;; serialize

(defmethod internal:serialize
    ((object api:record-object) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (loop
    with fields of-type fields = (api:fields (class-of object))
    and bytes-written of-type ufixnum = 0

    for field across fields
    for value of-type api:object = (slot-value
                                    object (nth-value 1 (api:name field)))

    do (incf bytes-written
             (internal:serialize value into :start (+ start bytes-written)))

    finally
       (return
         bytes-written)))

(defmethod internal:serialize
    ((object api:record-object) (into stream) &key)
  (loop
    with fields of-type fields = (api:fields (class-of object))
    and bytes-written of-type ufixnum = 0

    for field across fields
    for value of-type api:object = (slot-value
                                    object (nth-value 1 (api:name field)))

    do (incf bytes-written (internal:serialize value into))

    finally
       (return
         bytes-written)))

(defmethod api:serialize
    ((object api:record-object)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (+ (if sp 10 0) (api:serialized-size object))
                         :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

;;; deserialize

(deftype objects ()
  '(simple-array api:object (*)))

(deftype booleans ()
  '(simple-array boolean (*)))

(declaim
 (ftype (function (api:record fields objects list booleans)
                  (values api:record-object &optional))
        %make-object))
(defun %make-object (schema fields values initargs initargp-vector)
  (loop
    with object = (apply #'make-instance schema initargs)

    for field across fields
    for value across values
    for initargp across initargp-vector

    when initargp do
      (setf (slot-value object (nth-value 1 (api:name field))) value)

    finally
       (return
         object)))

(declaim
 (ftype (function (api:record fields objects)
                  (values api:record-object &optional))
        make-object))
(defun make-object (schema fields values)
  (loop
    with initargs of-type list = nil
    and initargp-vector = (make-array (length fields)
                                      :element-type 'boolean
                                      :initial-element nil)

    for index below (length fields)
    for field = (elt fields index)
    for value = (elt values index)
    for initarg = (first (closer-mop:slot-definition-initargs field))

    if initarg do
      (push value initargs)
      (push initarg initargs)
    else do
      (setf (elt initargp-vector index) t)

    finally
       (return
         (%make-object schema fields values initargs initargp-vector))))

(defmethod api:deserialize
    ((schema api:record) (input vector) &key (start 0))
  (declare (vector<uint8> input)
           (ufixnum start))
  (loop
    with fields of-type fields = (api:fields schema)
    with values = (make-array (length fields) :element-type 'api:object)
    and total-bytes-read of-type ufixnum = 0

    for index below (length fields)
    for field = (elt fields index)
    for new-start of-type ufixnum = (+ start total-bytes-read)
    do
       (multiple-value-bind (value bytes-read)
           (api:deserialize (api:type field) input :start new-start)
         (declare (api:object value)
                  (ufixnum bytes-read))
         (incf total-bytes-read bytes-read)
         (setf (elt values index) value))

    finally
       (return
         (values (make-object schema fields values) total-bytes-read))))

(defmethod api:deserialize
    ((schema api:record) (input stream) &key)
  (loop
    with fields of-type fields = (api:fields schema)
    with values = (make-array (length fields) :element-type 'api:object)
    and total-bytes-read of-type ufixnum = 0

    for index below (length fields)
    for field = (elt fields index)
    do
       (multiple-value-bind (value bytes-read)
           (api:deserialize (api:type field) input)
         (declare (api:object value)
                  (ufixnum bytes-read))
         (incf total-bytes-read bytes-read)
         (setf (elt values index) value))

    finally
       (return
         (values (make-object schema fields values) total-bytes-read))))

;;; compare

(defmethod internal:skip
    ((schema api:record) (input vector) &optional (start 0))
  (declare (vector<uint8> input)
           (ufixnum start))
  (loop
    with fields of-type fields = (api:fields schema)
    and bytes-read of-type ufixnum = 0

    for field across fields
    for type of-type api:schema = (api:type field)
    for new-start of-type ufixnum = (+ start bytes-read)

    do (incf bytes-read (internal:skip type input new-start))

    finally
       (return
         bytes-read)))

(defmethod internal:skip
    ((schema api:record) (input stream) &optional start)
  (declare (ignore start))
  (loop
    with fields of-type fields = (api:fields schema)
    and bytes-read of-type ufixnum = 0

    for field across fields
    for type of-type api:schema = (api:type field)

    do (incf bytes-read (internal:skip type input))

    finally
       (return
         bytes-read)))

(defmethod api:compare
    ((schema api:record) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (vector<uint8> left right)
           (ufixnum left-start right-start))
  (loop
    with fields of-type fields = (api:fields schema)
    and left-bytes-read of-type ufixnum = 0
    and right-bytes-read of-type ufixnum = 0

    for field across fields
    for order of-type order = (api:order field)
    for type of-type api:schema = (api:type field)
    for left-new-start of-type ufixnum = (+ left-start left-bytes-read)
    for right-new-start of-type ufixnum = (+ right-start right-bytes-read)

    if (eq order 'api:ignore) do
      (incf left-bytes-read (internal:skip type left left-new-start))
      (incf right-bytes-read (internal:skip type right right-new-start))
    else do
      (multiple-value-bind (comparison left right)
          (api:compare
           type left right
           :left-start left-new-start :right-start right-new-start)
        (declare (comparison comparison)
                 (ufixnum left right))
        (incf left-bytes-read left)
        (incf right-bytes-read right)
        (unless (zerop comparison)
          (return-from api:compare
            (values (if (eq order 'api:ascending)
                        comparison
                        (- comparison))
                    left-bytes-read
                    right-bytes-read))))

    finally
       (return
         (values 0 left-bytes-read right-bytes-read))))

(defmethod api:compare
    ((schema api:record) (left stream) (right stream) &key)
  (loop
    with fields of-type fields = (api:fields schema)
    and left-bytes-read of-type ufixnum = 0
    and right-bytes-read of-type ufixnum = 0

    for field across fields
    for order of-type order = (api:order field)
    for type of-type api:schema = (api:type field)

    if (eq order 'api:ignore) do
      (incf left-bytes-read (internal:skip type left))
      (incf right-bytes-read (internal:skip type right))
    else do
      (multiple-value-bind (comparison left right)
          (api:compare type left right)
        (declare (comparison comparison)
                 (ufixnum left right))
        (incf left-bytes-read left)
        (incf right-bytes-read right)
        (unless (zerop comparison)
          (return-from api:compare
            (values (if (eq order 'api:ascending)
                        comparison
                        (- comparison))
                    left-bytes-read
                    right-bytes-read))))

    finally
       (return
         (values 0 left-bytes-read right-bytes-read))))

;;; coerce

(declaim
 (ftype (function (name:name fields) (values (or null api:field) &optional))
        find-field-by-name))
(defun find-field-by-name (name fields)
  (loop
    for field across fields
    for field-name of-type name:name = (api:name field)
    when (string= name field-name)
      return field))

(declaim
 (ftype (function (array<alias>? fields)
                  (values (or null api:field) &optional))
        find-field-by-alias))
(defun find-field-by-alias (aliases fields)
  (when aliases
    (loop
      for field across fields
      for field-name of-type name:name = (api:name field)
      when (find field-name aliases :test #'string=)
        return field)))

(declaim
 (ftype (function (api:field fields) (values (or null api:field) &optional))
        find-field))
(defun find-field (field fields)
  (or (find-field-by-name (api:name field) fields)
      (find-field-by-alias (api:aliases field) fields)))

(defmethod api:coerce
    ((object api:record-object) (schema api:record))
  (if (eq (class-of object) schema)
      object
      (loop
        with writer of-type api:schema = (class-of object)
        with writer-fields of-type fields = (api:fields writer)
        and reader-fields of-type fields = (api:fields schema)
        with values = (make-array (length reader-fields)
                                  :element-type 'api:object)

          initially
             (name:assert-matching-names schema writer)

        for index below (length reader-fields)
        for reader-field = (elt reader-fields index)
        for writer-field of-type (or null api:field)
          = (find-field reader-field writer-fields)

        if writer-field do
          (let* ((writer-slot-name (nth-value 1 (api:name writer-field)))
                 (writer-value (slot-value object writer-slot-name))
                 (reader-type (api:type reader-field)))
            (declare (symbol writer-slot-name)
                     (api:object writer-value)
                     (api:schema reader-type))
            (setf (elt values index) (api:coerce writer-value reader-type)))
        else do
          (multiple-value-bind (default defaultp)
              (api:default reader-field)
            (declare (api:object default)
                     (boolean defaultp))
            (unless defaultp
              (error "Writer field ~S does not exist and reader has no default"
                     (api:name reader-field)))
            (setf (elt values index) default))

        finally
           (return
             (loop
               ;; TODO maybe specialize change-class to do this field
               ;; level coercing
               with object = (change-class object schema)

               for field across reader-fields
               for value across values
               for name = (nth-value 1 (api:name field))
               do
                  (setf (slot-value object name) value)

               finally
                  (return
                    object))))))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:record-object))
  (let ((jso (st-json:jso)))
    (flet ((fill-jso (field)
             (multiple-value-bind (name slot-name)
                 (api:name field)
               (setf (st-json:getjso name jso)
                     (internal:serialize-field-default
                      (slot-value default slot-name))))))
      (map nil #'fill-jso (api:fields (class-of default))))
    jso))

(declaim (ftype (function (fields) (values hash-table &optional)) name->index))
(defun name->index (fields)
  (loop
    with name->index = (make-hash-table :test #'equal :size (length fields))

    for index below (length fields)
    for field = (elt fields index)
    for name of-type name:name = (api:name field)
    do
       (setf (gethash name name->index) index)

    finally
       (return
         name->index)))

(declaim
 (ftype (function ((or symbol string) t fields hash-table objects)
                  (values &optional))
        set-values))
(defun set-values (key value fields name->index values)
  (let* ((index (gethash (string key) name->index))
         (type (api:type (elt fields index)))
         (value (internal:deserialize-field-default type value)))
    (setf (elt values index) value))
  (values))

(defmethod internal:deserialize-field-default
    ((schema api:record) (default st-json:jso))
  (let* ((fields (api:fields schema))
         (name->index (name->index fields))
         (values (make-array (length fields) :element-type 'api:object)))
    (declare (fields fields))
    (flet ((set-values (key value)
             (set-values key value fields name->index values)))
      (st-json:mapjso #'set-values default))
    (make-object schema fields values)))

(defmethod internal:deserialize-field-default
    ((schema api:record) (default list))
  (let* ((fields (api:fields schema))
         (name->index (name->index fields))
         (values (make-array (length fields) :element-type 'api:object)))
    (declare (fields fields))
    (if (consp (first default))
        (set-values-from-alist fields name->index values default)
        (set-values-from-plist fields name->index values default))
    (make-object schema fields values)))

(declaim
 (ftype (function (fields hash-table objects cons) (values &optional))
        set-values-from-alist))
(defun set-values-from-alist (fields name->index values alist)
  (loop
    for (key . value) in alist
    do (set-values key value fields name->index values))
  (values))

(declaim
 (ftype (function (fields hash-table objects list) (values &optional))
        set-values-from-plist))
(defun set-values-from-plist (fields name->index values plist)
  (loop
    for remaining = plist then (cddr remaining)
    while remaining

    for key = (car remaining)
    for rest = (cdr remaining)
    for value = (car rest)

    unless rest do
      (error "Odd number of key-value pairs: ~S" plist)

    do (set-values key value fields name->index values))
  (values))

;;; jso

(defparameter api:*add-accessors-and-initargs-p* t
  "When non-nil, record and error deserialization adds field accessors
and initargs.")

(declaim
 (ftype (function (st-json:jso hash-table name:namespace)
                  (values list &optional))
        read-field))
(defun read-field (jso fullname->schema enclosing-namespace)
  (internal:with-initargs ((doc :documentation) order aliases default) jso
    (multiple-value-bind (name namep)
        (st-json:getjso "name" jso)
      (assert namep () "Field name must be provided.")
      (push (make-symbol name) initargs)
      (push :name initargs))
    (multiple-value-bind (type typep)
        (st-json:getjso "type" jso)
      (assert typep () "Field type must be provided.")
      (push (internal:read-jso type fullname->schema enclosing-namespace)
            initargs)
      (push :type initargs))
    (when api:*add-accessors-and-initargs-p*
      (let ((name (getf initargs :name)))
        (push (list name) initargs)
        (push :initargs initargs)
        (push (list name) initargs)
        (push :readers initargs)
        (push (list `(setf ,name)) initargs)
        (push :writers initargs)))
    initargs))

(declaim
 (ftype (function (st-json:jso api:record hash-table) (values list &optional))
        read-fields))
(defun read-fields (jso record fullname->schema)
  (let ((namespace (api:namespace record)))
    (flet ((read-field (jso)
             (read-field jso fullname->schema namespace)))
      (multiple-value-bind (fields fieldsp)
          (st-json:getjso "fields" jso)
        (assert fieldsp () "Record schema must provide an array of fields.")
        (mapcar #'read-field fields)))))

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" "record")) fullname->schema enclosing-namespace)
      (internal:with-initargs (name namespace aliases (doc :documentation)) jso
        (push enclosing-namespace initargs)
        (push :enclosing-namespace initargs)
        (push (make-symbol (st-json:getjso "name" jso)) initargs)
        (push :name initargs)
        (let* ((schema (apply #'make-instance 'api:record initargs))
               (fullname (api:fullname schema)))
          (assert (not (gethash fullname fullname->schema)) ()
                  "Name ~S is already taken" fullname)
          (setf (gethash fullname fullname->schema) schema
                (getf initargs :direct-slots) (read-fields
                                               jso schema fullname->schema))
          (apply #'reinitialize-instance schema initargs)))))

(defmethod internal:write-jso
    ((field api:field) seen canonical-form-p)
  (let ((initargs (list
                   "name" (api:name field)
                   "type" (internal:write-jso
                           (api:type field) seen canonical-form-p)))
        (aliases (api:aliases field))
        (documentation (documentation field t)))
    (unless canonical-form-p
      (when aliases
        (push aliases initargs)
        (push "aliases" initargs))
      (when documentation
        (push documentation initargs)
        (push "doc" initargs))
      (multiple-value-bind (order orderp)
          (api:order field)
        (when orderp
          ;; TODO move this downcase-symbol into api:order method
          (push (internal:downcase-symbol order) initargs)
          (push "order" initargs)))
      (multiple-value-bind (default defaultp)
          (api:default field)
        (when defaultp
          (push (internal:serialize-field-default default) initargs)
          (push "default" initargs))))
    (apply #'st-json:jso initargs)))

(declaim
 (ftype (function (api:record hash-table boolean)
                  (values (simple-array st-json:jso (*)) &optional))
        write-fields))
(defun write-fields (schema seen canonical-form-p)
  (flet ((write-field (field)
           (internal:write-jso field seen canonical-form-p)))
    (map '(simple-array st-json:jso (*)) #'write-field (api:fields schema))))

(defmethod internal:write-jso
    ((schema api:record) seen canonical-form-p)
  (let ((initargs (list "fields" (write-fields schema seen canonical-form-p)))
        (documentation (documentation schema t)))
    (unless canonical-form-p
      (when documentation
        (push documentation initargs)
        (push "doc" initargs)))
    initargs))

;;; intern

(defmethod api:intern ((instance api:record) &key null-namespace)
  (declare (ignore null-namespace))
  (loop
    with direct-slots = nil

    for field across (api:fields instance)
    for direct-slot = (list :name (closer-mop:slot-definition-name field)
                            :type (closer-mop:slot-definition-type field))
    do
       (multiple-value-bind (order orderp) (api:order field)
         (when orderp
           (push order direct-slot)
           (push :order direct-slot)))
       (multiple-value-bind (default defaultp) (api:default field)
         (when defaultp
           (push default direct-slot)
           (push :default direct-slot)))
       (let ((aliases (api:aliases field)))
         (when aliases
           (push aliases direct-slot)
           (push :aliases direct-slot)))
       (loop
         with initargs = nil
         for initarg in (closer-mop:slot-definition-initargs field)
         do
            (push (intern (symbol-name initarg) 'keyword) initargs)
         finally
            (push (nreverse initargs) direct-slot)
            (push :initargs direct-slot))
       (loop
         with readers = nil
         for symbol in (internal:readers field)
         for interned = (intern (symbol-name symbol) intern:*intern-package*)
         do
            (export interned intern:*intern-package*)
            (push interned readers)
         finally
            (push (nreverse readers) direct-slot)
            (push :readers direct-slot))
       (loop
         with writers = nil
         for symbol in (internal:writers field)
         for interned = (intern
                         (symbol-name (second symbol)) intern:*intern-package*)
         for setf-symbol = `(setf ,interned)
         do
            (export interned intern:*intern-package*)
            (push setf-symbol writers)
         finally
            (push (nreverse writers) direct-slot)
            (push :writers direct-slot))
       (push direct-slot direct-slots)
    finally
       (let ((initargs (list :direct-slots (nreverse direct-slots)
                             :name (nth-value 1 (api:name instance)))))
         (multiple-value-bind
               (deduced provided provided-p)
             (api:namespace instance)
           (declare (ignore deduced))
           (when provided-p
             (push provided initargs)
             (push :namespace initargs)))
         (let ((aliases (api:aliases instance)))
           (when aliases
             (push aliases initargs)
             (push :aliases initargs)))
         (apply #'reinitialize-instance instance initargs))))
