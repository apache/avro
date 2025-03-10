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
(defpackage #:org.apache.avro.internal.ipc.error
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:record #:org.apache.avro.internal.record)
   (#:intern #:org.apache.avro.internal.intern))
  (:import-from #:org.apache.avro.internal.recursive-descent.pattern
                #:define-pattern-method)
  (:import-from #:org.apache.avro.internal.record
                #:fields
                #:objects
                #:booleans))
(in-package #:org.apache.avro.internal.ipc.error)

(define-condition api:rpc-error (error)
  ((metadata
    :initarg :metadata
    :type api:map<bytes>
    :reader api:metadata
    :documentation "Metadata from server"))
  (:default-initargs
   :metadata (make-instance 'api:map<bytes> :size 0))
  (:documentation
   "Base condition for rpc errors."))

(define-condition api:undeclared-rpc-error (api:rpc-error)
  ((message
    :initarg :message
    :type api:string
    :reader api:message
    :documentation "Error message from server."))
  (:report
   (lambda (condition stream)
     (format stream (api:message condition))))
  (:default-initargs
   :message (error "Must supply MESSAGE"))
  (:documentation
   "Undeclared error from server."))

(define-condition api:declared-rpc-error (api:rpc-error)
  ((schema
    :type api:record
    :reader internal:schema
    :allocation :class))
  (:documentation
   "Declared error from server."))

(declaim
 (ftype (function (symbol fields objects list booleans)
                  (values api:declared-rpc-error &optional))
        %make-error))
(defun %make-error (type fields values initargs initargp-vector)
  (loop
    with error = (apply #'make-condition type initargs)

    for field across fields
    for value across values
    for initargp across initargp-vector
    for writers = (internal:writers field)

    when initargp do
      (let ((writer (fdefinition (first writers))))
        (funcall writer value error))

    finally
       (return
         error)))

(declaim
 (ftype (function (symbol fields objects api:map<bytes>)
                  (values api:declared-rpc-error &optional))
        make-error))
(defun make-error (type fields values metadata)
  (loop
    with initargs of-type list = (list :metadata metadata)
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
         (%make-error type fields values initargs initargp-vector))))

(declaim
 (ftype (function (symbol api:record-object &optional api:map<bytes>)
                  (values api:declared-rpc-error &optional))
        internal:make-declared-rpc-error))
(defun internal:make-declared-rpc-error
    (type error &optional (metadata (make-instance 'api:map<bytes> :size 0)))
  (loop
    with fields of-type fields = (api:fields (class-of error))
    with values = (make-array (length fields) :element-type 'api:object)

    for index below (length fields)
    for field = (elt fields index)
    for name of-type symbol = (nth-value 1 (api:name field))
    for value of-type api:object = (slot-value error name)
    do
       (setf (elt values index) value)

    finally
       (return
         (make-error type fields values metadata))))

(declaim
 (ftype (function (api:declared-rpc-error)
                  (values api:record-object &optional))
        internal:to-record))
(defun internal:to-record (error)
  (loop
    with schema of-type api:record = (internal:schema error)
    with fields of-type fields = (api:fields schema)
    with values = (make-array (length fields) :element-type 'api:object)

    for index below (length fields)
    for field = (elt fields index)
    for reader = (fdefinition (first (internal:readers field)))
    for value = (funcall reader error)
    do
       (setf (elt values index) value)

    finally
       (return
         (record:make-object schema fields values))))

(declaim
 (ftype (function (api:field) (values cons &optional))
        %%add-accessors-and-initargs))
(defun %%add-accessors-and-initargs (field)
  (let ((initargs (list
                   :name (nth-value 1 (api:name field))
                   :type (api:type field)
                   :allocation (closer-mop:slot-definition-allocation field)
                   :initargs (closer-mop:slot-definition-initargs field)
                   :documentation (documentation field t))))
    (let ((initfunction (closer-mop:slot-definition-initfunction field)))
      (when initfunction
        (push initfunction initargs)
        (push :initfunction initargs)
        (push (closer-mop:slot-definition-initform field) initargs)
        (push :initform initargs)))
    (multiple-value-bind (default defaultp)
        (api:default field)
      (when defaultp
        (push default initargs)
        (push :default initargs)))
    (multiple-value-bind (order orderp)
        (api:order field)
      (when orderp
        (push order initargs)
        (push :order initargs)))
    (let ((aliases (api:aliases field)))
      (when aliases
        (push aliases initargs)
        (push :aliases initargs)))
    (let ((readers (internal:readers field))
          (writers (internal:writers field))
          (symbol (gensym)))
      (unless readers
        (push symbol readers))
      (unless (or writers (closer-mop:slot-definition-initargs field))
        (push `(setf ,symbol) writers))
      (push readers initargs)
      (push :readers initargs)
      (push writers initargs)
      (push :writers initargs))
    initargs))

(declaim
 (ftype (function (fields) (values list &optional))
        %add-accessors-and-initargs))
(defun %add-accessors-and-initargs (fields)
  (map 'list #'%%add-accessors-and-initargs fields))

(declaim
 (ftype (function (api:record) (values api:record &optional))
        add-accessors-and-initargs))
(defun add-accessors-and-initargs (record)
  (let ((initargs
          (list
           :direct-default-initargs (closer-mop:class-direct-default-initargs
                                     record)
           :documentation (documentation record t)
           :direct-superclasses (closer-mop:class-direct-superclasses
                                 record))))
    (multiple-value-bind
          (deduced-namespace provided-namespace namespace-provided-p)
        (api:namespace record)
      (when namespace-provided-p
        (push provided-namespace initargs)
        (push :namespace initargs))
      (when (and deduced-namespace
                 (or (null provided-namespace)
                     (string/= deduced-namespace provided-namespace)))
        (push deduced-namespace initargs)
        (push :enclosing-namespace initargs)))
    (let ((aliases (api:aliases record)))
      (when aliases
        (push aliases initargs)
        (push :aliases initargs)))
    (push (%add-accessors-and-initargs (api:fields record)) initargs)
    (push :direct-slots initargs)
    (apply #'reinitialize-instance record initargs)))

(declaim
 (ftype (function (api:field) (values cons &optional)) make-condition-slot))
(defun make-condition-slot (field)
  (let ((name (nth-value 1 (api:name field)))
        (type (api:type field))
        (documentation (documentation field t))
        (initargs (closer-mop:slot-definition-initargs field))
        (readers (internal:readers field))
        (writers (internal:writers field)))
    `(,name
      :type ,type
      ,@(when documentation
          `(:documentation ,documentation))
      ,@(mapcan (lambda (initarg)
                  `(:initarg ,initarg))
                initargs)
      ,@(mapcan (lambda (reader)
                  `(:reader ,reader))
                readers)
      ,@(mapcan (lambda (writer)
                  `(:writer ,writer))
                writers))))

(declaim
 (ftype (function (api:record) (values list &optional)) make-condition-slots))
(defun make-condition-slots (record)
  (map 'list #'make-condition-slot (api:fields record)))

(declaim
 (ftype (function (symbol list list api:record) (values cons &optional))
        expand-define-condition))
(defun expand-define-condition (name slots options record)
  `(progn
     (define-condition ,name (api:declared-rpc-error)
       ((schema :allocation :class :initform ,record)
        ,@slots)
       ,@options)

     (find-class ',name)))

(declaim
 (ftype (function (api:record &optional symbol) (values class &optional))
        to-error))
(defun to-error (record &optional (name (make-symbol (api:name record))))
  (let* ((record (add-accessors-and-initargs record))
         (condition-slots (make-condition-slots record))
         (condition-options (when (documentation record t)
                              `((:documentation ,(documentation record t))))))
    (eval
     (expand-define-condition name condition-slots condition-options record))))

(defmethod internal:write-jso
    ((error class) seen canonical-form-p)
  (assert (subtypep error 'api:declared-rpc-error) ())
  (closer-mop:ensure-finalized error)
  (let* ((record (internal:schema (closer-mop:class-prototype error)))
         (jso (internal:write-jso record seen canonical-form-p)))
    (when (typep jso 'st-json:jso)
      (setf (st-json:getjso "type" jso) "error"))
    jso))

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" "error")) fullname->schema enclosing-namespace)
      (setf (st-json:getjso "type" jso) "record")
      (let* ((record
               (internal:read-jso jso fullname->schema enclosing-namespace))
             (condition (to-error record)))
        (setf (gethash (api:fullname record) fullname->schema) condition)
        condition)))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim (ftype (function (list) (values &optional)) assert-valid-options))
  (defun assert-valid-options (options)
    (loop
      ;; TODO should I add :name that overrides like named-schemas?
      with expected-keys = '(:namespace :enclosing-namespace :aliases
                             :default-initargs :documentation :report)
        initially
           (assert (every #'consp options) (options)
                   "Expected a list of cons cells: ~S" options)

      for (key &rest) in options
      unless (member key expected-keys) do
        (error "Unknown key ~S, expected one of ~S" key expected-keys)
      collect key into keys
      finally
         (loop
           for expected-key in expected-keys
           do (setf keys (delete expected-key keys :count 1))
           finally
              (when keys
                (error "Duplicate keys: ~S" keys))))
    (values))

  (declaim
   (ftype (function (list) (values list &optional)) options->record-initargs))
  (defun options->record-initargs (options)
    (loop
      with keys = '(:namespace :enclosing-namespace :aliases :documentation)

      for key in keys
      for assoc = (assoc key options)
      when assoc
        nconc (list key (cdr assoc))))

  (declaim
   (ftype (function (list) (values list &optional))
          options->condition-options))
  (defun options->condition-options (options)
    (loop
      with keys = '(:default-initargs :documentation :report)

      for key in keys
      when (assoc key options)
        collect it))

  (declaim
   (ftype (function (list symbol symbol (function (t) (values t &optional)))
                    (values list &optional))
          %gather-into))
  (defun %gather-into (field from to transform)
    (loop
      while (member from field)
      collect (funcall transform (getf field from)) into gathered
      do (remf field from)

      finally
         (when gathered
           (setf (getf field to) gathered))
         (return field)))

  (declaim
   (ftype (function
           (list symbol symbol &optional (function (t) (values t &optional)))
           (values list &optional))
          gather-into))
  (defun gather-into (fields from to &optional (transform #'identity))
    (flet ((%gather-into (field)
             (%gather-into field from to transform)))
      (mapcar #'%gather-into fields)))

  (declaim (ftype (function (list) (values list &optional)) to-direct-slots))
  (defun to-direct-slots (fields)
    (flet ((add-name (field)
             (push :name field))
           (expand-accessors (field)
             (loop
               while (member :accessor field) do
                 (push (getf field :accessor) field)
                 (push :reader field)
                 (push (getf field :accessor) field)
                 (push :writer field)
                 (remf field :accessor)

               finally
                  (return field)))
           (writer-transform (writer)
             (declare ((or cons symbol) writer))
             (if (consp writer)
                 writer
                 `(setf ,writer))))
      (setf fields (mapcar #'add-name fields)
            fields (mapcar #'expand-accessors fields)
            fields (gather-into fields :reader :readers)
            fields (gather-into fields :writer :writers #'writer-transform)
            fields (gather-into fields :initarg :initargs)))))

(defmacro api:define-error (name (&rest fields) &body options)
  "Define NAME as a DECLARED-RPC-ERROR with condition FIELDS and OPTIONS."
  (declare (symbol name))
  (assert-valid-options options)
  (let ((record-initargs (list*
                          ;; if this is the same symbol as name, then
                          ;; I can't use the ctor twice
                          :name (make-symbol (string name))
                          :direct-slots (to-direct-slots fields)
                          (options->record-initargs options)))
        (condition-options (options->condition-options options))
        (record (gensym))
        (condition-slots (gensym)))
    `(eval-when (:compile-toplevel :load-toplevel :execute)
       (let* ((,record
                (add-accessors-and-initargs
                 (let ((class (find-class ',name nil)))
                   (if class
                       (let ((record (internal:schema
                                      (closer-mop:class-prototype class))))
                         (apply
                          #'reinitialize-instance record ',record-initargs))
                       (apply
                        #'make-instance 'api:record ',record-initargs)))))
              (,condition-slots
                (make-condition-slots ,record)))
         (eval
          (expand-define-condition
           ',name ,condition-slots ',condition-options ,record))))))

;;; intern

(defmethod api:intern
    ((instance class) &key (null-namespace api:*null-namespace*))
  (assert (subtypep instance 'api:declared-rpc-error) (instance)
          "Not an error class")
  (let* ((schema (internal:schema (closer-mop:class-prototype instance)))
         (class-name (api:intern schema :null-namespace null-namespace)))
    (assert (eq instance (to-error schema (class-name instance))))
    ;; Muffle bogus warning about changing meta class
    (handler-bind ((warning #'muffle-warning))
      (setf (find-class class-name) instance))
    class-name))
