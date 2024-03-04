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
(defpackage #:org.apache.avro.internal.name.class
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:mop #:org.apache.avro.internal.mop)
   (#:type #:org.apache.avro.internal.name.type)
   (#:deduce #:org.apache.avro.internal.name.deduce)
   (#:intern #:org.apache.avro.internal.intern))
  (:export #:named-class
           #:provided-name
           #:deduced-name
           #:provided-namespace
           #:deduced-namespace
           #:fullname))
(in-package #:org.apache.avro.internal.name.class)

(defclass named-class (standard-class)
  ((provided-name
    :reader provided-name
    :type type:fullname
    :documentation "Provided class name.")
   (provided-namespace
    :initarg :namespace
    :type type:namespace
    :documentation "Provided class namespace.")
   (deduced-name
    :reader deduced-name
    :type type:name
    :documentation "Namespace unqualified name of class.")
   (deduced-namespace
    :reader deduced-namespace
    :type type:namespace
    :documentation "Namespace of class.")
   (fullname
    :reader api:fullname
    :type type:fullname
    :documentation "Namespace qualified name of class."))
  (:metaclass mop:scalar-class)
  (:scalars :name :namespace)
  (:documentation
   "Base class for named metaclasses."))

(defmethod closer-mop:validate-superclass
    ((class named-class) (superclass standard-class))
  t)

;;; name

(deftype api:name-return-type ()
  "(values deduced provided)"
  '(values type:name type:fullname &optional))

(defmethod api:name
    ((instance named-class))
  "Returns (values deduced provided).

The returned values conform to NAME-RETURN-TYPE."
  (let ((deduced-name (deduced-name instance))
        (provided-name (provided-name instance)))
    (declare (type:name deduced-name)
             (type:fullname provided-name))
    (values deduced-name provided-name)))

;;; namespace

(declaim
 (ftype (function (named-class) (values boolean &optional))
        namespace-provided-p))
(defun namespace-provided-p (named-class)
  "True if namespace was provided."
  (slot-boundp named-class 'provided-namespace))

(declaim
 (ftype (function (named-class) (values type:namespace &optional))
        provided-namespace))
(defun provided-namespace (named-class)
  "Returns the provided-namespace."
  (when (namespace-provided-p named-class)
    (slot-value named-class 'provided-namespace)))

(deftype api:namespace-return-type ()
  "(values deduced provided provided-p)"
  '(values type:namespace type:namespace boolean))

(defmethod api:namespace
    ((instance named-class))
  "Returns (values deduced provided provided-p).

The returned values conform to NAMESPACE-RETURN-TYPE."
  (let ((deduced-namespace (deduced-namespace instance))
        (provided-namespace (provided-namespace instance))
        (namespace-provided-p (namespace-provided-p instance)))
    (declare (type:namespace deduced-namespace provided-namespace)
             (boolean namespace-provided-p))
    (values deduced-namespace provided-namespace namespace-provided-p)))

;;; initialization

(declaim (ftype (function (list) (values type:fullname &optional)) parse-name))
(defun parse-name (initargs)
  (let* ((first (member :name initargs))
         (second (member :name (cddr first))))
    (assert first () "Must supply NAME")
    (if second
        (second second)
        (string (second first)))))

(defmethod initialize-instance :after
    ((instance named-class) &rest initargs)
  (let ((provided-namespace (provided-namespace instance)))
    (with-slots
          (provided-name deduced-name deduced-namespace fullname) instance
      (setf provided-name (parse-name initargs)
            deduced-name (deduce:fullname->name provided-name)
            deduced-namespace (deduce:deduce-namespace
                               provided-name provided-namespace nil)
            fullname (deduce:deduce-fullname
                      provided-name provided-namespace nil)))))

(defmethod reinitialize-instance :around
    ((instance named-class) &rest initargs)
  (when (typep (getf initargs :name) '(or null (not symbol)))
    (push (class-name instance) initargs)
    (push :name initargs))
  (apply #'call-next-method instance initargs))

;;; intern

(defmethod api:intern :around
    ((instance named-class) &key (null-namespace api:*null-namespace*))
  (let ((name (api:name instance))
        (namespace (api:namespace instance)))
    (when (deduce:null-namespace-p namespace)
      (setf namespace null-namespace))
    (let* ((intern:*intern-package* (or (find-package namespace)
                                        (make-package namespace)))
           (class-name (intern name intern:*intern-package*)))
      (export class-name intern:*intern-package*)
      (setf (find-class class-name) instance)
      (call-next-method instance :null-namespace null-namespace)
      class-name)))
