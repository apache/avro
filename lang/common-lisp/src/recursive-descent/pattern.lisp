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
(defpackage #:org.apache.avro.internal.recursive-descent.pattern
  (:use #:cl)
  (:local-nicknames
   (#:mop #:org.apache.avro.internal.mop))
  (:import-from #:org.apache.avro.internal.type
                #:ufixnum
                #:comparison)
  (:export #:get-value
           #:intern-pattern-specializer
           #:pattern-generic-function
           #:define-pattern-method))
(in-package #:org.apache.avro.internal.recursive-descent.pattern)

;;; pattern-specializer

(defclass pattern-specializer (closer-mop:specializer)
  ((pattern
    :initarg :pattern
    :accessor pattern)
   (methods
    :type list
    :accessor methods
    :initform nil))
  (:default-initargs
   :pattern (error "Must supply PATTERN")))

(defmethod initialize-instance :after
    ((instance pattern-specializer) &key)
  (when (consp (pattern instance))
    (assert (evenp (length (pattern instance))) ((pattern instance))
            "Pattern has an odd number of key-value pairs: ~S"
            (pattern instance))))

(defmethod closer-mop:specializer-direct-methods
    ((specializer pattern-specializer))
  (methods specializer))

(defmethod closer-mop:specializer-direct-generic-functions
    ((specializer pattern-specializer))
  (let ((gfs
          (mapcar #'closer-mop:method-generic-function (methods specializer))))
    (delete-duplicates (delete nil gfs))))

(defmethod closer-mop:add-direct-method
    ((specializer pattern-specializer) method)
  (pushnew method (methods specializer)))

(defmethod closer-mop:remove-direct-method
    ((specializer pattern-specializer) method)
  (setf (methods specializer) (delete method (methods specializer))))

;; surjective-bias with injective-bias provided by superclass methods

(defgeneric get-value (key object)
  (:method (key object)
    (values nil nil))

  (:documentation
   "Return (values value valuep)."))

(defgeneric match-score (pattern object)
  (:method (pattern object)
    (when (equal pattern object)
      1))

  (:method ((pattern null) object)
    0)

  (:method ((pattern cons) object)
    (loop
      with match-score = nil

      for (key nested-pattern) on pattern by #'cddr
      for (value valuep) = (multiple-value-list (get-value key object))
      unless valuep
        return nil

      if (setf match-score (match-score nested-pattern value))
        sum (1+ match-score)
      else
        return nil)))

;;; intern-pattern-specializer

(declaim ((vector pattern-specializer) *pattern-specializers*))
(defparameter *pattern-specializers*
  (make-array 0 :element-type 'pattern-specializer :adjustable t
                :fill-pointer t))

(declaim
 (ftype (function (t) (values (or null pattern-specializer) &optional))
        find-pattern-specializer))
(defun find-pattern-specializer (pattern)
  ;; TODO equal won't work for structures or vectors
  (find pattern *pattern-specializers* :test #'equal :key #'pattern))

(declaim
 (ftype (function (t) (values pattern-specializer &optional))
        intern-pattern-specializer))
(defun intern-pattern-specializer (pattern)
  (or (find-pattern-specializer pattern)
      (let ((new (make-instance 'pattern-specializer :pattern pattern)))
        (prog1 new
          (vector-push-extend new *pattern-specializers*)))))

;;; pattern-generic-function

(deftype ordering ()
  '(simple-array ufixnum (*)))

(defclass pattern-generic-function (closer-mop:standard-generic-function)
  ((argument-order
    :type ordering
    :accessor argument-order))
  (:metaclass closer-mop:funcallable-standard-class))

(declaim
 (ftype (function (pattern-generic-function) (values &optional))
        set-argument-order))
(defun set-argument-order (gf)
  (let ((lambda-list (closer-mop:generic-function-lambda-list gf))
        (ordered (closer-mop:generic-function-argument-precedence-order gf)))
    (setf (argument-order gf)
          (make-array (length ordered) :element-type 'ufixnum))
    (loop
      for index below (length ordered)
      for argument = (elt ordered index)
      for position = (position argument lambda-list :test #'eq)
      do (setf (elt (argument-order gf) index) position)))
  (values))

(mop:definit
    ((instance pattern-generic-function) :after
     &key (lambda-list nil lambda-list-p))
  (declare (ignore lambda-list))
  (when lambda-list-p
    (set-argument-order instance)))

(defmethod add-method :after
    ((instance pattern-generic-function) method)
  (set-argument-order instance))

(defmethod closer-mop:compute-applicable-methods-using-classes
    ((gf pattern-generic-function) classes)
  (values nil nil))

(declaim
 (ftype (function (closer-mop:specializer closer-mop:specializer t)
                  (values comparison &optional))
        compare-specializers))
(defun compare-specializers (specializer1 specializer2 argument)
  (cond
    ((and (typep specializer1 'closer-mop:eql-specializer)
          (typep specializer2 'closer-mop:eql-specializer))
     0)
    ((typep specializer1 'closer-mop:eql-specializer)
     -1)
    ((typep specializer2 'closer-mop:eql-specializer)
     1)
    ((and (typep specializer1 'pattern-specializer)
          (typep specializer2 'pattern-specializer))
     (let ((score1 (match-score (pattern specializer1) argument))
           (score2 (match-score (pattern specializer2) argument)))
       (cond
         ((= score1 score2) 0)
         ((< score1 score2) 1)
         (t -1))))
    ((typep specializer1 'pattern-specializer)
     -1)
    ((typep specializer2 'pattern-specializer)
     1)
    ((eq specializer1 specializer2)
     0)
    ((subtypep specializer1 specializer2)
     -1)
    ((subtypep specializer2 specializer1)
     1)
    (t
     (error "oh no"))))

(declaim
 (ftype (function (method method list ordering) (values boolean &optional))
        method<))
(defun method< (method1 method2 arguments argument-order)
  (loop
    with specializers1 = (closer-mop:method-specializers method1)
    and specializers2 = (closer-mop:method-specializers method2)

    for index across argument-order
    for specializer1 = (elt specializers1 index)
    for specializer2 = (elt specializers2 index)
    for argument = (elt arguments index)

    for comparison = (compare-specializers specializer1 specializer2 argument)
    if (= -1 comparison)
      return t
    else if (= 1 comparison)
           return nil

    finally
       (return
         (error "Both methods apply equally"))))

(declaim
 (ftype (function (closer-mop:specializer t) (values boolean &optional))
        applicable-specializer-p))
(defun applicable-specializer-p (specializer argument)
  (etypecase specializer
    (closer-mop:eql-specializer
     (eql (closer-mop:eql-specializer-object specializer) argument))
    (pattern-specializer
     (not (null (match-score (pattern specializer) argument))))
    (class
     (typep argument specializer))))

(declaim
 (ftype (function (method list) (values boolean &optional))
        applicable-method-p))
(defun applicable-method-p (method arguments)
  (let ((specializers (closer-mop:method-specializers method)))
    (every #'applicable-specializer-p specializers arguments)))

(declaim
 (ftype (function (generic-function list) (values list &optional))
        applicable-methods))
(defun applicable-methods (gf arguments)
  (loop
    for method in (closer-mop:generic-function-methods gf)
    when (applicable-method-p method arguments)
      collect method))

(defmethod compute-applicable-methods
    ((gf pattern-generic-function) arguments)
  (let ((methods (applicable-methods gf arguments))
        (argument-order (argument-order gf)))
    (flet ((method< (method1 method2)
             (method< method1 method2 arguments argument-order)))
      (sort methods #'method<))))

;;; define-pattern-method

(deftype string? ()
  '(or string null))

(declaim (ftype (function (list) (values string? list &optional)) parse-body))
(defun parse-body (body)
  (cond
    ((and (stringp (car body))
          (cdr body))
     (values (car body) (cdr body)))
    ((and (consp (car body))
          (eq (caar body) 'declare)
          (stringp (cadr body))
          (cddr body))
     (values (cadr body) (cons (car body) (cddr body))))
    (t
     (values nil body))))

(declaim (ftype (function (list) (values list &optional)) parse-specializers))
(defun parse-specializers (specialized-lambda-list)
  (loop
    for name in (closer-mop:extract-specializer-names specialized-lambda-list)
    if (symbolp name)
      collect (find-class name)
    else if (and (listp name)
                 (eq (first name) 'eql))
           collect (closer-mop:intern-eql-specializer (rest name))
    else if (and (listp name)
                 (symbolp (first name))
                 (string= (nstring-downcase (string (first name))) "pattern"))
           collect (intern-pattern-specializer (rest name))
    else
      collect (intern-pattern-specializer name)))

(declaim
 (ftype (function (generic-function list list list string? cons)
                  (values method &optional))
        make-and-add-method))
(defun make-and-add-method
    (gf qualifiers lambda-list specializers documentation lambda-form)
  (let* ((method-class (closer-mop:generic-function-method-class gf))
         (method-prototype (closer-mop:class-prototype method-class))
         (method-lambda (closer-mop:make-method-lambda
                         gf method-prototype lambda-form nil))
         (method (make-instance
                  method-class
                  :qualifiers qualifiers
                  :lambda-list lambda-list
                  :documentation documentation
                  :specializers specializers
                  :function (compile nil method-lambda))))
    (prog1 method
      (add-method gf method))))

(declaim
 (ftype (function (symbol &rest (or symbol cons)) (values method &optional))
        define-pattern-method))
(defun define-pattern-method (name &rest args)
  (let* ((qualifiers (loop
                       for arg in args
                       until (listp arg)
                       collect (pop args)))
         (specialized-lambda-form (pop args))
         (specialized-lambda-list (second specialized-lambda-form))
         (lambda-list (closer-mop:extract-lambda-list specialized-lambda-list))
         (specializers (parse-specializers specialized-lambda-list)))
    (multiple-value-bind (documentation body)
        (parse-body (cddr specialized-lambda-form))
      (make-and-add-method (symbol-function name)
                           qualifiers
                           lambda-list
                           specializers
                           documentation
                           `(lambda ,lambda-list ,@body)))))
