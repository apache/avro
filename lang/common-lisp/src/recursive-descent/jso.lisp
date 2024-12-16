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
(defpackage #:org.apache.avro.internal.recursive-descent.jso
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal))
  (:import-from #:org.apache.avro.internal.recursive-descent.pattern
                #:get-value
                #:pattern-generic-function)
  (:import-from #:org.apache.avro.internal.type
                #:ufixnum))
(in-package #:org.apache.avro.internal.recursive-descent.jso)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim
   (ftype (function (symbol) (values simple-string &optional))
          internal:downcase-symbol))
  (defun internal:downcase-symbol (symbol)
    (string-downcase (symbol-name symbol))))

(defmethod st-json:write-json-element
    ((vector vector) (stream stream))
  (if (zerop (length vector))
      (write-string "[]" stream)
      (loop
        initially
           (write-char #\[ stream)
           (st-json:write-json-element (elt vector 0) stream)

        for index from 1 below (length vector)
        for element = (elt vector index)
        do
           (write-char #\, stream)
           (st-json:write-json-element element stream)

        finally
           (write-char #\] stream))))

;;; deserialize

(defgeneric internal:read-jso (jso fullname->schema enclosing-namespace)
  (:generic-function-class pattern-generic-function))

(defmethod get-value
    ((key string) (value st-json:jso))
  (st-json:getjso key value))

;; TODO return number of characters consumed
(defmethod api:deserialize
    ((schema (eql 'api:schema)) (input stream) &key)
  (declare (ignore schema))
  (let* ((jso (st-json:read-json input t))
         (fullname->schema (make-hash-table :test #'equal))
         (schema (internal:read-jso jso fullname->schema nil)))
    (closer-mop:ensure-finalized schema nil)
    schema))

(defmethod api:deserialize
    ((schema (eql 'api:schema)) (input string) &key (start 0))
  (declare (ignore schema))
  (multiple-value-bind (jso index)
      (st-json:read-json-from-string input :start start :junk-allowed-p t)
    (let* ((fullname->schema (make-hash-table :test #'equal))
           (schema (internal:read-jso jso fullname->schema nil)))
      (closer-mop:ensure-finalized schema nil)
      (values schema (- index start)))))

(defmethod api:deserialize
    ((schema symbol) input &rest initargs)
  "Deserialize with class named SCHEMA."
  (apply #'api:deserialize (find-class schema) input initargs))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim (ftype (function (symbol) (values symbol &optional)) +p))
  (defun +p (name)
    (nth-value 0 (intern (format nil "~SP" name)))))

;;; with-initargs

(defmacro internal:with-initargs ((&rest bindings) jso &body body)
  "Binds an INITARGS symbol for use in BODY.

Each binding should either be a symbol or (field initarg) list."
  (flet ((parse-binding (binding)
           (if (symbolp binding)
               (values (internal:downcase-symbol binding)
                       (intern (string binding) 'keyword))
               (destructuring-bind (field initarg) binding
                 (declare (symbol field)
                          (keyword initarg))
                 (values (internal:downcase-symbol field) initarg)))))
    (let ((%jso (gensym))
          (initargs (intern "INITARGS")))
      `(let ((,%jso ,jso)
             (,initargs nil))
         ,@(mapcar (lambda (binding)
                     (multiple-value-bind (field initarg)
                         (parse-binding binding)
                       (let ((value (gensym))
                             (valuep (gensym)))
                         `(multiple-value-bind (,value ,valuep)
                              (st-json:getjso ,field ,%jso)
                            (when ,valuep
                              (push ,value ,initargs)
                              (push ,initarg ,initargs))))))
                   bindings)
         ,@body))))
