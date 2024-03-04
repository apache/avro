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
(defpackage #:org.apache.avro/test/resolution/array
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro)
   (#:base #:org.apache.avro/test/resolution/base)))
(in-package #:org.apache.avro/test/resolution/array)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim
   (ftype (function (symbol) (values cons &optional)) find-schema))
  (defun find-schema (name)
    (handler-case
        `',(base:find-schema name)
      (error ()
        `(find-class ',name)))))

(defmacro deftest (from to input compare)
  (declare (symbol from to)
           (list input)
           ((or symbol cons) compare))
  (let ((test-name (intern (format nil "~A->~A" from to)))
        (from (find-schema from))
        (to (find-schema to))
        (writer-schema (gensym))
        (reader-schema (gensym))
        (writer-array (gensym))
        (reader-array (gensym)))
    `(test ,test-name
       (let* ((,writer-schema (make-instance 'avro:array :items ,from))
              (,reader-schema (make-instance 'avro:array :items ,to))
              (,writer-array (make-instance
                              ,writer-schema :initial-contents ,input))
              (,reader-array (avro:coerce
                              (avro:deserialize
                               ,writer-schema (avro:serialize ,writer-array))
                              ,reader-schema)))
         (is (typep ,writer-array ,writer-schema))
         (is (typep ,reader-array ,reader-schema))
         (is (= (length ,writer-array) (length ,reader-array)))
         (is (every ,compare ,writer-array ,reader-array))))))

(deftest string string '("foo" "bar") #'string=)

(deftest int long '(2 4 6) #'=)

(deftest int float '(2 4 6) #'=)

(deftest float double '(2.0 4.5 6.3) #'=)

(defclass array<int> ()
  ()
  (:metaclass avro:array)
  (:items avro:int))

(defclass array<long> ()
  ()
  (:metaclass avro:array)
  (:items avro:long))

(deftest array<int> array<long>
  (list (make-instance 'array<int> :initial-contents '(2 4 6))
        (make-instance 'array<int> :initial-contents '(8 10 12)))
  (lambda (left right)
    (every #'= left right)))
