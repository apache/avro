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
(defpackage #:org.apache.avro/test/resolution/map
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro)
   (#:base #:org.apache.avro/test/resolution/base)))
(in-package #:org.apache.avro/test/resolution/map)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim
   (ftype (function (symbol) (values cons &optional)) find-schema))
  (defun find-schema (name)
    (handler-case
        `',(base:find-schema name)
      (error ()
        `(find-class ',name)))))

(declaim
 (ftype (function ((or symbol avro:map) &rest avro:object)
                  (values avro:map-object &optional))
        make-map))
(defun make-map (schema &rest key-value-pairs)
  (loop
    with map = (make-instance schema)
      initially
         (unless (evenp (length key-value-pairs))
           (error "Odd number of key-value pairs: ~S" key-value-pairs))

    for (key value) on key-value-pairs by #'cddr
    do (setf (avro:gethash key map) value)

    finally
       (return map)))

(declaim
 (ftype (function (avro:map-object
                   avro:map-object
                   (function (avro:object avro:object)
                             (values boolean &optional)))
                  (values (eql t) &optional))
        assert-map=))
(defun assert-map= (left right compare)
  (is (= (avro:hash-table-count left)
         (avro:hash-table-count right)))
  (flet ((compare (key left-value)
           (multiple-value-bind (right-value existsp)
               (avro:gethash key right)
             (is existsp)
             (is (funcall compare left-value right-value)))))
    (avro:maphash #'compare left))
  t)

(defmacro deftest (from to input compare)
  (declare (symbol from to)
           (list input)
           ((or symbol cons) compare))
  (let ((test-name (intern (format nil "~A->~A" from to)))
        (from (find-schema from))
        (to (find-schema to))
        (writer-schema (gensym))
        (reader-schema (gensym))
        (writer-map (gensym))
        (reader-map (gensym)))
    `(test ,test-name
       (let* ((,writer-schema (make-instance 'avro:map :values ,from))
              (,reader-schema (make-instance 'avro:map :values ,to))
              (,writer-map (make-map ,writer-schema ,@input))
              (,reader-map (avro:coerce
                            (avro:deserialize
                             ,writer-schema (avro:serialize ,writer-map))
                            ,reader-schema)))
         (is (typep ,writer-map ,writer-schema))
         (is (typep ,reader-map ,reader-schema))
         (assert-map= ,writer-map ,reader-map ,compare)))))

(deftest string string ("foo-key" "foo-value" "bar-key" "bar-value") #'string=)

(deftest int long ("two" 2 "four" 4 "six" 6) #'=)

(deftest int float ("two" 2 "four" 4 "six" 6) #'=)

(deftest float double ("two.zero" 2.0 "four.five" 4.5 "six.three" 6.3) #'=)

(defclass map<int> ()
  ()
  (:metaclass avro:map)
  (:values avro:int))

(defclass map<long> ()
  ()
  (:metaclass avro:map)
  (:values avro:long))

(deftest map<int> map<long>
  ("246" (make-map 'map<int> "two" 2 "four" 4 "six" 6)
   "81012"(make-map 'map<int> "eight" 8 "ten" 10 "twelve" 12))
  (lambda (left right)
    (assert-map= left right #'=)))
