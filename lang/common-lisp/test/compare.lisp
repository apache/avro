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
(defpackage #:org.apache.avro/test/compare
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:use #:cl #:1am))
(in-package #:org.apache.avro/test/compare)

(defmacro with-compare ((schema &body preprocess) &body body)
  (let ((preprocess-gensym (gensym))
        (schema-gensym (gensym)))
    `(let ((,schema-gensym ,schema))
       (labels
           ((,preprocess-gensym (schema object)
              (declare (ignorable schema object))
              ,@(or preprocess '(object)))
            (compare (left right)
              (avro:compare ,schema-gensym
                            (avro:serialize
                             (,preprocess-gensym ,schema-gensym left))
                            (avro:serialize
                             (,preprocess-gensym ,schema-gensym right)))))
         ,@body))))

(test null-compare
  (with-compare ('avro:null)
    (is (= 0 (compare nil nil)))))

(test boolean-compare
  (with-compare ('avro:boolean)
    (is (= 0 (compare 'avro:true 'avro:true)))
    (is (= -1 (compare 'avro:false 'avro:true)))
    (is (= 1 (compare 'avro:true 'avro:false)))
    (is (= 0 (compare 'avro:false 'avro:false)))))

(test int-compare
  (with-compare ('avro:int)
    (is (= 0 (compare 2 2)))
    (is (= -1 (compare -1 0)))
    (is (= 1 (compare 2 1)))))

(test long-compare
  (with-compare ('avro:long)
    (is (= 0 (compare 20 20)))
    (is (= -1 (compare 2 4)))
    (is (= 1 (compare 4 2)))))

;; TODO for some reason, this is failing against the debian:stable
;; dockerized run
#+nil
(test float-compare
  (with-compare ('avro:float)
    (is (= 0 (compare 2.3f0 2.3f0)))
    (is (= -1 (compare 2.1f0 2.2f0)))
    (is (= 1 (compare 2.2f0 2.1f0)))))

(test double-compare
  (with-compare ('avro:double)
    (is (= 0 (compare 2.3d0 2.3d0)))
    (is (= -1 (compare 2.1d0 2.2d0)))
    (is (= 1 (compare 2.2d0 2.1d0)))))

(test bytes-compare
  (with-compare
      ('avro:bytes
        (make-array (length object) :element-type '(unsigned-byte 8)
                                    :initial-contents object))
    (is (= 0 (compare '(2 4 6) '(2 4 6))))
    (is (= -1 (compare '(2 4) '(2 4 6))))
    (is (= -1 (compare '(2 4 5) '(2 4 6))))
    (is (= 1 (compare '(2 4 6) '(2 4 5))))
    (is (= 1 (compare '(2 4 6) '(2 4))))))

(test string-compare
  (with-compare ('avro:string)
    (is (= 0 (compare "abc" "abc")))
    (is (= -1 (compare "abc" "abd")))
    (is (= -1 (compare "ab" "abc")))
    (is (= 1 (compare "ਸਾਹਿਲ ਕੰਗ" "abd")))
    (is (= 1 (compare "abc" "ab")))))

(test fixed-compare
  (with-compare
      ((make-instance 'avro:fixed :name "foo" :size 3)
        (make-instance schema :initial-contents object))
    (is (= 0 (compare '(2 4 6) '(2 4 6))))
    (is (= -1 (compare '(2 4 5) '(2 4 6))))
    (is (= 1 (compare '(2 4 6) '(2 4 5))))))

(test array-compare
  (with-compare
      ((make-instance
        'avro:array
        :items (make-instance
                'avro:enum
                :name "foo"
                :symbols '("ABC" "AB")))
        (flet ((string->enum (string)
                 (make-instance (avro:items schema) :enum string)))
          (make-instance
           schema
           :initial-contents (mapcar #'string->enum object))))
    (is (= 0 (compare '("ABC" "ABC") '("ABC" "ABC"))))
    (is (= -1 (compare '("ABC" "ABC") '("ABC" "AB"))))
    (is (= -1 (compare '("ABC") '("ABC" "ABC"))))
    (is (= 1 (compare '("ABC" "AB") '("ABC" "ABC"))))
    (is (= 1 (compare '("AB" "AB") '("AB"))))))

(test enum-compare
  (with-compare
      ((make-instance
        'avro:enum
        :name "foo"
        :symbols '("ABC" "AB"))
        (make-instance schema :enum object))
    (is (= 0 (compare "AB" "AB")))
    (is (= -1 (compare "ABC" "AB")))
    (is (= 1 (compare "AB" "ABC")))))

(test union-compare
  (with-compare
      ((make-instance
        'avro:union
        :schemas `(avro:int ,(closer-mop:ensure-class
                              'enum_name
                              :metaclass 'avro:enum
                              :symbols '("ABC" "AB"))))
        (make-instance
         schema
         :object (if (stringp object)
                     (make-instance 'enum_name :enum object)
                     object)))
    (is (= 0 (compare 2 2)))
    (is (= 0 (compare "AB" "AB")))
    (is (= -1 (compare 2 3)))
    (is (= -1 (compare "ABC" "AB")))
    (is (= -1 (compare 2 "ABC")))
    (is (= 1 (compare 3 2)))
    (is (= 1 (compare "AB" "ABC")))
    (is (= 1 (compare "ABC" 2)))))

(test record-compare
  (with-compare
      ((make-instance
        'avro:record
        :name "foo"
        :direct-slots
        `((:name #:|field_1| :type avro:int :initargs (:field-1))
          (:name #:|field_2|
                 :order avro:descending
                 :initargs (:field-2)
                 :type ,(closer-mop:ensure-class
                         'enum_name
                         :metaclass 'avro:enum
                         :symbols '("ABC" "AB")))
          (:name #:|field_3|
                 :order avro:ignore
                 :initargs (:field-3)
                 :type ,(closer-mop:ensure-class
                         'map<string>
                         :metaclass 'avro:map
                         :values 'avro:string))))
        (make-instance
         schema
         :field-1 (first object)
         :field-2 (make-instance 'enum_name :enum (second object))
         :field-3 (loop
                    with map = (make-instance 'map<string>)
                    for (key value) on (third object) by #'cddr
                    do (setf (avro:gethash key map) value)
                    finally (return map))))
    (is (= 0 (compare '(2 "ABC" ("foo" "bar")) '(2 "ABC" nil))))
    (is (= -1 (compare '(1 "ABC" ("foo" "bar")) '(2 "ABC" ("foo" "bar")))))
    (is (= -1 (compare '(2 "AB" ("foo" "bar")) '(2 "ABC" ("foo" "bar")))))
    (is (= 1 (compare '(2 "ABC" ("foo" "bar")) '(1 "ABC" ("foo" "bar")))))
    (is (= 1 (compare '(2 "ABC" ("foo" "bar")) '(2 "AB" ("foo" "bar")))))))

(test map-compare
  (with-compare
      ((make-instance 'avro:map :values 'avro:string)
        (make-instance schema))
    (signals error
      (compare nil nil))))
