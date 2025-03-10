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
(defpackage #:org.apache.avro/test/union
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:define-schema-test
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/union)

(named-readtables:in-readtable json-syntax)

(define-schema-test python-test
  ["string", "null", "long"]
  ["string", "null", "long"]
  #x6675680d41bea565
  (make-instance
   'avro:union
   :schemas '(avro:string avro:null avro:long))
  (defclass union<string-null-long> ()
    ()
    (:metaclass avro:union)
    (:schemas avro:string avro:null avro:long)))

(define-schema-test fixed-union
  [
    "string",
    {
      "type": "fixed",
      "name": "baz",
      "namespace": "foo.bar",
      "size": 12
    }
  ]
  [
    "string",
    {
      "name": "foo.bar.baz",
      "type": "fixed",
      "size": 12
    }
  ]
  #x370f6358da669947
  (make-instance
   'avro:union
   :schemas (list
             'avro:string
             (make-instance
              'avro:fixed
              :name '|baz|
              :namespace "foo.bar"
              :size 12)))
  (defclass |baz| ()
    ()
    (:metaclass avro:fixed)
    (:namespace "foo.bar")
    (:size 12))
  (defclass union<string-foo.bar.baz> ()
    ()
    (:metaclass avro:union)
    (:schemas avro:string |baz|)))

(define-io-test io
    ((expected "foobar"))
    (make-instance 'avro:union :schemas '(avro:null avro:string))
    (make-instance schema :object expected)
    (2 12 #x66 #x6f #x6f #x62 #x61 #x72)
  (is (string= expected (avro:object arg)))
  (is (null (avro:object (make-instance schema :object nil))))
  (signals error
    (make-instance schema :object 3)))

(test late-type-check
  (setf (find-class 'late_union) nil
        (find-class 'late_fixed) nil)

  (defclass late_union ()
    ()
    (:metaclass avro:union)
    (:schemas avro:string late_fixed))

  (signals error
    (avro:schemas (find-class 'late_union)))

  (defclass late_fixed ()
    ()
    (:metaclass avro:fixed)
    (:size 12))

  (is (eq (find-class 'late_fixed)
          (elt (avro:schemas (find-class 'late_union)) 1))))

(test no-schemas
  (signals error
    (make-instance
     'avro:union
     :schemas #())))

(test non-schema
  (setf (find-class 'some_union) nil
        (find-class 'some_class) nil)

  (defclass some_class ()
    ())

  (defclass some_union ()
    ()
    (:metaclass avro:union)
    (:schemas avro:string some_class))

  (signals error
    (avro:schemas (find-class 'some_union))))

(test duplicate-primitive
  (let ((schema (make-instance
                 'avro:union
                 :schemas '(avro:string avro:null avro:string))))
    (signals error
      (avro:schemas schema))))

(test duplicate-array
  (let* ((array<int> (make-instance 'avro:array :items 'avro:int))
         (array<string> (make-instance 'avro:array :items 'avro:string))
         (schema (make-instance
                  'avro:union
                  :schemas (list 'avro:null array<int> array<string>))))
    (signals error
      (avro:schemas schema))))

(test duplicate-named
  (let* ((fixed (make-instance 'avro:fixed :name "foo" :size 12))
         (enum (make-instance 'avro:enum :name "foo" :symbols '("FOO" "BAR")))
         (schema (make-instance
                  'avro:union
                  :schemas (list 'avro:null fixed enum))))
    (signals error
      (avro:schemas schema))))

(test different-name-same-type
  (let* ((fixed-1 (make-instance 'avro:fixed :name "foo" :size 12))
         (fixed-2 (make-instance 'avro:fixed :name "bar" :size 12))
         (schema (make-instance
                  'avro:union
                  :schemas (list 'avro:null fixed-1 fixed-2))))
    (is (equalp (vector 'avro:null fixed-1 fixed-2)
                (avro:schemas schema)))))
