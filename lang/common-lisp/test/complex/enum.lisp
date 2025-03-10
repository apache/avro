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
(defpackage #:org.apache.avro/test/enum
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:define-schema-test
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/enum)

(named-readtables:in-readtable json-syntax)

(define-schema-test python-test
  {
    "type": "enum",
    "name": "Test",
    "symbols": ["A", "B"]
  }
  {
    "name": "Test",
    "type": "enum",
    "symbols": ["A", "B"]
  }
  #x167a7fe2c2f2a203
  (make-instance
   'avro:enum
   :name "Test"
   :symbols '("A" "B"))
  (defclass |Test| ()
    ()
    (:metaclass avro:enum)
    (:symbols "A" "B")))

(define-schema-test python-test-with-doc
  {
    "type": "enum",
    "name": "Test",
    "symbols": ["A", "B"],
    "doc": "Doc String"
  }
  {
    "name": "Test",
    "type": "enum",
    "symbols": ["A", "B"],
  }
  #x167a7fe2c2f2a203
  (make-instance
   'avro:enum
   :name '|Test|
   :symbols '("A" "B")
   :documentation "Doc String")
  (defclass |Test| ()
    ()
    (:metaclass avro:enum)
    (:symbols "A" "B")
    (:documentation "Doc String")))

(define-schema-test all-fields
  {
    "type": "enum",
    "name": "baz",
    "namespace": "foo.bar",
    "aliases": ["some", "foo.bar.alias"],
    "doc": "Here's a doc",
    "symbols": ["FOO", "BAR", "BAZ"],
    "default": "BAR"
  }
  {
    "name": "foo.bar.baz",
    "type": "enum",
    "symbols": ["FOO", "BAR", "BAZ"]
  }
  #x2d77464e9fd3b6b5
  (make-instance
   'avro:enum
   :name "baz"
   :namespace "foo.bar"
   :aliases '("some" "foo.bar.alias")
   :documentation "Here's a doc"
   :symbols '("FOO" "BAR" "BAZ")
   :default "BAR")
  (defclass |baz| ()
    ()
    (:metaclass avro:enum)
    (:namespace "foo.bar")
    (:aliases "some" "foo.bar.alias")
    (:documentation "Here's a doc")
    (:symbols "FOO" "BAR" "BAZ")
    (:default "BAR")))

(define-schema-test optional-fields
  {
    "type": "enum",
    "name": "EnumName",
    "aliases": [],
    "default": "FOO",
    "symbols": [
      "FOO",
      "BAR",
      "BAZ"
    ]
  }
  {
    "name": "EnumName",
    "type": "enum",
    "symbols": [
      "FOO",
      "BAR",
      "BAZ"
    ]
  }
  #xcda0ac7bef77b89c
  (make-instance
   'avro:enum
   :name "EnumName"
   :aliases nil
   :default "FOO"
   :symbols #("FOO" "BAR" "BAZ"))
  (defclass |EnumName| ()
    ()
    (:metaclass avro:enum)
    (:aliases)
    (:default "FOO")
    (:symbols "FOO" "BAR" "BAZ")))

(define-io-test io
    ((expected "B"))
    (make-instance 'avro:enum :name "foo" :symbols '("A" "B"))
    (make-instance schema :enum expected)
    (2)
  (is (string= expected (avro:which-one arg)))
  (signals error
    (make-instance schema :enum "C")))
