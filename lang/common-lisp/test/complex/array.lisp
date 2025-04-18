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
(defpackage #:org.apache.avro/test/array
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:define-schema-test
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/array)

(named-readtables:in-readtable json-syntax)

;; TODO need to support default
(define-schema-test long-array
  {
    "type": "array",
    "items": "long"
  }
  {
    "type": "array",
    "items": "long"
  }
  #x5416c98ba22e5e71
  (make-instance
   'avro:array
   :items 'avro:long)
  (defclass long_array ()
    ()
    (:metaclass avro:array)
    (:items avro:long)))

(define-schema-test enum-array
  {
    "type": "array",
    "items": {
      "type": "enum",
      "name": "Test",
      "symbols": ["A", "B"]
    }
  }
  {
    "type": "array",
    "items": {
      "name": "Test",
      "type": "enum",
      "symbols": ["A", "B"]
    }
  }
  #x87033afae1add910
  (make-instance
   'avro:array
   :items (make-instance
           'avro:enum
           :name "Test"
           :symbols '("A" "B")))
  (defclass |Test| ()
    ()
    (:metaclass avro:enum)
    (:symbols "A" "B"))
  (defclass enum_array ()
    ()
    (:metaclass avro:array)
    (:items |Test|)))

(define-schema-test record-array
  {
    "type": "array",
    "items": {
      "type": "record",
      "name": "RecordName",
      "fields": [
        {
          "name": "Foo",
          "type": "string",
          "default": "foo"
        },
        {
          "name": "Bar",
          "type": "int",
          "default": 7
        }
      ]
    }
  }
  {
    "type": "array",
    "items": {
      "name": "RecordName",
      "type": "record",
      "fields": [
        {
          "name": "Foo",
          "type": "string",
        },
        {
          "name": "Bar",
          "type": "int",
        }
      ]
    }
  }
  #x2a6ad7a6c7ab28ea
  (make-instance
   'avro:array
   :items (make-instance
           'avro:record
           :name "RecordName"
           :direct-slots
           `((:name |Foo| :type avro:string :default "foo")
             (:name |Bar| :type avro:int :default 7))))
  (defclass |RecordName| ()
    ((|Foo| :type avro:string :default "foo")
     (|Bar| :type avro:int :default 7))
    (:metaclass avro:record))
  (defclass array<record-name> ()
    ()
    (:metaclass avro:array)
    (:items |RecordName|)))

(define-io-test io
    ((enum-schema (make-instance 'avro:enum :name "Test" :symbols '("A" "B")))
     (expected '("A" "A" "B")))
    (make-instance 'avro:array :items enum-schema)
    (make-instance
     schema
     :initial-contents (mapcar
                        (lambda (enum)
                          (make-instance enum-schema :enum enum))
                        expected))
    (5 6 0 0 2 0)
  (is (equal expected (map 'list #'avro:which-one arg))))

(test late-type-check
  (setf (find-class 'late_array) nil
        (find-class 'late_enum) nil)

  (defclass late_array ()
    ()
    (:metaclass avro:array)
    (:items late_enum))

  (signals error
    (avro:items (find-class 'late_array)))

  (defclass late_enum ()
    ()
    (:metaclass avro:enum)
    (:symbols "FOO" "BAR"))

  (is (eq (find-class 'late_enum) (avro:items (find-class 'late_array)))))
