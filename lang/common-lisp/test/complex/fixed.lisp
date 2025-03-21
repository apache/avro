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
(defpackage #:org.apache.avro/test/fixed
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:define-schema-test
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/fixed)

(named-readtables:in-readtable json-syntax)

(define-schema-test schema
  {
    "type": "fixed",
    "name": "FixedName",
    "namespace": "fixed.name.space",
    "aliases": ["foo", "foo.bar"],
    "size": 12
  }
  {
    "name": "fixed.name.space.FixedName",
    "type": "fixed",
    "size": 12
  }
  #x606d48807f193010
  (make-instance
   'avro:fixed
   :name '|FixedName|
   :namespace "fixed.name.space"
   :aliases '("foo" "foo.bar")
   :size 12)
  (defclass |FixedName| ()
    ()
    (:metaclass avro:fixed)
    (:namespace "fixed.name.space")
    (:aliases "foo" "foo.bar")
    (:size 12)))

(define-io-test io
    ((expected '(2 4 6)))
    (make-instance 'avro:fixed :name 'fixed :size 3)
    (make-instance schema :initial-contents expected)
    (2 4 6)
  (is (equal expected (coerce arg 'list))))

(define-schema-test python-test
  {
    "type": "fixed",
    "name": "Test",
    "size": 1
  }
  {
    "name": "Test",
    "type": "fixed",
    "size": 1
  }
  #x5b3549407b896968
  (make-instance
   'avro:fixed
   :name "Test"
   :size 1)
  (defclass |Test| ()
    ()
    (:metaclass avro:fixed)
    (:size 1)))

(define-schema-test python-my-fixed
  {
    "type": "fixed",
    "name": "MyFixed",
    "namespace": "org.apache.hadoop.avro",
    "size": 1
  }
  {
    "name": "org.apache.hadoop.avro.MyFixed",
    "type": "fixed",
    "size": 1
  }
  #x45df5be838d1dbfa
  (make-instance
   'avro:fixed
   :name '|MyFixed|
   :namespace "org.apache.hadoop.avro"
   :size 1)
  (defclass |MyFixed| ()
    ()
    (:metaclass avro:fixed)
    (:namespace "org.apache.hadoop.avro")
    (:size 1)))
