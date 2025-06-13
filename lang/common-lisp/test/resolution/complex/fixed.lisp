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
(defpackage #:org.apache.avro/test/resolution/fixed
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro)))
(in-package #:org.apache.avro/test/resolution/fixed)

(test fixed
  (let* ((writer-schema (make-instance
                         'avro:fixed
                         :name "foo.bar"
                         :size 3))
         (reader-schema (make-instance
                         'avro:fixed
                         :name "baz"
                         :aliases '("baz.bar")
                         :size 3))
         (writer-object (make-instance
                         writer-schema :initial-contents '(2 4 6)))
         (reader-object (avro:coerce
                         (avro:deserialize
                          writer-schema (avro:serialize writer-object))
                         reader-schema)))
    (is (typep writer-object writer-schema))
    (is (typep reader-object reader-schema))
    (is (equal (coerce writer-object 'list) (coerce reader-object 'list)))))
