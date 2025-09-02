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
(defpackage #:org.apache.avro/test/reinitialization
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:use #:cl #:1am))
(in-package #:org.apache.avro/test/reinitialization)

(test direct
  (let ((schema (make-instance 'avro:fixed :name "foo" :size 1)))
    (is (= 1 (avro:size schema)))
    (is (eq schema (reinitialize-instance schema :name "foo" :size 2)))
    (is (= 2 (avro:size schema)))))

(test inherited
  (let ((schema (make-instance
                 'avro:fixed :name "bar" :namespace "foo" :size 1)))
    (is (string= "foo.bar" (avro:fullname schema)))
    (is (eq schema (reinitialize-instance schema :name "bar" :size 1)))
    (is (string= "bar" (avro:fullname schema)))))

(test invalid
  (let ((schema (make-instance 'avro:fixed :name "foo" :size 1)))
    (signals error
      (reinitialize-instance schema :name "foo"))))
