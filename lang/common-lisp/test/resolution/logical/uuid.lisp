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
(defpackage #:org.apache.avro/test/resolution/uuid
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro)))
(in-package #:org.apache.avro/test/resolution/uuid)

(test uuid->uuid
  (let* ((writer (make-instance
                  'avro:uuid :uuid "6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
         (reader (avro:coerce
                  (avro:deserialize 'avro:uuid (avro:serialize writer))
                  'avro:uuid)))
    (is (typep writer 'avro:uuid))
    (is (typep reader 'avro:uuid))
    (is (string= (avro:raw writer) (avro:raw reader)))))
