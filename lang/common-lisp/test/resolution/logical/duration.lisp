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
(defpackage #:org.apache.avro/test/resolution/duration
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro)))
(in-package #:org.apache.avro/test/resolution/duration)

(defclass writer_fixed ()
  ()
  (:metaclass avro:fixed)
  (:size 12))

(defclass reader_fixed ()
  ()
  (:metaclass avro:fixed)
  (:size 12))

(defclass writer-schema ()
  ()
  (:metaclass avro:duration)
  (:underlying writer_fixed))

(defclass reader-schema ()
  ()
  (:metaclass avro:duration)
  (:underlying reader_fixed))

(test duration->duration
  (let* ((writer (make-instance
                  'writer-schema :months 3 :days 4 :milliseconds 5))
         (reader (avro:coerce
                  (avro:deserialize 'writer-schema (avro:serialize writer))
                  'reader-schema)))
    (is (typep writer 'writer-schema))
    (is (typep reader 'reader-schema))
    (is (= (avro:months writer) (avro:months reader)))
    (is (= (avro:days writer) (avro:days reader)))
    (is (= (avro:milliseconds writer) (avro:milliseconds reader)))))
