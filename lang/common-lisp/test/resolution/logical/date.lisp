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
(defpackage #:org.apache.avro/test/resolution/date
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/resolution/base
                #:find-schema
                #:initarg-for-millis/micros))
(in-package #:org.apache.avro/test/resolution/date)

(test date->date
  (let* ((writer (make-instance 'avro:date :year 2021 :month 8 :day 6))
         (reader (avro:coerce
                  (avro:deserialize 'avro:date (avro:serialize writer))
                  'avro:date)))
    (is (typep writer 'avro:date))
    (is (typep reader 'avro:date))
    (is (= (avro:year writer) (avro:year reader)))
    (is (= (avro:month writer) (avro:month reader)))
    (is (= (avro:day writer) (avro:day reader)))))
