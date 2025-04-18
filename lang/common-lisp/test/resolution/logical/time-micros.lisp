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
(defpackage #:org.apache.avro/test/resolution/time-micros
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/resolution/base
                #:find-schema
                #:initarg-for-millis/micros
                #:microsecond))
(in-package #:org.apache.avro/test/resolution/time-micros)

(declaim
 (ftype (function (local-time:timestamp local-time:timestamp)
                  (values &optional))
        assert=))
(defun assert= (writer reader)
  (is (= (avro:hour writer) (avro:hour reader)))
  (is (= (avro:minute writer) (avro:minute reader)))
  (is (= (microsecond writer) (microsecond reader)))
  (values))

(test time-micros->time-micros
  (let* ((writer (make-instance
                  'avro:time-micros :hour 2 :minute 25 :microsecond 32350450))
         (reader (avro:coerce
                  (avro:deserialize 'avro:time-micros (avro:serialize writer))
                  'avro:time-micros)))
    (is (typep writer 'avro:time-micros))
    (is (typep reader 'avro:time-micros))
    (assert= writer reader)))

(test time-millis->time-micros
  (let* ((writer (make-instance
                  'avro:time-millis :hour 2 :minute 25 :millisecond 32350))
         (reader (avro:coerce
                  (avro:deserialize 'avro:time-millis (avro:serialize writer))
                  'avro:time-micros)))
    (is (typep writer 'avro:time-millis))
    (is (typep reader 'avro:time-micros))
    (assert= writer reader)))
