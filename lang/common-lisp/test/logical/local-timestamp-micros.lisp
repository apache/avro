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
(defpackage #:org.apache.avro/test/local-timestamp-micros
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:json-syntax
                #:json-string=
                #:define-io-test))
(in-package #:org.apache.avro/test/local-timestamp-micros)

(named-readtables:in-readtable json-syntax)

(test schema
  (let ((json {"type": "long", "logicalType": "local-timestamp-micros"})
        (fingerprint #x10d2958f23b25beb)
        (expected (find-class 'avro:local-timestamp-micros)))
    (is (eq expected (avro:deserialize 'avro:schema json)))
    (is (json-string= json (avro:serialize expected)))
    (is (= fingerprint (avro:fingerprint expected)))))

(define-io-test io
    ((year 2021)
     (month 5)
     (day 14)
     (hour 2)
     (minute 54)
     (microsecond 23800900))
    avro:local-timestamp-micros
    (make-instance
     'avro:local-timestamp-micros
     :year year
     :month month
     :day day
     :hour hour
     :minute minute
     :microsecond microsecond)
    (#x88 #xb9 #xda #xc7 #xab #x90 #xe1 #x05)
  (is (local-time:timestamp= object arg))
  (is (= year (avro:year arg)))
  (is (= month (avro:month arg)))
  (is (= day (avro:day arg)))
  (is (= hour (avro:hour arg)))
  (is (= minute (avro:minute arg)))
  (is (= microsecond
         (multiple-value-bind (second remainder)
             (avro:second arg)
           (+ (* 1000 1000 second)
              (* 1000 1000 remainder))))))
