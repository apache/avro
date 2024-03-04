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
(defpackage #:org.apache.avro/test/duration
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro)
   (#:internal #:org.apache.avro.internal))
  (:import-from #:org.apache.avro/test/common
                #:define-schema-test
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/duration)

(named-readtables:in-readtable json-syntax)

(define-schema-test schema
  {
    "type": {
      "type": "fixed",
      "name": "foo",
      "size": 12
    },
    "logicalType": "duration"
  }
  {
    "type": {
      "name": "foo",
      "type": "fixed",
      "size": 12
    },
    "logicalType": "duration"
  }
  #x49f6bf2b9652c399
  (make-instance
   'avro:duration
   :underlying (make-instance
                'avro:fixed
                :name "foo"
                :size 12))
  (defclass |foo| ()
    ()
    (:metaclass avro:fixed)
    (:size 12))
  (defclass duration ()
    ()
    (:metaclass avro:duration)
    (:underlying |foo|)))

(define-io-test io
    ((months 39)
     (days 17)
     (milliseconds 14831053))
    (make-instance
     'avro:duration
     :underlying (make-instance 'avro:fixed :name "foo" :size 12))
    (make-instance
     schema
     :years 3
     :months 3
     :weeks 2
     :days 3
     :hours 4
     :minutes 7
     :seconds 11
     :milliseconds 3
     :nanoseconds 50000000)
    (#x27 #x00 #x00 #x00
     #x11 #x00 #x00 #x00
     #xcd #x4d #xe2 #x00)
  ;; TODO check with size 11 to make sure fixed is returned do
  ;; this with the other logical-fallthrough tests
  (is (= months (avro:months arg)))
  (is (= days (avro:days arg)))
  (is (= milliseconds (avro:milliseconds arg))))

(test late-type-check
  (setf (find-class 'late_duration) nil
        (find-class 'late_fixed) nil)

  (defclass late_duration ()
    ()
    (:metaclass avro:duration)
    (:underlying late_fixed))

  (signals error
    (internal:underlying (find-class 'late_duration)))

  (defclass late_fixed ()
    ()
    (:metaclass avro:fixed)
    (:size 12))

  (is (eq (find-class 'late_fixed)
          (internal:underlying (find-class 'late_duration)))))

(test bad-fixed-size
  (let ((schema (make-instance
                 'avro:duration
                 :underlying (make-instance
                              'avro:fixed
                              :name "foo"
                              :size 13))))
    (signals error
      (internal:underlying schema))))
