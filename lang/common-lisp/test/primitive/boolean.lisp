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
(defpackage #:org.apache.avro/test/boolean
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/boolean)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "boolean"})
        (fingerprint #x9f42fc78a4d4f764))
    (is (eq 'avro:boolean (avro:deserialize 'avro:schema json)))
    (is (string= "\"boolean\"" (avro:serialize 'avro:boolean)))
    (is (= fingerprint (avro:fingerprint 'avro:boolean)))))

(test canonical-form
  (let ((json "\"boolean\"")
        (fingerprint #x9f42fc78a4d4f764))
    (is (eq 'avro:boolean (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:boolean :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint 'avro:boolean)))))

(define-io-test io-true
    ()
    avro:boolean
    'avro:true
    (1)
  (is (eq 'avro:true arg)))

(define-io-test io-false
    ()
    avro:boolean
    'avro:false
    (0)
  (is (eq 'avro:false arg)))
