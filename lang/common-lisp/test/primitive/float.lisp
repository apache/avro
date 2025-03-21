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
(defpackage #:org.apache.avro/test/float
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/float)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "float"})
        (fingerprint #x4d7c02cb3ea8d790))
    (is (eq 'avro:float (avro:deserialize 'avro:schema json)))
    (is (string= "\"float\"" (avro:serialize 'avro:float)))
    (is (= fingerprint (avro:fingerprint 'avro:float)))))

(test canonical-form
  (let ((json "\"float\"")
        (fingerprint #x4d7c02cb3ea8d790))
    (is (eq 'avro:float (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:float :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint 'avro:float)))))

(define-io-test io
    ()
    avro:float
    23.7f0
    (#x9a #x99 #xbd #x41)
  (is (= object arg)))

(define-io-test io-zero
    ()
    avro:float
    0.0f0
    (0 0 0 0)
  (is (= object arg)))
