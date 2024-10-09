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
(defpackage #:org.apache.avro/test/uuid
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:json-syntax
                #:json-string=
                #:define-io-test))
(in-package #:org.apache.avro/test/uuid)

(named-readtables:in-readtable json-syntax)

(test schema
  (let ((json {"type": "string", "logicalType": "uuid"})
        (fingerprint #x33ec648d41dc37c9)
        (expected (find-class 'avro:uuid)))
    (is (eq expected (avro:deserialize 'avro:schema json)))
    (is (json-string= json (avro:serialize expected)))
    (is (= fingerprint (avro:fingerprint expected)))))

(define-io-test io
    ((expected "6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
    avro:uuid
    (make-instance 'avro:uuid :uuid expected)
    (72
     #x36 #x62 #x61 #x37 #x62 #x38 #x31 #x30 #x2d #x39 #x64 #x61 #x64
     #x2d #x31 #x31 #x64 #x31 #x2d #x38 #x30 #x62 #x34 #x2d #x30 #x30
     #x63 #x30 #x34 #x66 #x64 #x34 #x33 #x30 #x63 #x38)
  (is (string= expected (avro:raw arg)))
  (signals (or error warning)
    (make-instance 'avro:uuid :uuid "abc"))
  (signals (or error warning)
    (make-instance 'avro:uuid :uuid "")))

;; TODO move this into some generic logical fall-through
#+nil
(let ((schema (avro:deserialize
               'avro:schema "{type: \"bytes\", logicalType: \"uuid\"}")))
  (is (eq 'avro:bytes schema)))
