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
(defpackage #:org.apache.avro/test/double
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/double)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "double"})
        (fingerprint #x8e7535c032ab957e))
    (is (eq 'avro:double (avro:deserialize 'avro:schema json)))
    (is (string= "\"double\"" (avro:serialize 'avro:double)))
    (is (= fingerprint (avro:fingerprint 'avro:double)))))

(test canonical-form
  (let ((json "\"double\"")
        (fingerprint #x8e7535c032ab957e))
    (is (eq 'avro:double (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:double :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint 'avro:double)))))

(define-io-test io
    ()
    avro:double
    23.7d0
    (#x33 #x33 #x33 #x33 #x33 #xB3 #x37 #x40)
  (is (= object arg)))

(define-io-test io-zero
    ()
    avro:double
    0.0d0
    (0 0 0 0 0 0 0 0)
  (is (= object arg)))
