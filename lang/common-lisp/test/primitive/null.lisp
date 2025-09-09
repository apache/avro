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
(defpackage #:org.apache.avro/test/null
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/null)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "null"})
        (fingerprint #x63dd24e7cc258f8a))
    (is (eq 'avro:null (avro:deserialize 'avro:schema json)))
    (is (string= "\"null\"" (avro:serialize 'avro:null)))
    (is (= fingerprint (avro:fingerprint 'avro:null)))))

(test canonical-form
  (let ((json "\"null\"")
        (fingerprint #x63dd24e7cc258f8a))
    (is (eq 'avro:null (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:null :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint 'avro:null)))))

(define-io-test io
    ()
    avro:null
    nil
    ()
  (is (eq object arg))
  (multiple-value-bind (deserialized size)
      (let ((bytes (make-array 3 :element-type '(unsigned-byte 8)
                                 :initial-contents '(2 4 6))))
        (avro:deserialize 'avro:null bytes))
    (is (zerop size))
    (is (eq object deserialized))))
