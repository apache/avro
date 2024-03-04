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
(defpackage #:org.apache.avro/test/bytes
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/bytes)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "bytes"})
        (fingerprint #x4fc016dac3201965))
    (is (eq 'avro:bytes (avro:deserialize 'avro:schema json)))
    (is (string= "\"bytes\"" (avro:serialize 'avro:bytes)))
    (is (= fingerprint (avro:fingerprint 'avro:bytes)))))

(test canonical-form
  (let ((json "\"bytes\"")
        (fingerprint #x4fc016dac3201965))
    (is (eq 'avro:bytes (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:bytes :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint 'avro:bytes)))))

(define-io-test io
    ()
    avro:bytes
    (make-array 3 :element-type '(unsigned-byte 8)
                  :adjustable t :fill-pointer t
                  :initial-contents '(2 4 6))
    (6 2 4 6)
  (is (equalp object arg)))

(define-io-test io-empty
    ()
    avro:bytes
    (make-array 0 :element-type '(unsigned-byte 8)
                  :adjustable t :fill-pointer t)
    (0)
  (is (equalp object arg)))
