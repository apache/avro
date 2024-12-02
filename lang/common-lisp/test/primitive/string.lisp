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
(defpackage #:org.apache.avro/test/string
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/string)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "string"})
        (fingerprint #x8f014872634503c7))
    (is (eq 'avro:string (avro:deserialize 'avro:schema json)))
    (is (string= "\"string\"" (avro:serialize 'avro:string)))
    (is (= fingerprint (avro:fingerprint 'avro:string)))))

(test canonical-form
  (let ((json "\"string\"")
        (fingerprint #x8f014872634503c7))
    (is (eq 'avro:string (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:string :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint 'avro:string)))))

(define-io-test io
    ()
    avro:string
    (concatenate 'string (string #\u1f44b) " hello world!")
    (34                                 ; length
     #xf0 #x9f #x91 #x8b                ; waving hand sign emoji
     #x20 #x68 #x65 #x6c #x6c #x6f      ; <space> hello
     #x20 #x77 #x6f #x72 #x6c #x64 #x21 ; <space> world!
     )
  (is (string= object arg)))

(define-io-test io-empty
    ()
    avro:string
    ""
    (0)
  (is (string= object arg)))
