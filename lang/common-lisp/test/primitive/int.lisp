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
(defpackage #:org.apache.avro/test/int
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/int)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "int"})
        (fingerprint #x7275d51a3f395c8f))
    (is (eq 'avro:int (avro:deserialize 'avro:schema json)))
    (is (string= "\"int\"" (avro:serialize 'avro:int)))
    (is (= fingerprint (avro:fingerprint 'avro:int)))))

(test canonical-form
  (let ((json "\"int\"")
        (fingerprint #x7275d51a3f395c8f))
    (is (eq 'avro:int (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:int :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint 'avro:int)))))

(macrolet
    ((make-tests (&rest int->serialized)
       `(progn
          ,@(mapcar
             (lambda (int&serialized)
               `(define-io-test
                    ,(intern (format nil "IO-~A" (car int&serialized)))
                    ()
                    avro:int
                    ,(car int&serialized)
                    ,(cdr int&serialized)
                  (is (= object arg))))
             int->serialized))))
  (make-tests
   (0 . (#x00))
   (-1 . (#x01))
   (1 . (#x02))
   (-2 . (#x03))
   (2 . (#x04))
   (-64 . (#x7f))
   (64 . (#x80 #x01))
   (8192 . (#x80 #x80 #x01))
   (-8193 . (#x81 #x80 #x01))))

(define-io-test io-min
    ()
    avro:int
    (- (expt 2 31))
    (#xff #xff #xff #xff #x0f)
  (is (= object arg))
  (is (not (typep (1- arg) 'avro:int)))
  ;; this gets serialized as a long so that's why we check for an
  ;; error during deserialization
  (signals error
    (avro:deserialize 'avro:int (avro:serialize (1- arg)))))

(define-io-test io-max
    ()
    avro:int
    (1- (expt 2 31))
    (#xfe #xff #xff #xff #x0f)
  (is (= object arg))
  (is (not (typep (1+ arg) 'avro:int)))
  (signals error
    (avro:deserialize 'avro:int (avro:serialize (1+ arg)))))
