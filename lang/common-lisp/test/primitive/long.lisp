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
(defpackage #:org.apache.avro/test/long
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/long)

(named-readtables:in-readtable json-syntax)

(test non-canonical-form
  (let ((json {"type": "long"})
        (fingerprint #xd054e14493f41db7))
    (is (eq 'avro:long (avro:deserialize 'avro:schema json)))
    (is (string= "\"long\"" (avro:serialize 'avro:long)))
    (is (= fingerprint (avro:fingerprint 'avro:long)))))

(test canonical-form
  (let ((json "\"long\"")
        (fingerprint #xd054e14493f41db7))
    (is (eq 'avro:long (avro:deserialize 'avro:schema json)))
    (is (string= json (avro:serialize 'avro:long :canonical-form-p t)))
    (is (= fingerprint (avro:fingerprint 'avro:long)))))

(macrolet
    ((make-tests (&rest long->serialized)
       `(progn
          ,@(mapcar
             (lambda (long&serialized)
               `(define-io-test
                    ,(intern (format nil "IO-~A" (car long&serialized)))
                    ()
                    ,(if (typep (car long&serialized) '(signed-byte 32))
                         '(avro:long avro:int)
                         'avro:long)
                    ,(car long&serialized)
                    ,(cdr long&serialized)
                  (is (= object arg))))
             long->serialized))))
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
    avro:long
    (- (expt 2 63))
    (#xff #xff #xff #xff #xff #xff #xff #xff #xff #x01)
  (is (= object arg))
  (is (not (typep (1- arg) 'avro:long)))
  (signals error
    (avro:serialize (1- arg))))

(define-io-test io-max
    ()
    avro:long
    (1- (expt 2 63))
    (#xfe #xff #xff #xff #xff #xff #xff #xff #xff #x01)
  (is (= object arg))
  (is (not (typep (1+ arg) 'avro:long)))
  (signals error
    (avro:serialize (1+ arg))))
