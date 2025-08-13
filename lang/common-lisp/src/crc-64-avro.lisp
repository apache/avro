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
(defpackage #:org.apache.avro.internal.crc-64-avro
  (:use #:cl)
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:uint64
                #:vector<uint8>)
  (:import-from #:alexandria
                #:define-constant)
  (:export #:crc-64-avro))
(in-package #:org.apache.avro.internal.crc-64-avro)

(declaim (uint64 +empty+))
(defconstant +empty+ #xc15d213aa4d7a795)

(declaim ((simple-array uint64 (256)) +table+))
(define-constant +table+
    (map '(simple-array uint64 (256))
         (lambda (i)
           (loop
             repeat 8
             do (setf i (logxor (ash i -1)
                                (logand +empty+
                                        (- (logand i 1)))))
             finally (return i)))
         (loop for i below 256 collect i))
  :test #'equalp)

(declaim
 (ftype (function (uint64 uint8) (values uint64 &optional)) %crc-64-avro))
(defun %crc-64-avro (fp byte)
  (let ((table-ref (elt +table+ (logand #xff (logxor fp byte)))))
    (logxor (ash fp -8) table-ref)))

(declaim
 (ftype (function (vector<uint8>) (values uint64 &optional)) crc-64-avro))
(defun crc-64-avro (bytes)
  (reduce #'%crc-64-avro bytes :initial-value +empty+))
