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
(defpackage #:org.apache.avro.internal.type
  (:use #:cl)
  (:export #:uint8
           #:uint32
           #:uint64
           #:ufixnum
           #:vector<uint8>
           #:array<uint8>
           #:comparison))
(in-package #:org.apache.avro.internal.type)

;;; numbers

(deftype uint8 ()
  "Unsigned byte."
  '(unsigned-byte 8))

(deftype uint32 ()
  "Unsigned 32-bits."
  '(unsigned-byte 32))

(deftype uint64 ()
  "Unsigned 64-bits."
  '(unsigned-byte 64))

(deftype ufixnum ()
  "Nonnegative fixnum."
  '(and fixnum (integer 0)))

;;; vectors

(deftype vector<uint8> (&optional size)
  "Vector of bytes."
  `(vector (unsigned-byte 8) ,(if size size *)))

(deftype array<uint8> (&optional size)
  "Simple array of bytes."
  `(simple-array (unsigned-byte 8) (,(if size size *))))

;;; comparison

(deftype comparison ()
  "Return type of compare."
  '(integer -1 1))
