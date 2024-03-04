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
(defpackage #:org.apache.avro.internal.logical.big-endian
  (:use #:cl)
  (:local-nicknames
   (#:bytes #:org.apache.avro.internal.bytes))
  (:import-from #:org.apache.avro.internal.type
                #:vector<uint8>
                #:ufixnum
                #:uint8)
  (:export #:from-vector
           #:from-stream
           #:to-vector
           #:to-stream))
(in-package #:org.apache.avro.internal.logical.big-endian)

(deftype shift ()
  `(integer 0 ,bytes:+max-size+))

;;; from-vector

(declaim
 (ftype (function (vector<uint8> ufixnum ufixnum)
                  (values (integer 0) ufixnum &optional))
        from-vector))
(defun from-vector (input start end)
  (loop
    with integer of-type (integer 0) = 0

    for index of-type ufixnum from start below end
    for byte of-type uint8 = (elt input index)
    for shift of-type shift = (* 8 (1- (- end start))) then (- shift 8)

    do (setf integer (logior integer (ash byte shift)))

    finally
       (return
         (values integer (- end start)))))

;;; from-stream

(declaim
 (ftype (function (stream ufixnum) (values (integer 0) ufixnum &optional))
        from-stream))
(defun from-stream (input size)
  (loop
    with integer of-type (integer 0) = 0

    repeat size
    for byte of-type uint8 = (read-byte input)
    for shift of-type shift = (* 8 (1- size)) then (- shift 8)

    do (setf integer (logior integer (ash byte shift)))

    finally
       (return
         (values integer size))))

;;; to-vector

(declaim
 (ftype (function ((integer 0) vector<uint8> ufixnum ufixnum)
                  (values ufixnum &optional))
        to-vector))
(defun to-vector (integer into start end)
  (loop
    for index of-type ufixnum from start below end
    for shift of-type shift = (* 8 (1- (- end start))) then (- shift 8)
    for byte of-type uint8 = (logand #xff (ash integer (- shift)))

    do (setf (elt into index) byte)

    finally
       (return
         (- end start))))

;;; to-stream

(declaim
 (ftype (function ((integer 0) stream ufixnum) (values ufixnum &optional))
        to-stream))
(defun to-stream (integer into size)
  (loop
    repeat size
    for shift of-type shift = (* 8 (1- size)) then (- shift 8)
    for byte of-type uint8 = (logand #xff (ash integer (- shift)))

    do (write-byte byte into)

    finally
       (return
         size)))
