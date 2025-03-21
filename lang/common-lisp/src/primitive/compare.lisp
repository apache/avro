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
(defpackage #:org.apache.avro.internal.compare
  (:use #:cl)
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:vector<uint8>
                #:ufixnum
                #:comparison)
  (:export #:compare-reals
           #:compare-byte-vectors
           #:compare-byte-streams))
(in-package #:org.apache.avro.internal.compare)

(declaim
 (ftype (function (real real) (values comparison &optional)) compare-reals))
(defun compare-reals (left right)
  (cond
    ((= left right) 0)
    ((< left right) -1)
    (t 1)))

(declaim
 (ftype (function (vector<uint8> vector<uint8> ufixnum ufixnum ufixnum ufixnum)
                  (values comparison ufixnum ufixnum &optional))
        compare-byte-vectors))
(defun compare-byte-vectors
    (left right left-start right-start left-end right-end)
  (loop
    for left-index from left-start below left-end
    for right-index from right-start below right-end

    for bytes-read of-type ufixnum from 1

    for left-byte of-type uint8 = (elt left left-index)
    for right-byte of-type uint8 = (elt right right-index)

    if (< left-byte right-byte)
      return (values -1 bytes-read bytes-read)
    else if (> left-byte right-byte)
           return (values 1 bytes-read bytes-read)

    finally
       (return
         (let ((left-length (- left-end left-start))
               (right-length (- right-end right-start)))
           (declare (ufixnum left-length right-length))
           (values (compare-reals left-length right-length)
                   bytes-read
                   bytes-read)))))

(declaim
 (ftype (function (stream stream ufixnum ufixnum)
                  (values comparison ufixnum ufixnum &optional))
        compare-byte-streams))
(defun compare-byte-streams (left right left-length right-length)
  (loop
    repeat (min left-length right-length)
    for bytes-read of-type ufixnum from 1

    ;; TODO deftype a stream<uint8> and let folks optimize as needed
    for left-byte of-type uint8 = (read-byte left)
    for right-byte of-type uint8 = (read-byte right)

    if (< left-byte right-byte)
      return (values -1 bytes-read bytes-read)
    else if (> left-byte right-byte)
           return (values 1 bytes-read bytes-read)

    finally
       (return
         (values (compare-reals left-length right-length)
                 bytes-read
                 bytes-read))))
