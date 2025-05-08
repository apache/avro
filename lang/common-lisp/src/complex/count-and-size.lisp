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
(defpackage #:org.apache.avro.internal.count-and-size
  (:use #:cl)
  (:local-nicknames
   (#:long #:org.apache.avro.internal.long))
  (:import-from #:org.apache.avro.internal.type
                #:ufixnum
                #:vector<uint8>)
  (:export #:from-vector
           #:from-stream))
(in-package #:org.apache.avro.internal.count-and-size)

(declaim
 (ftype (function (vector<uint8> ufixnum)
                  (values ufixnum (or null ufixnum) ufixnum &optional))
        from-vector))
(defun from-vector (input start)
  (multiple-value-bind (count bytes-read)
      (long:deserialize-from-vector input start)
    (declare (fixnum count))
    (if (not (minusp count))
        (values count nil bytes-read)
        (multiple-value-bind (size more-bytes-read)
            (long:deserialize-from-vector input (+ start bytes-read))
          ;; this is only ufixnum if the input is wellformed
          (declare (ufixnum size))
          (values (abs count) size (+ bytes-read more-bytes-read))))))

(declaim
 (ftype (function (stream)
                  (values ufixnum (or null ufixnum) ufixnum &optional))
        from-stream))
(defun from-stream (input)
  (multiple-value-bind (count bytes-read)
      (long:deserialize-from-stream input)
    (declare (fixnum count))
    (if (not (minusp count))
        (values count nil bytes-read)
        (multiple-value-bind (size more-bytes-read)
            (long:deserialize-from-stream input)
          (declare (ufixnum size))
          (values (abs count) size (+ bytes-read more-bytes-read))))))
