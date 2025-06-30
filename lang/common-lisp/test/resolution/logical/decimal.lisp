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
(defpackage #:org.apache.avro/test/resolution/decimal
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro)))
(in-package #:org.apache.avro/test/resolution/decimal)

;;; fixed underlying

(defclass fixed_size_2 ()
  ()
  (:metaclass avro:fixed)
  (:size 2))

(defclass fixed_size_3 ()
  ()
  (:metaclass avro:fixed)
  (:size 3))

;;; writer schema

(defclass writer-fixed ()
  ()
  (:metaclass avro:decimal)
  (:precision 3)
  (:scale 2)
  (:underlying fixed_size_2))

(defclass writer-bytes ()
  ()
  (:metaclass avro:decimal)
  (:precision 3)
  (:scale 2)
  (:underlying avro:bytes))

;;; reader schema

(defclass reader-fixed ()
  ()
  (:metaclass avro:decimal)
  (:precision 3)
  (:scale 2)
  (:underlying fixed_size_3))

(defclass reader-bytes ()
  ()
  (:metaclass avro:decimal)
  (:precision 3)
  (:scale 2)
  (:underlying avro:bytes))

;;; mismatched precision/scale

(defclass bad-precision ()
  ()
  (:metaclass avro:decimal)
  (:precision 2)
  (:scale 2)
  (:underlying avro:bytes))

(defclass bad-scale ()
  ()
  (:metaclass avro:decimal)
  (:precision 3)
  (:scale 1)
  (:underlying avro:bytes))

(test fixed->fixed
  (let* ((writer (make-instance 'writer-fixed :unscaled 123))
         (reader (avro:coerce
                  (avro:deserialize 'writer-fixed (avro:serialize writer))
                  'reader-fixed)))
    (is (typep writer 'writer-fixed))
    (is (typep reader 'reader-fixed))
    (is (= (avro:unscaled writer) (avro:unscaled reader)))))

(test fixed->bytes
  (let* ((writer (make-instance 'writer-fixed :unscaled 123))
         (reader (avro:coerce
                  (avro:deserialize 'writer-fixed (avro:serialize writer))
                  'reader-bytes)))
    (is (typep writer 'writer-fixed))
    (is (typep reader 'reader-bytes))
    (is (= (avro:unscaled writer) (avro:unscaled reader)))))

(test bytes->bytes
  (let* ((writer (make-instance 'writer-bytes :unscaled 123))
         (reader (avro:coerce
                  (avro:deserialize 'writer-bytes (avro:serialize writer))
                  'reader-bytes)))
    (is (typep writer 'writer-bytes))
    (is (typep reader 'reader-bytes))
    (is (= (avro:unscaled writer) (avro:unscaled reader)))))

(test bytes->fixed
  (let* ((writer (make-instance 'writer-bytes :unscaled 123))
         (reader (avro:coerce
                  (avro:deserialize 'writer-bytes (avro:serialize writer))
                  'reader-fixed)))
    (is (typep writer 'writer-bytes))
    (is (typep reader 'reader-fixed))
    (is (= (avro:unscaled writer) (avro:unscaled reader)))))

(test mismatched-precision
  (let ((writer (make-instance 'writer-bytes :unscaled 123)))
    (signals error
      (avro:coerce
       (avro:deserialize 'writer-bytes (avro:serialize writer))
       'bad-precision))))

(test mismatched-scale
  (let ((writer (make-instance 'writer-bytes :unscaled 123)))
    (signals error
      (avro:coerce
       (avro:deserialize 'writer-bytes (avro:serialize writer))
       'bad-scale))))
