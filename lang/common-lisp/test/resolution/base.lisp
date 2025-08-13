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
(defpackage #:org.apache.avro/test/resolution/base
  (:use #:cl)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:export #:find-schema
           #:bytes
           #:initarg-for-millis/micros
           #:millisecond
           #:microsecond))
(in-package #:org.apache.avro/test/resolution/base)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim
   (ftype (function (symbol) (values symbol &optional)) find-schema))
  (defun find-schema (name)
    (multiple-value-bind (schema status)
        (find-symbol (string name) 'avro)
      (unless (eq status :external)
        (error "~S does not name a schema" name))
      (unless (typep schema 'avro:primitive-schema)
        (check-type (find-class schema) avro:schema))
      schema))

  (declaim (ftype (function (string) (values string &optional)) suffix))
  (defun suffix (string)
    (let ((last-hyphen (position #\- string :from-end t :test #'char=)))
      (unless last-hyphen
        (error "~S does not contain a hyphen" string))
      (subseq string (1+ last-hyphen))))

  (declaim
   (ftype (function (symbol)
                    (values (or (eql :millisecond)
                                (eql :microsecond)) &optional))
          initarg-for-millis/micros))
  (defun initarg-for-millis/micros (symbol)
    (let ((suffix (suffix (string symbol))))
      (cond
        ((string= suffix "MILLIS") :millisecond)
        ((string= suffix "MICROS") :microsecond)
        (t (error "~S is neither MILLIS or MICROS" suffix))))))

(declaim
 (ftype (function (&rest (unsigned-byte 8))
                  (values (simple-array (unsigned-byte 8) (*)) &optional))
        bytes))
(defun bytes (&rest bytes)
  (make-array
   (length bytes) :element-type '(unsigned-byte 8) :initial-contents bytes))

(declaim
 (ftype (function (local-time:timestamp) (values avro:int &optional))
        millisecond))
(defun millisecond (time)
  (multiple-value-bind (second remainder)
      (avro:second time)
    (nth-value 0 (truncate
                  (+ (* 1000 second)
                     (* 1000 remainder))))))

(declaim
 (ftype (function (local-time:timestamp) (values avro:long &optional))
        microsecond))
(defun microsecond (time)
  (multiple-value-bind (second remainder)
      (avro:second time)
    (+ (* 1000 1000 second)
       (* 1000 1000 remainder))))
