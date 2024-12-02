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
(defpackage #:org.apache.avro.internal.ascii
  (:use #:cl)
  (:export #:digit-p
           #:letter-p
           #:hex-p))
(in-package #:org.apache.avro.internal.ascii)

(defmacro between (code start end)
  "Determines if CODE is between the char-codes of START and END, inclusive."
  (declare (symbol code)
           (character start end))
  (let ((start (char-code start))
        (end (char-code end)))
    `(and (>= ,code ,start)
          (<= ,code ,end))))

(declaim (ftype (function (character) (values boolean &optional)) digit-p))
(defun digit-p (digit)
  "True if DIGIT is a base-10 digit."
  (let ((code (char-code digit)))
    (between code #\0 #\9)))

(declaim (ftype (function (character) (values boolean &optional)) letter-p))
(defun letter-p (letter)
  "True if LETTER is a an ascii letter conforming to /[a-zA-Z]/"
  (let ((code (char-code letter)))
    (or (between code #\a #\z)
        (between code #\A #\Z))))

(declaim (ftype (function (character) (values boolean &optional)) hex-p))
(defun hex-p (hex)
  "True if HEX is a hex digit conforming to /[0-9a-f-A-F]/"
  (let ((code (char-code hex)))
    (or (between code #\0 #\9)
        (between code #\a #\f)
        (between code #\A #\F))))
