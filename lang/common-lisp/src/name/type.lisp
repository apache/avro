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
(defpackage #:org.apache.avro.internal.name.type
  (:use #:cl)
  (:local-nicknames
   (#:ascii #:org.apache.avro.internal.ascii))
  (:import-from #:org.apache.avro.internal.type
                #:ufixnum)
  (:export #:name
           #:namespace
           #:fullname))
(in-package #:org.apache.avro.internal.name.type)

;;; name

(declaim
 (ftype (function (simple-string ufixnum ufixnum) (values boolean &optional))
        %name-p))
(defun %name-p (string start end)
  (unless (>= start end)
    (let ((first-char (char string start)))
      (when (or (ascii:letter-p first-char)
                (char= #\_ first-char))
        (loop
          for i of-type ufixnum from (1+ start) below end
          for char of-type character = (char string i)

          always (or (ascii:letter-p char)
                     (ascii:digit-p char)
                     (char= #\_ char)))))))

(declaim (ftype (function (t) (values boolean &optional)) name-p))
(defun name-p (string)
  "True if NAME matches regex /^[A-Za-z_][A-Za-z0-9_]*$/"
  (when (simple-string-p string)
    (%name-p string 0 (length string))))

(deftype name ()
  "A string that matches the regex /^[A-Za-z_][A-Za-z0-9_]*$/"
  '(and simple-string (satisfies name-p)))

;;; fullname

(declaim
 (ftype (function (simple-string) (values boolean &optional)) %fullname-p))
(defun %fullname-p (string)
  (loop
    with length = (length string)
    for start of-type ufixnum = 0 then (1+ end)
    for end of-type (or ufixnum null) = (position #\. string :start start)
    always (%name-p string start (or end length))
    while end))

(declaim (ftype (function (t) (values boolean &optional)) fullname-p))
(defun fullname-p (string)
  "True if FULLNAME is a nonempty, dot-separated string of NAMES."
  (when (simple-string-p string)
    (%fullname-p string)))

(deftype fullname ()
  "A nonempty, dot-separated string of NAMEs."
  '(and simple-string (satisfies fullname-p)))

;;; namespace

(declaim (ftype (function (t) (values boolean &optional)) namespace-p))
(defun namespace-p (string)
  "True if NAMESPACE is either nil, an empty string, or a FULLNAME."
  (or (null string)
      (when (simple-string-p string)
        (or (zerop (length string))
            (%fullname-p string)))))

(deftype namespace ()
  "Either nil, an empty string, or a FULLNAME."
  '(or null (and simple-string (satisfies namespace-p))))
