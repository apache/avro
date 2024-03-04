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
(defpackage #:org.apache.avro.internal.name.deduce
  (:use #:cl)
  (:local-nicknames
   (#:type #:org.apache.avro.internal.name.type))
  (:import-from #:org.apache.avro.internal.type
                #:ufixnum)
  (:export #:fullname->name
           #:deduce-namespace
           #:deduce-fullname
           #:null-namespace-p))
(in-package #:org.apache.avro.internal.name.deduce)

(declaim
 (ftype (function (type:fullname) (values (or null ufixnum) &optional))
        last-dot-position))
(defun last-dot-position (fullname)
  (position #\. fullname :test #'char= :from-end t))

;;; fullname->name

(declaim
 (ftype (function (type:fullname) (values type:name &optional))
        fullname->name))
(defun fullname->name (fullname)
  "Return namespace unqualified name."
  (let ((last-dot-position (last-dot-position fullname)))
    (if last-dot-position
        (subseq fullname (1+ last-dot-position))
        fullname)))

;;; deduce-namespace

(declaim
 (ftype (function (type:namespace) (values boolean &optional))
        null-namespace-p))
(defun null-namespace-p (namespace)
  "True if NAMESPACE is either nil or empty."
  (or (null namespace)
      (zerop (length namespace))))

(declaim
 (ftype (function (type:fullname type:namespace type:namespace)
                  (values type:namespace &optional))
        deduce-namespace))
(defun deduce-namespace (fullname namespace enclosing-namespace)
  "Deduce namespace."
  (let ((last-dot-position (last-dot-position fullname)))
    (cond
      (last-dot-position
       (subseq fullname 0 last-dot-position))
      ((not (null-namespace-p namespace))
       namespace)
      (t
       enclosing-namespace))))

;;; deduce-fullname

(declaim
 (ftype (function ((and type:namespace (not null)) type:name)
                  (values type:fullname &optional))
        make-fullname))
(defun make-fullname (namespace name)
  (let* ((namespace-length (length namespace))
         (name-length (length name))
         (fullname (make-string
                    (+ 1 namespace-length name-length) :initial-element #\.)))
    (replace fullname namespace)
    (replace fullname name :start1 (1+ namespace-length))))

(declaim
 (ftype (function (type:fullname type:namespace type:namespace)
                  (values type:fullname &optional))
        deduce-fullname))
(defun deduce-fullname (fullname namespace enclosing-namespace)
  "Deduce fullname."
  (cond
    ((find #\. fullname :test #'char=)
     fullname)
    ((not (null-namespace-p namespace))
     (make-fullname namespace fullname))
    ((not (null-namespace-p enclosing-namespace))
     (make-fullname enclosing-namespace fullname))
    (t
     fullname)))
