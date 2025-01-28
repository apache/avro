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
(defpackage #:org.apache.avro.internal.name.coerce
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:deduce #:org.apache.avro.internal.name.deduce)
   (#:schema #:org.apache.avro.internal.name.schema))
  (:export #:assert-matching-names))
(in-package #:org.apache.avro.internal.name.coerce)

(declaim
 (ftype (function (schema:valid-name schema:valid-fullname)
                  (values boolean &optional))
        matching-alias-p))
(defun matching-alias-p (writer-name reader-alias)
  (string= writer-name (deduce:fullname->name reader-alias)))

(declaim
 (ftype (function (schema:valid-name schema:array<alias>)
                  (values boolean &optional))
        matching-aliases-p))
(defun matching-aliases-p (writer-name reader-aliases)
  (flet ((matching-alias-p (reader-alias)
           (matching-alias-p writer-name reader-alias)))
    (some #'matching-alias-p reader-aliases)))

(declaim
 (ftype (function (schema:named-schema schema:named-schema)
                  (values boolean &optional))
        matching-names-p))
(defun matching-names-p (reader writer)
  (let ((reader-name (api:name reader))
        (writer-name (api:name writer)))
    (declare (schema:valid-name reader-name writer-name))
    (or (string= reader-name writer-name)
        (let ((reader-aliases (api:aliases reader)))
          (declare (schema:array<alias>? reader-aliases))
          (when reader-aliases
            (matching-aliases-p writer-name reader-aliases))))))

(declaim
 (ftype (function (schema:named-schema schema:named-schema) (values &optional))
        assert-matching-names))
(defun assert-matching-names (reader writer)
  (unless (matching-names-p reader writer)
    (error "Names don't match between reader schema ~S and writer schema ~S"
           reader writer))
  (values))
