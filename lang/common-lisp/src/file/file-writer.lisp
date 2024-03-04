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
(defpackage #:org.apache.avro.internal.file.writer
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro))
  (:import-from #:org.apache.avro.internal.file.block
                #:objects->block
                #:codec->compress))
(in-package #:org.apache.avro.internal.file.writer)

;;; file-writer

(defclass api:file-writer ()
  ((output-stream
    :type stream
    :accessor output-stream)
   (file-header
    :type api:file-header
    :reader api:file-header
    :accessor file-header
    :documentation "File header.")
   (wrote-header-p
    :type boolean
    :initform nil
    :accessor wrote-header-p))
  (:documentation
   "A writer for an avro object container file."))

(defmethod initialize-instance :after
    ((instance api:file-writer)
     &key
       (output (error "Must supply OUTPUT"))
       (meta (error "Must supply META"))
       (sync (make-instance 'api:sync)))
  (with-accessors
        ((output-stream output-stream)
         (file-header file-header))
      instance
    (setf output-stream output
          file-header (make-instance 'api:file-header :sync sync :meta meta))))

;; TODO the skip/read-block TODO is applicable to write-block, too

(declaim (ftype (function (api:file-writer (vector api:object))
                          (values api:file-block &optional))
                api:write-block))
(defun api:write-block (file-writer objects)
  "Writes OBJECTS as a block into FILE-WRITER."
  (with-accessors
        ((file-header file-header)
         (output-stream output-stream)
         (wrote-header-p wrote-header-p))
      file-writer
    (unless wrote-header-p
      (api:serialize file-header :into output-stream)
      (setf wrote-header-p t))
    (let* ((header-sync (api:sync file-header))
           (schema (api:schema file-header))
           (compress (codec->compress (api:codec file-header)))
           (file-block (objects->block objects header-sync schema compress)))
      (api:serialize file-block :into output-stream)
      file-block)))
