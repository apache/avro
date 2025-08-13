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
(defpackage #:org.apache.avro.internal.file.reader
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro))
  (:import-from #:org.apache.avro.internal.type
                #:uint8)
  (:import-from #:org.apache.avro.internal.file.block
                #:block->objects
                #:codec->decompress))
(in-package #:org.apache.avro.internal.file.reader)

;;; file-reader

(deftype %input-stream ()
  '(or flexi-streams:flexi-input-stream flexi-streams:in-memory-input-stream))

(defclass api:file-reader ()
  ((input-stream
    :type %input-stream
    :accessor input-stream)
   (file-header
    :type api:file-header
    :reader api:file-header
    :accessor file-header
    :documentation "File header.")
   (file-block
    :type (or null api:file-block)
    :accessor file-block))
  (:documentation
   "A reader for an avro object container file."))

(declaim (ftype (function (t) (values %input-stream &optional)) parse-input))
(defun parse-input (input)
  (if (streamp input)
      (flexi-streams:make-flexi-stream input :element-type 'uint8)
      (flexi-streams:make-in-memory-input-stream input)))

(defmethod initialize-instance :after
    ((instance api:file-reader) &key (input (error "Must supply INPUT")))
  (with-accessors
        ((input-stream input-stream)
         (file-header file-header)
         (file-block file-block))
      instance
    (setf input-stream (parse-input input)
          file-header (api:deserialize 'api:file-header input-stream)
          file-block (api:deserialize 'api:file-block input-stream))))

(declaim
 (ftype (function (api:file-reader) (values &optional)) set-next-file-block))
(defun set-next-file-block (file-reader)
  (with-accessors
        ((input-stream input-stream)
         (file-block file-block))
      file-reader
    (setf file-block
          (when (flexi-streams:peek-byte input-stream nil nil nil)
            (api:deserialize 'api:file-block input-stream))))
  (values))

;; TODO both skip-block and read-block are applicable to array-reader
;; and map-reader, so they should be generic-functions instead

(declaim
 (ftype (function (api:file-reader)
                  (values (or null api:file-block) &optional))
        api:skip-block))
(defun api:skip-block (file-reader)
  "Skips the current block from FILE-READER."
  (prog1 (file-block file-reader)
    (set-next-file-block file-reader)))

(declaim
 (ftype (function (api:file-reader)
                  (values (or null (simple-array api:object (*))) &optional))
        api:read-block))
(defun api:read-block (file-reader)
  (let ((file-header (file-header file-reader))
        (file-block (api:skip-block file-reader)))
    (when file-block
      (let ((header-sync (api:raw (api:sync file-header)))
            (schema (api:schema file-header))
            (decompress (codec->decompress (api:codec file-header))))
        (block->objects file-block header-sync schema decompress)))))
