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
(defpackage #:org.apache.avro.internal.file.header
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro))
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:array<uint8>)
  (:import-from #:alexandria
                #:define-constant))
(in-package #:org.apache.avro.internal.file.header)

;;; magic

(declaim ((array<uint8> 4) +magic+))
(define-constant +magic+
    (make-array 4 :element-type 'uint8
                  :initial-contents (nconc
                                     (mapcar #'char-code '(#\O #\b #\j))
                                     (list 1)))
  :test #'equalp)

(defclass api:magic ()
  ()
  (:metaclass api:fixed)
  (:size 4)
  (:name "Magic")
  (:enclosing-namespace "org.apache.avro.file")
  (:default-initargs
   :initial-contents +magic+)
  (:documentation
   "Magic field for object container file headers."))

(defmethod initialize-instance :after
    ((instance api:magic) &key)
  (let ((bytes (api:raw instance)))
    (assert (equalp bytes +magic+) ()
            "Incorrect header magic ~S, expected ~S" bytes +magic+)))

;;; meta

(defclass api:meta ()
  ((schema
    :initarg :schema
    :type api:schema)
   (codec
    :initarg :codec
    :type simple-string))
  (:metaclass api:map)
  (:values api:bytes)
  (:documentation
   "Meta field for object container file headers."))

(defmethod initialize-instance :after
    ((instance api:meta) &key)
  (when (slot-boundp instance 'schema)
    (setf (api:gethash "avro.schema" instance)
          (babel:string-to-octets
           (api:serialize (api:schema instance)) :encoding :utf-8)))
  (when (slot-boundp instance 'codec)
    (setf (api:gethash "avro.codec" instance)
          (babel:string-to-octets (api:codec instance) :encoding :utf-8))))

(declaim
 (ftype (function (api:meta) (values api:schema &optional)) parse-schema))
(defun parse-schema (meta)
  (multiple-value-bind (bytes existsp)
      (api:gethash "avro.schema" meta)
    (assert existsp () "Missing avro.schema in header meta")
    (let ((string (babel:octets-to-string bytes :encoding :utf-8)))
      (nth-value 0 (api:deserialize 'api:schema string)))))

(defmethod api:schema
    ((instance api:meta))
  "Return the schema associated with file-header meta INSTANCE."
  (if (slot-boundp instance 'schema)
      (slot-value instance 'schema)
      (setf (slot-value instance 'schema)
            (parse-schema instance))))

(declaim (ftype (function (api:meta) (values string &optional)) parse-codec))
(defun parse-codec (meta)
  (multiple-value-bind (bytes existsp)
      (api:gethash "avro.codec" meta)
    (if (or (not existsp)
            (zerop (length bytes)))
        "null"
        (babel:octets-to-string bytes :encoding :utf-8))))

(defmethod api:codec
    ((instance api:meta))
  "Return the codec from INSTANCE."
  (if (slot-boundp instance 'codec)
      (slot-value instance 'codec)
      (setf (slot-value instance 'codec)
            (parse-codec instance))))

;;; sync

(defclass api:sync ()
  ()
  (:metaclass api:fixed)
  (:size 16)
  (:name "Sync")
  (:enclosing-namespace "org.apache.avro.file")
  (:default-initargs
   :initial-contents (loop repeat 16 collect (random 256)))
  (:documentation
   "Sync field for object container file headers."))

;;; file-header

(defclass api:file-header ()
  ((|magic|
    :initarg :magic
    :type api:magic
    :reader api:magic
    :documentation "Magic field.")
   (|meta|
    :initarg :meta
    :type api:meta
    :reader api:meta
    :documentation "Meta field.")
   (|sync|
    :initarg :sync
    :type api:sync
    :reader api:sync
    :documentation "Sync field."))
  (:metaclass api:record)
  (:name "org.apache.avro.file.Header")
  (:default-initargs
   :magic (make-instance 'api:magic)
   :sync (make-instance 'api:sync))
  (:documentation
   "File header for object container files."))

(defmethod api:schema
    ((instance api:file-header))
  "Return the schema associated with file-header INSTANCE."
  (api:schema (api:meta instance)))

(defmethod api:codec
    ((instance api:file-header))
  "Return the codec from INSTANCE."
  (api:codec (api:meta instance)))
