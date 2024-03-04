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
(defpackage #:org.apache.avro.internal.file.block
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro))
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>)
  (:import-from #:org.apache.avro.internal.recursive-descent.pattern
                #:pattern-generic-function)
  (:import-from #:org.apache.avro.internal.recursive-descent.pattern
                #:define-pattern-method)
  (:import-from #:org.apache.avro.internal.array
                #:%serialized-size)
  (:export #:codec->decompress
           #:codec->compress
           #:block->objects
           #:objects->block))
(in-package #:org.apache.avro.internal.file.block)

;;; file-block

(defclass api:file-block ()
  ((count
    :initarg :count
    :type api:long
    :reader api:count
    :documentation "The number of objects in this block.")
   (bytes
    :initarg :bytes
    :type api:bytes
    :reader api:bytes
    :documentation "The serialized and compressed data for this block.")
   (sync
    :initarg :sync
    :type api:sync
    :reader api:sync
    :documentation "Sync field."))
  (:metaclass api:record)
  (:name "org.apache.avro.file.Block")
  (:documentation
   "File block for object container files."))

;;; decompress

(deftype decompress ()
  '(function (vector<uint8>) (values vector<uint8> &optional)))

(declaim (decompress api:*decompress-deflate*))
(defvar api:*decompress-deflate*
  (lambda (bytes)
    (chipz:decompress nil 'chipz:deflate bytes))
  "The function used to decompress deflate-compressed data.")

(declaim (decompress api:*decompress-bzip2*))
(defvar api:*decompress-bzip2*)
(setf (documentation 'api:*decompress-bzip2* 'variable)
      "The function used to decompress bzip2-compressed data.

A default implementation is not provided.")

(declaim (decompress api:*decompress-snappy*))
(defvar api:*decompress-snappy*)
(setf (documentation 'api:*decompress-snappy* 'variable)
      "The function used to decompress snappy-compressed data.

A default implementation is not provided.")

(declaim (decompress api:*decompress-xz*))
(defvar api:*decompress-xz*)
(setf (documentation 'api:*decompress-xz* 'variable)
      "The function used to decompress xz-compressed data.

A default implementation is not provided.")

(declaim (decompress api:*decompress-zstandard*))
(defvar api:*decompress-zstandard*)
(setf (documentation 'api:*decompress-zstandard* 'variable)
      "The function used to decompress zstandard-compressed data.

A default implementation is not provided.")

(defgeneric codec->decompress (codec)
  (:generic-function-class pattern-generic-function))

(defmethod codec->decompress
    ((codec string))
  (error "Unknown codec: ~S" codec))

(define-pattern-method 'codec->decompress
    '(lambda ((codec "null"))
      #'identity))

(define-pattern-method 'codec->decompress
    '(lambda ((codec "deflate"))
      api:*decompress-deflate*))

(define-pattern-method 'codec->decompress
    '(lambda ((codec "bzip2"))
      api:*decompress-bzip2*))

(define-pattern-method 'codec->decompress
    '(lambda ((codec "snappy"))
      api:*decompress-snappy*))

(define-pattern-method 'codec->decompress
    '(lambda ((codec "xz"))
      api:*decompress-xz*))

(define-pattern-method 'codec->decompress
    '(lambda ((codec "zstandard"))
      api:*decompress-zstandard*))

;;; compress

(deftype compress ()
  '(function (vector<uint8>) (values vector<uint8> &optional)))

(declaim (compress api:*compress-deflate*))
(defvar api:*compress-deflate*
  (lambda (bytes)
    (salza2:compress-data bytes 'salza2:deflate-compressor))
  "The function used to perform deflate compression.")

(declaim (compress api:*compress-bzip2*))
(defvar api:*compress-bzip2*)
(setf (documentation 'api:*compress-bzip2* 'variable)
      "The function used to perform bzip2 compression.

A default implementation is not provided.")

(declaim (compress api:*compress-snappy*))
(defvar api:*compress-snappy*)
(setf (documentation 'api:*compress-snappy* 'variable)
      "The function used to perform snappy compression.

A default implementation is not provided.")

(declaim (compress api:*compress-xz*))
(defvar api:*compress-xz*)
(setf (documentation 'api:*compress-xz* 'variable)
      "The function used to perform xz compression.

A default implementation is not provided.")

(declaim (compress api:*compress-zstandard*))
(defvar api:*compress-zstandard*)
(setf (documentation 'api:*compress-zstandard* 'variable)
      "The function used to perform zstandard compression.

A default implementation is not provided.")

(defgeneric codec->compress (codec)
  (:generic-function-class pattern-generic-function))

(defmethod codec->compress
    ((codec string))
  (error "Unknown codec: ~S" codec))

(define-pattern-method 'codec->compress
    '(lambda ((codec "null"))
      #'identity))

(define-pattern-method 'codec->compress
    '(lambda ((codec "deflate"))
      api:*compress-deflate*))

(define-pattern-method 'codec->compress
    '(lambda ((codec "bzip2"))
      api:*compress-bzip2*))

(define-pattern-method 'codec->compress
    '(lambda ((codec "snappy"))
      api:*compress-snappy*))

(define-pattern-method 'codec->compress
    '(lambda ((codec "xz"))
      api:*compress-xz*))

(define-pattern-method 'codec->compress
    '(lambda ((codec "zstandard"))
      api:*compress-zstandard*))

;;; block->objects

(declaim
 (ftype (function ((vector<uint8> 16) api:file-block) (values &optional))
        assert-valid-sync-marker))
(defun assert-valid-sync-marker (header-sync file-block)
  (let ((block-sync (api:raw (api:sync file-block))))
    (declare ((vector<uint8> 16) block-sync))
    (assert (equalp header-sync block-sync) ()
            "File block sync marker does not match header's"))
  (values))

(declaim
 (ftype (function (api:file-block (vector<uint8> 16) api:schema decompress)
                  (values (simple-array api:object (*)) &optional))
        block->objects))
(defun block->objects (file-block header-sync schema decompress)
  (assert-valid-sync-marker header-sync file-block)
  (loop
    with count of-type api:long = (api:count file-block)
    and bytes of-type vector<uint8> = (funcall decompress (api:bytes file-block))
    and total-bytes-read of-type ufixnum = 0
    with objects = (make-array count :element-type schema)

    for index below count
    for object of-type api:object
      = (multiple-value-bind (object bytes-read)
            (api:deserialize schema bytes :start total-bytes-read)
          (incf total-bytes-read bytes-read)
          object)

    do (setf (elt objects index) object)

    finally
       (return
         objects)))

;;; objects->block

(declaim
 (ftype (function ((vector api:object) api:sync api:schema compress)
                  (values api:file-block &optional))
        objects->block))
(defun objects->block (objects header-sync schema compress)
  (loop
    with bytes = (make-array (%serialized-size objects schema)
                             :element-type 'uint8)
    and bytes-written of-type ufixnum = 0

    for object across objects
    do (incf bytes-written
             (nth-value 1 (api:serialize
                           object :into bytes :start bytes-written)))

    finally
       (return
         (make-instance
          'api:file-block
          :count (length objects)
          :bytes (funcall compress bytes)
          :sync header-sync))))
