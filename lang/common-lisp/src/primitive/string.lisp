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
(defpackage #:org.apache.avro.internal.string
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:long #:org.apache.avro.internal.long)
   (#:bytes #:org.apache.avro.internal.bytes))
  (:import-from #:org.apache.avro.internal.defprimitive
                #:defprimitive)
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>)
  (:export #:+jso+
           #:+json+
           #:+crc-64-avro+))
(in-package #:org.apache.avro.internal.string)

(defprimitive api:string string
  "Avro string schema.")

(defmethod api:schema-of
    ((object string))
  (declare (ignore object))
  'api:string)

(defmethod internal:fixed-size
    ((schema (eql 'api:string)))
  (declare (ignore schema))
  nil)

(deftype size ()
  'bytes:size)

(defmethod api:serialized-size
    ((object string))
  (let ((length (babel:string-size-in-octets object :encoding :utf-8)))
    (the size (+ (long:serialized-size length) length))))

(defmethod internal:serialize
    ((object string) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (let ((bytes (babel:string-to-octets object :encoding :utf-8)))
    (internal:serialize bytes into :start start)))

(defmethod internal:serialize
    ((object string) (into stream) &key)
  (let ((bytes (babel:string-to-octets object :encoding :utf-8)))
    (internal:serialize bytes into)))

(defmethod api:serialize
    ((object string)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (+ (if sp 10 0) (api:serialized-size object))
                         :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

(defmethod api:deserialize
    ((schema (eql 'api:string)) (input vector) &key (start 0))
  (declare (ignore schema)
           (vector<uint8> input)
           (ufixnum start))
  (multiple-value-bind (length bytes-read)
      (long:deserialize-from-vector input start)
    (let ((object (babel:octets-to-string
                   input
                   :encoding :utf-8
                   :start (+ start bytes-read)
                   :end (+ start bytes-read length))))
      (declare (api:string object))
      (values object (the size (+ bytes-read length))))))

(defmethod api:deserialize
    ((schema (eql 'api:string)) (input stream) &key)
  (declare (ignore schema))
  (multiple-value-bind (bytes bytes-read)
      (api:deserialize 'api:bytes input)
    (let ((object (babel:octets-to-string bytes :encoding :utf-8)))
      (the (values api:string size &optional)
           (values object bytes-read)))))

(defmethod internal:skip
    ((schema (eql 'api:string)) (input vector) &optional (start 0))
  (declare (ignore schema))
  (internal:skip 'api:bytes input start))

(defmethod internal:skip
    ((schema (eql 'api:string)) (input stream) &optional start)
  (declare (ignore schema start))
  (internal:skip 'api:bytes input))

(defmethod api:compare
    ((schema (eql 'api:string)) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (ignore schema))
  (api:compare
   'api:bytes left right :left-start left-start :right-start right-start))

(defmethod api:compare
    ((schema (eql 'api:string)) (left stream) (right stream) &key)
  (declare (ignore schema))
  (api:compare 'api:bytes left right))

(defmethod api:coerce
    ((object string) (schema (eql 'api:string)))
  (declare (ignore schema))
  object)

(defmethod api:coerce
    ((object vector) (schema (eql 'api:string)))
  (declare (ignore schema)
           (api:bytes object))
  (babel:octets-to-string object :encoding :utf-8))

(defmethod internal:serialize-field-default
    ((default string))
  default)

(defmethod internal:deserialize-field-default
    ((schema (eql 'api:string)) (default string))
  (declare (ignore schema))
  default)
