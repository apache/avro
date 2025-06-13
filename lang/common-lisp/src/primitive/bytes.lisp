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
(defpackage #:org.apache.avro.internal.bytes
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:long #:org.apache.avro.internal.long))
  (:import-from #:org.apache.avro.internal.defprimitive
                #:defprimitive)
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>)
  (:import-from #:org.apache.avro.internal.compare
                #:compare-byte-vectors
                #:compare-byte-streams)
  (:export #:+jso+
           #:+json+
           #:+crc-64-avro+
           #:size
           #:+max-size+))
(in-package #:org.apache.avro.internal.bytes)

(macrolet
    ((defbytes ()
       (assert (subtypep (upgraded-array-element-type 'uint8) 'uint8))
       `(defprimitive api:bytes vector<uint8>
          "Avro bytes schema.")))
  (defbytes))

(defconstant +max-size+ (expt 2 40))

(deftype size ()
  "A terabyte of ram ought to be enough and keeps us within a fixnum."
  `(integer 0 ,+max-size+))

(defmethod api:schema-of
    ((object vector))
  (declare (api:bytes object))
  'api:bytes)

(defmethod internal:fixed-size
    ((schema (eql 'api:bytes)))
  (declare (ignore schema))
  nil)

(defmethod api:serialized-size
    ((object vector))
  (declare (api:bytes object))
  (let ((length (length object)))
    (the size (+ (long:serialized-size length) length))))

(defmethod internal:serialize
    ((object vector) (into vector) &key (start 0))
  (declare (api:bytes object)
           (vector<uint8> into)
           (ufixnum start))
  (assert (>= (- (length into) start)
              (api:serialized-size object))
          (into)
          "Not enough room in vector, need ~S more byte:~P"
          (- (- (length into) start)
             (api:serialized-size object)))
  (let* ((length (length object))
         (bytes-written (long:serialize-into-vector length into start)))
    (replace into object :start1 (+ start bytes-written))
    (the size (+ bytes-written length))))

(defmethod internal:serialize
    ((object vector) (into stream) &key)
  (declare (api:bytes object))
  (let* ((length (length object))
         (bytes-written (long:serialize-into-stream length into)))
    (write-sequence object into)
    (the size (+ bytes-written length))))

(defmethod api:serialize
    ((object vector)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (+ (if sp 10 0) (api:serialized-size object))
                         :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

(defmethod api:deserialize
    ((schema (eql 'api:bytes)) (input vector) &key (start 0))
  (declare (ignore schema)
           (vector<uint8> input)
           (ufixnum start))
  (multiple-value-bind (length bytes-read)
      (long:deserialize-from-vector input start)
    (let ((object (make-array length
                              :element-type 'uint8
                              :displaced-to input
                              :displaced-index-offset (+ start bytes-read))))
      (the (values api:bytes size &optional)
           (values object (+ bytes-read length))))))

(defmethod api:deserialize
    ((schema (eql 'api:bytes)) (input stream) &key)
  (declare (ignore schema))
  (multiple-value-bind (length bytes-read)
      (long:deserialize-from-stream input)
    (let ((object (make-array length :element-type 'uint8)))
      (unless (= (read-sequence object input) length)
        (error 'end-of-file :stream *error-output*))
      (the (values api:bytes size &optional)
           (values object (+ bytes-read length))))))

(defmethod internal:skip
    ((schema (eql 'api:bytes)) (input vector) &optional (start 0))
  (declare (vector<uint8> input)
           (ufixnum start)
           (ignore schema))
  (multiple-value-bind (length bytes-read)
      (long:deserialize-from-vector input start)
    (the size (+ length bytes-read))))

(defmethod internal:skip
    ((schema (eql 'api:bytes)) (input stream) &optional start)
  (declare (ignore schema start))
  (multiple-value-bind (length bytes-read)
      (long:deserialize-from-stream input)
    (loop repeat length do (read-byte input))
    (the size (+ length bytes-read))))

(defmethod api:compare
    ((schema (eql 'api:bytes)) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (ignore schema)
           (vector<uint8> left right)
           (ufixnum left-start right-start))
  (multiple-value-bind (left-length left-bytes-read)
      (long:deserialize-from-vector left left-start)
    (declare (ufixnum left-length left-bytes-read))
    (multiple-value-bind (right-length right-bytes-read)
        (long:deserialize-from-vector right right-start)
      (declare (ufixnum right-length right-bytes-read))
      (let* ((left-start (+ left-start left-bytes-read))
             (right-start (+ right-start right-bytes-read))
             (left-end (+ left-start left-length))
             (right-end (+ right-start right-length)))
        (declare (ufixnum left-start right-start left-end right-end))
        (compare-byte-vectors
         left right left-start right-start left-end right-end)))))

(defmethod api:compare
    ((schema (eql 'api:bytes)) (left stream) (right stream) &key)
  (declare (ignore schema))
  (let ((left-length (long:deserialize-from-stream left))
        (right-length (long:deserialize-from-stream right)))
    (compare-byte-streams left right left-length right-length)))

(defmethod api:coerce
    ((object vector) (schema (eql 'api:bytes)))
  (declare (ignore schema)
           (api:bytes object))
  object)

(defmethod api:coerce
    ((object string) (schema (eql 'api:bytes)))
  (declare (ignore schema))
  (babel:string-to-octets object :encoding :utf-8))

(defmethod internal:serialize-field-default
    ((default vector))
  (declare (api:bytes default))
  (babel:octets-to-string default :encoding :latin-1))

(defmethod internal:deserialize-field-default
    ((schema (eql 'api:bytes)) (default string))
  (babel:string-to-octets default :encoding :latin-1))
