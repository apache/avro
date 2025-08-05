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
(defpackage #:org.apache.avro.internal.long
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:zigzag #:org.apache.avro.internal.zigzag))
  (:import-from #:org.apache.avro.internal.defprimitive
                #:defprimitive)
  (:import-from #:org.apache.avro.internal.type
                #:uint8)
  (:export #:+jso+
           #:+json+
           #:size
           #:+crc-64-avro+
           #:serialized-size
           #:serialize-into-vector
           #:serialize-into-stream
           #:deserialize-from-vector
           #:deserialize-from-stream))
(in-package #:org.apache.avro.internal.long)

(defprimitive api:long (signed-byte 64)
  "Avro long schema.")

(defmethod api:schema-of
    ((object integer))
  (etypecase object
    (api:int 'api:int)
    (api:long 'api:long)))

(zigzag:implement 64)

(defmethod internal:fixed-size
    ((schema (eql 'api:long)))
  (declare (ignore schema))
  nil)

(defmethod api:serialized-size
    ((object integer))
  (serialized-size object))

(defmethod internal:serialize
    ((object integer) (into vector) &key (start 0))
  (serialize-into-vector object into start))

(defmethod internal:serialize
    ((object integer) (into stream) &key)
  (serialize-into-stream object into))

(defmethod api:serialize
    ((object integer)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (+ (if sp 10 0) (serialized-size object))
                         :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

(defmethod api:deserialize
    ((schema (eql 'api:long)) (input vector) &key (start 0))
  (declare (ignore schema))
  (deserialize-from-vector input start))

(defmethod api:deserialize
    ((schema (eql 'api:long)) (input stream) &key)
  (declare (ignore schema))
  (deserialize-from-stream input))

(defmethod internal:skip
    ((schema (eql 'api:long)) (input vector) &optional (start 0))
  (declare (ignore schema))
  (nth-value 1 (deserialize-from-vector input start)))

(defmethod internal:skip
    ((schema (eql 'api:long)) (input stream) &optional start)
  (declare (ignore schema start))
  (nth-value 1 (deserialize-from-stream input)))

(defmethod api:compare
    ((schema (eql 'api:long)) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (ignore schema))
  (compare-vectors left right left-start right-start))

(defmethod api:compare
    ((schema (eql 'api:long)) (left stream) (right stream) &key)
  (declare (ignore schema))
  (compare-streams left right))

(defmethod api:coerce
    ((object integer) (schema (eql 'api:long)))
  (declare (ignore schema)
           (api:long object))
  object)

(defmethod internal:serialize-field-default
    ((default integer))
  (declare ((or api:int api:long) default))
  default)

(defmethod internal:deserialize-field-default
    ((schema (eql 'api:long)) (default integer))
  (declare (ignore schema)
           (api:long default))
  default)
