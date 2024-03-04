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
(defpackage #:org.apache.avro.internal.int
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
(in-package #:org.apache.avro.internal.int)

(defprimitive api:int (signed-byte 32)
  "Avro int schema.")

(zigzag:implement 32)

(defmethod internal:fixed-size
    ((schema (eql 'api:int)))
  (declare (ignore schema))
  nil)

(defmethod api:deserialize
    ((schema (eql 'api:int)) (input vector) &key (start 0))
  (declare (ignore schema))
  (deserialize-from-vector input start))

(defmethod api:deserialize
    ((schema (eql 'api:int)) (input stream) &key)
  (declare (ignore schema))
  (deserialize-from-stream input))

(defmethod internal:skip
    ((schema (eql 'api:int)) (input vector) &optional (start 0))
  (declare (ignore schema))
  (nth-value 1 (deserialize-from-vector input start)))

(defmethod internal:skip
    ((schema (eql 'api:int)) (input stream) &optional start)
  (declare (ignore schema start))
  (nth-value 1 (deserialize-from-stream input)))

(defmethod api:compare
    ((schema (eql 'api:int)) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (ignore schema))
  (compare-vectors left right left-start right-start))

(defmethod api:compare
    ((schema (eql 'api:int)) (left stream) (right stream) &key)
  (declare (ignore schema))
  (compare-streams left right))

(defmethod api:coerce
    ((object integer) (schema (eql 'api:int)))
  (declare (ignore schema)
           (api:int object))
  object)

(defmethod internal:deserialize-field-default
    ((schema (eql 'api:int)) (default integer))
  (declare (ignore schema)
           (api:int default))
  default)
