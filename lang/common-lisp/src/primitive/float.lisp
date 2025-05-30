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
(defpackage #:org.apache.avro.internal.float
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:ieee-754 #:org.apache.avro.internal.ieee-754))
  (:import-from #:org.apache.avro.internal.defprimitive
                #:defprimitive)
  (:import-from #:org.apache.avro.internal.type
                #:uint8)
  (:export #:+jso+
           #:+json+
           #:+crc-64-avro+))
(in-package #:org.apache.avro.internal.float)

(macrolet
    ((defloat ()
       (assert (= 24 (float-digits 1f0)))
       `(defprimitive api:float single-float
          "Avro float schema.")))
  (defloat))

(defmethod api:schema-of
    ((object single-float))
  (declare (ignore object))
  'api:float)

(ieee-754:implement 32)

(defmethod internal:fixed-size
    ((schema (eql 'api:float)))
  (declare (ignore schema))
  4)

(defmethod api:serialized-size
    ((object single-float))
  (declare (ignore object))
  4)

(defmethod internal:serialize
    ((object single-float) (into vector) &key (start 0))
  (serialize-into-vector object into start))

(defmethod internal:serialize
    ((object single-float) (into stream) &key)
  (serialize-into-stream object into))

(defmethod api:serialize
    ((object single-float)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (if sp 14 4) :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

(defmethod api:deserialize
    ((schema (eql 'api:float)) (input vector) &key (start 0))
  (declare (ignore schema))
  (deserialize-from-vector input start))

(defmethod api:deserialize
    ((schema (eql 'api:float)) (input stream) &key)
  (declare (ignore schema))
  (deserialize-from-stream input))

(defmethod internal:skip
    ((schema (eql 'api:float)) (input vector) &optional start)
  (declare (ignore schema input start))
  4)

(defmethod internal:skip
    ((schema (eql 'api:float)) (input stream) &optional start)
  (declare (ignore schema start))
  (loop repeat 4 do (read-byte input))
  4)

(defmethod api:compare
    ((schema (eql 'api:float)) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (ignore schema))
  (compare-vectors left right left-start right-start))

(defmethod api:compare
    ((schema (eql 'api:float)) (left stream) (right stream) &key)
  (declare (ignore schema))
  (compare-streams left right))

(defmethod api:coerce
    ((object single-float) (schema (eql 'api:float)))
  (declare (ignore schema))
  object)

(defmethod api:coerce
    ((object integer) (schema (eql 'api:float)))
  (declare (ignore schema)
           (api:long object))
  (coerce object 'api:float))

(defmethod internal:serialize-field-default
    ((default single-float))
  default)

(defmethod internal:deserialize-field-default
    ((schema (eql 'api:float)) (default double-float))
  (declare (ignore schema))
  (coerce default 'api:float))
