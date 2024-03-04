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
(defpackage #:org.apache.avro.internal.null
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal))
  (:import-from #:org.apache.avro.internal.defprimitive
                #:defprimitive)
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>)
  (:export #:+jso+
           #:+json+
           #:+crc-64-avro+))
(in-package #:org.apache.avro.internal.null)

(defprimitive api:null null
  "Avro null schema.")

(defmethod api:schema-of
    ((object null))
  (declare (ignore object))
  'api:null)

(defmethod internal:fixed-size
    ((schema (eql 'api:null)))
  (declare (ignore schema))
  0)

(defmethod api:serialized-size
    ((object null))
  (declare (ignore object))
  0)

(defmethod internal:serialize
    ((object null) (into vector) &key (start 0))
  (declare (ignore object)
           (vector<uint8> into)
           (ufixnum start))
  (assert (<= start (length into)) (start)
          "Start ~S too large for vector of length ~S"
          start (length into))
  0)

(defmethod internal:serialize
    ((object null) (into stream) &key)
  (declare (ignore object into))
  0)

(defmethod api:serialize
    ((object null)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (if sp 10 0) :element-type 'uint8))
       (start 0))
  (declare (ignore object start))
  (values into (apply #'internal:serialize nil into initargs)))

(defmethod api:deserialize
    ((schema (eql 'api:null)) input &key)
  (declare (ignore schema input))
  (values nil 0))

(defmethod internal:skip
    ((schema (eql 'api:null)) input &optional start)
  (declare (ignore schema input start))
  0)

(defmethod api:compare
    ((schema (eql 'api:null)) left right &key)
  (declare (ignore schema left right))
  (values 0 0 0))

(defmethod api:coerce
    ((object null) (schema (eql 'api:null)))
  (declare (ignore object schema))
  nil)

(defmethod internal:serialize-field-default
    ((default null))
  (declare (ignore default))
  :null)

(defmethod internal:deserialize-field-default
    ((schema (eql 'api:null)) (default (eql :null)))
  (declare (ignore schema default))
  nil)
