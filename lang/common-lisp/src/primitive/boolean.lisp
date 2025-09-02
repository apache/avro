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
(defpackage #:org.apache.avro.internal.boolean
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
(in-package #:org.apache.avro.internal.boolean)

(defprimitive api:boolean (member api:true api:false)
  "Avro boolean schema.")

(declaim ((eql api:true) api:true))
(defconstant api:true 'api:true
  "Avro true value.")

(declaim ((eql api:false) api:false))
(defconstant api:false 'api:false
  "Avro false value.")

(defmethod api:schema-of
    ((object (eql 'api:true)))
  (declare (ignore object))
  'api:boolean)

(defmethod api:schema-of
    ((object (eql 'api:false)))
  (declare (ignore object))
  'api:boolean)

(defmethod internal:fixed-size
    ((schema (eql 'api:boolean)))
  (declare (ignore schema))
  1)

(defmethod api:serialized-size
    ((object (eql 'api:true)))
  (declare (ignore object))
  1)

(defmethod api:serialized-size
    ((object (eql 'api:false)))
  (declare (ignore object))
  1)

(defmethod internal:serialize
    ((object (eql 'api:true)) (into vector) &key (start 0))
  (declare (ignore object)
           (vector<uint8> into)
           (ufixnum start))
  (setf (elt into start) 1)
  1)

(defmethod internal:serialize
    ((object (eql 'api:false)) (into vector) &key (start 0))
  (declare (ignore object)
           (vector<uint8> into)
           (ufixnum start))
  (setf (elt into start) 0)
  1)

(defmethod internal:serialize
    ((object (eql 'api:true)) (into stream) &key)
  (declare (ignore object))
  (write-byte 1 into)
  1)

(defmethod internal:serialize
    ((object (eql 'api:false)) (into stream) &key)
  (declare (ignore object))
  (write-byte 0 into)
  1)

(defmethod api:serialize
    ((object (eql 'api:true))
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (if sp 11 1) :element-type 'uint8))
       (start 0))
  (declare (ignore object start))
  (values into (apply #'internal:serialize 'api:true into initargs)))

(defmethod api:serialize
    ((object (eql 'api:false))
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (if sp 11 1) :element-type 'uint8))
       (start 0))
  (declare (ignore object start))
  (values into (apply #'internal:serialize 'api:false into initargs)))

(defmethod api:deserialize
    ((schema (eql 'api:boolean)) (input vector) &key (start 0))
  (declare (ignore schema)
           (vector<uint8> input)
           (ufixnum start))
  (let ((object (ecase (elt input start)
                  (0 'api:false)
                  (1 'api:true))))
    (values object 1)))

(defmethod api:deserialize
    ((schema (eql 'api:boolean)) (input stream) &key)
  (declare (ignore schema))
  (let ((object (ecase (read-byte input)
                  (0 'api:false)
                  (1 'api:true))))
    (values object 1)))

(defmethod internal:skip
    ((schema (eql 'api:boolean)) (input vector) &optional start)
  (declare (ignore schema input start))
  1)

(defmethod internal:skip
    ((schema (eql 'api:boolean)) (input stream) &optional start)
  (declare (ignore schema start))
  (read-byte input)
  1)

(defmethod api:compare
    ((schema (eql 'api:boolean)) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (ignore schema)
           (vector<uint8> left right)
           (ufixnum left-start right-start))
  (let ((left (elt left left-start))
        (right (elt right right-start)))
    (declare (bit left right))
    (values (- left right) 1 1)))

(defmethod api:compare
    ((schema (eql 'api:boolean)) (left stream) (right stream) &key)
  (declare (ignore schema))
  (let ((left (read-byte left))
        (right (read-byte right)))
    (declare (bit left right))
    (values (- left right) 1 1)))

(defmethod api:coerce
    ((object (eql 'api:true)) (schema (eql 'api:boolean)))
  (declare (ignore object schema))
  'api:true)

(defmethod api:coerce
    ((object (eql 'api:false)) (schema (eql 'api:boolean)))
  (declare (ignore object schema))
  'api:false)

(defmethod internal:serialize-field-default
    ((default (eql 'api:true)))
  (declare (ignore default))
  (st-json:as-json-bool t))

(defmethod internal:serialize-field-default
    ((default (eql 'api:false)))
  (declare (ignore default))
  (st-json:as-json-bool nil))

(defmethod internal:deserialize-field-default
    ((schema (eql 'api:boolean)) (default (eql (st-json:as-json-bool t))))
  (declare (ignore schema default))
  'api:true)

(defmethod internal:deserialize-field-default
    ((schema (eql 'api:boolean)) (default (eql (st-json:as-json-bool nil))))
  (declare (ignore schema default))
  'api:false)
