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
(defpackage #:org.apache.avro.internal.local-timestamp-millis
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:long #:org.apache.avro.internal.long))
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>)
  (:import-from #:org.apache.avro.internal.logical.datetime
                #:unix-epoch
                #:local-timezone
                #:local-date
                #:local-hour-minute
                #:local-second-millis
                #:adjust-for-daylight-savings
                #:local-unix-epoch-diff)
  (:import-from #:org.apache.avro.internal.recursive-descent.pattern
                #:define-pattern-method))
(in-package #:org.apache.avro.internal.local-timestamp-millis)

;;; local-timestamp-millis

(defclass api:local-timestamp-millis
    (api:logical-object local-date local-hour-minute local-second-millis)
  ()
  (:metaclass api:logical-schema)
  (:documentation
   "Avro local-timestamp-millis schema.

This represents a millisecond-precision timestamp in a local timezone,
regardless of what specific timezone is considered local."))

(defmethod internal:underlying
    ((schema (eql (find-class 'api:local-timestamp-millis))))
  (declare (ignore schema))
  'api:long)

(defmethod initialize-instance :after
    ((instance api:local-timestamp-millis)
     &key year month day hour minute millisecond)
  (when (or year month hour minute millisecond)
    (multiple-value-bind (second remainder)
        (truncate millisecond 1000)
      (local-time:encode-timestamp
       (* remainder 1000 1000) second minute hour day month year
       :into instance :timezone (local-timezone instance)))))

;;; to/from-underlying

(deftype second-diff ()
  (let ((min (truncate (- (expt 2 63)) 1000))
        (max (truncate (1- (expt 2 63)) 1000)))
    `(integer ,min ,max)))

(declaim
 (ftype (function (api:local-timestamp-millis) (values api:long &optional))
        to-underlying))
(defun to-underlying (local-timestamp-millis)
  "Serialized as the number of milliseconds from 1970-01-01T00:00:00.000."
  (let ((diff (local-unix-epoch-diff local-timestamp-millis)))
    (multiple-value-bind (second-diff diff)
        (local-time-duration:duration-as diff :sec)
      (declare (second-diff second-diff))
      (let ((nanosecond-diff (local-time-duration:duration-as diff :nsec)))
        (declare ((mod 1000000000) nanosecond-diff))
        (+ (* second-diff 1000)
           (truncate nanosecond-diff (* 1000 1000)))))))

(declaim
 (ftype (function (api:long) (values api:local-timestamp-millis &optional))
        from-underlying))
(defun from-underlying (milliseconds-from-local-unix-epoch)
  (multiple-value-bind
        (seconds-from-local-unix-epoch milliseconds-from-local-unix-epoch)
      (truncate milliseconds-from-local-unix-epoch 1000)
    (let ((timestamp
            (local-time:adjust-timestamp!
                (unix-epoch local-time:*default-timezone*)
              (offset :sec seconds-from-local-unix-epoch)
              (offset :nsec (* milliseconds-from-local-unix-epoch 1000 1000)))))
      (adjust-for-daylight-savings timestamp)
      (change-class timestamp 'api:local-timestamp-millis))))

;;; serialized-size

(defmethod internal:fixed-size
    ((schema (eql (find-class 'api:local-timestamp-millis))))
  (declare (ignore schema))
  nil)

(defmethod api:serialized-size
    ((object api:local-timestamp-millis))
  (long:serialized-size (to-underlying object)))

;;; serialize

(defmethod internal:serialize
    ((object api:local-timestamp-millis) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (long:serialize-into-vector (to-underlying object) into start))

(defmethod internal:serialize
    ((object api:local-timestamp-millis) (into stream) &key)
  (long:serialize-into-stream (to-underlying object) into))

(defmethod api:serialize
    ((object api:local-timestamp-millis)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (+ (if sp 10 0) (api:serialized-size object))
                         :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

;;; deserialize

(defmethod api:deserialize
    ((schema (eql (find-class 'api:local-timestamp-millis)))
     (input vector) &key (start 0))
  (declare (ignore schema)
           (vector<uint8> input)
           (ufixnum start))
  (multiple-value-bind (milliseconds-from-local-unix-epoch bytes-read)
      (api:deserialize 'api:long input :start start)
    (values (from-underlying milliseconds-from-local-unix-epoch) bytes-read)))

(defmethod api:deserialize
    ((schema (eql (find-class 'api:local-timestamp-millis)))
     (input stream) &key)
  (declare (ignore schema))
  (multiple-value-bind (milliseconds-from-local-unix-epoch bytes-read)
      (api:deserialize 'api:long input)
    (values (from-underlying milliseconds-from-local-unix-epoch) bytes-read)))

;;; compare

(defmethod internal:skip
    ((schema (eql (find-class 'api:local-timestamp-millis))) (input vector)
     &optional (start 0))
  (declare (ignore schema))
  (internal:skip 'api:long input start))

(defmethod internal:skip
    ((schema (eql (find-class 'api:local-timestamp-millis))) (input stream)
     &optional start)
  (declare (ignore schema start))
  (internal:skip 'api:long input))

(defmethod api:compare
    ((schema (eql (find-class 'api:local-timestamp-millis)))
     (left vector) (right vector) &key (left-start 0) (right-start 0))
  (declare (ignore schema))
  (api:compare
   'api:long left right :left-start left-start :right-start right-start))

(defmethod api:compare
    ((schema (eql (find-class 'api:local-timestamp-millis)))
     (left stream) (right stream) &key)
  (declare (ignore schema))
  (api:compare 'api:long left right))

;;; coerce

(defmethod api:coerce
    ((object api:local-timestamp-millis)
     (schema (eql (find-class 'api:local-timestamp-millis))))
  (declare (ignore schema))
  object)

(defmethod api:coerce
    ((object api:local-timestamp-millis) (schema (eql 'api:long)))
  (declare (ignore schema))
  (to-underlying object))

(defmethod api:coerce
    ((object api:local-timestamp-millis) (schema (eql 'api:float)))
  (declare (ignore schema))
  (coerce (to-underlying object) 'api:float))

(defmethod api:coerce
    ((object api:local-timestamp-millis) (schema (eql 'api:double)))
  (declare (ignore schema))
  (coerce (to-underlying object) 'api:double))

(defmethod api:coerce
    ((object integer) (schema (eql (find-class 'api:local-timestamp-millis))))
  (declare (ignore schema)
           (api:long object))
  (from-underlying object))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:local-timestamp-millis))
  (to-underlying default))

(defmethod internal:deserialize-field-default
    ((schema (eql (find-class 'api:local-timestamp-millis))) (default integer))
  (declare (ignore schema)
           (api:long default))
  (from-underlying default))

;;; jso

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" "long"
                    "logicalType" "local-timestamp-millis"))
              fullname->schema
              enclosing-namespace)
      (declare (ignore jso fullname->schema enclosing-namespace))
      (find-class 'api:local-timestamp-millis)))
