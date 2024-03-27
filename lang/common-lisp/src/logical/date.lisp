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
(defpackage #:org.apache.avro.internal.date
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal))
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>)
  (:import-from #:org.apache.avro.internal.logical.datetime
                #:unix-epoch
                #:+utc-unix-epoch+)
  (:import-from #:org.apache.avro.internal.recursive-descent.pattern
                #:define-pattern-method))
(in-package #:org.apache.avro.internal.date)

;; TODO this seems like it should be a local date instead of utc

;;; date

(defclass api:date (api:logical-object local-time:timestamp)
  ()
  (:metaclass api:logical-schema)
  (:documentation
   "Avro date schema.

This represents a date on the calendar, with no reference to a
particular timezone or time-of-day."))

(defmethod internal:underlying
    ((schema (eql (find-class 'api:date))))
  (declare (ignore schema))
  'api:int)

(defmethod initialize-instance :after
    ((instance api:date) &key year month day)
  (when (or year month day)
    (local-time:encode-timestamp
     0 0 0 0 day month year :into instance :timezone local-time:+utc-zone+)))

(defmethod api:year
    ((object api:date) &key)
  "Return the year of OBJECT in utc."
  (local-time:timestamp-year object :timezone local-time:+utc-zone+))

(defmethod api:month
    ((object api:date) &key)
  "Return the month of object in utc."
  (local-time:timestamp-month object :timezone local-time:+utc-zone+))

(defmethod api:day
    ((object api:date) &key)
  "Return the day of object in utc."
  (local-time:timestamp-day object :timezone local-time:+utc-zone+))

;;; to/from-underlying

(declaim
 (ftype (function (api:date) (values api:int &optional)) to-underlying))
(defun to-underlying (date)
  "Serialized as the number of days from the ISO unix epoch 1970-01-01."
  (let ((diff
          (local-time-duration:timestamp-difference date +utc-unix-epoch+)))
    (nth-value 0 (local-time-duration:duration-as diff :day))))

(declaim
 (ftype (function (api:int) (values api:date &optional)) from-underlying))
(defun from-underlying (days)
  (let ((timestamp (local-time:adjust-timestamp!
                       (unix-epoch local-time:+utc-zone+)
                     (offset :day days)
                     (timezone local-time:+utc-zone+))))
    (change-class timestamp 'api:date)))

;;; serialized-size

(defmethod internal:fixed-size
    ((schema (eql (find-class 'api:date))))
  (declare (ignore schema))
  nil)

(defmethod api:serialized-size
    ((object api:date))
  (api:serialized-size (to-underlying object)))

;;; serialize

(defmethod internal:serialize
    ((object api:date) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (internal:serialize (to-underlying object) into :start start))

(defmethod internal:serialize
    ((object api:date) (into stream) &key)
  (internal:serialize (to-underlying object) into))

(defmethod api:serialize
    ((object api:date)
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
    ((schema (eql (find-class 'api:date))) (input vector) &key (start 0))
  (declare (ignore schema)
           (vector<uint8> input)
           (ufixnum start))
  (multiple-value-bind (days bytes-read)
      (api:deserialize 'api:int input :start start)
    (values (from-underlying days) bytes-read)))

(defmethod api:deserialize
    ((schema (eql (find-class 'api:date))) (input stream) &key)
  (declare (ignore schema))
  (multiple-value-bind (days bytes-read)
      (api:deserialize 'api:int input)
    (values (from-underlying days) bytes-read)))

;;; compare

(defmethod internal:skip
    ((schema (eql (find-class 'api:date))) (input vector) &optional (start 0))
  (declare (ignore schema))
  (internal:skip 'api:int input start))

(defmethod internal:skip
    ((schema (eql (find-class 'api:date))) (input stream) &optional start)
  (declare (ignore schema start))
  (internal:skip 'api:int input))

(defmethod api:compare
    ((schema (eql (find-class 'api:date))) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (ignore schema))
  (api:compare
   'api:int left right :left-start left-start :right-start right-start))

(defmethod api:compare
    ((schema (eql (find-class 'api:date))) (left stream) (right stream) &key)
  (declare (ignore schema))
  (api:compare 'api:int left right))

;;; coerce

(defmethod api:coerce
    ((object api:date) (schema (eql (find-class 'api:date))))
  (declare (ignore schema))
  object)

(defmethod api:coerce
    ((object api:date) (schema (eql 'api:int)))
  (declare (ignore schema))
  (to-underlying object))

(defmethod api:coerce
    ((object api:date) (schema (eql 'api:long)))
  (declare (ignore schema))
  (to-underlying object))

(defmethod api:coerce
    ((object api:date) (schema (eql 'api:float)))
  (declare (ignore schema))
  (coerce (to-underlying object) 'api:float))

(defmethod api:coerce
    ((object api:date) (schema (eql 'api:double)))
  (declare (ignore schema))
  (coerce (to-underlying object) 'api:double))

(defmethod api:coerce
    ((object integer) (schema (eql (find-class 'api:date))))
  (declare (ignore schema)
           (api:int object))
  (from-underlying object))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:date))
  (to-underlying default))

(defmethod internal:deserialize-field-default
    ((schema (eql (find-class 'api:date))) (default integer))
  (declare (ignore schema)
           (api:int default))
  (from-underlying default))

;;; jso

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" "int"
                    "logicalType" "date"))
              fullname->schema
              enclosing-namespace)
      (declare (ignore jso fullname->schema enclosing-namespace))
      (find-class 'api:date)))
