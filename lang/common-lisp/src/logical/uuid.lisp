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
(defpackage #:org.apache.avro.internal.uuid
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)
   (#:ascii #:org.apache.avro.internal.ascii))
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:ufixnum
                #:vector<uint8>)
  (:import-from #:org.apache.avro.internal.recursive-descent.pattern
                #:define-pattern-method))
(in-package #:org.apache.avro.internal.uuid)

;;; uuid

(deftype 36-char-string ()
  '(simple-string 36))

(declaim
 (ftype (function (36-char-string) (values boolean &optional))
        %rfc-4122-uuid-p))
(defun %rfc-4122-uuid-p (uuid)
  "True if UUID conforms to RFC-4122."
  (and (char= #\-
              (char uuid 8)
              (char uuid 13)
              (char uuid 18)
              (char uuid 23))
       (loop for i from 00 below 08 always (ascii:hex-p (char uuid i)))
       (loop for i from 09 below 13 always (ascii:hex-p (char uuid i)))
       (loop for i from 14 below 18 always (ascii:hex-p (char uuid i)))
       (loop for i from 19 below 23 always (ascii:hex-p (char uuid i)))
       (loop for i from 24 below 36 always (ascii:hex-p (char uuid i)))))

(declaim (ftype (function (t) (values boolean &optional)) rfc-4122-uuid-p))
(defun rfc-4122-uuid-p (uuid)
  (when (typep uuid '36-char-string)
    (%rfc-4122-uuid-p uuid)))

(deftype rfc-4122-uuid ()
  '(and 36-char-string (satisfies rfc-4122-uuid-p)))

(defclass api:uuid (api:logical-object)
  ((uuid
    :initarg :uuid
    :reader api:raw
    :type rfc-4122-uuid
    :documentation "UUID string conforming to RFC-4122."))
  (:metaclass api:logical-schema)
  (:default-initargs
   :uuid (error "Must supply UUID"))
  (:documentation
   "Avro uuid schema."))

(defmethod internal:underlying
    ((schema (eql (find-class 'api:uuid))))
  (declare (ignore schema))
  'api:string)

;;; serialized-size

(defmethod internal:fixed-size
    ((schema (eql (find-class 'api:uuid))))
  (declare (ignore schema))
  37)

(defmethod api:serialized-size
    ((object api:uuid))
  (declare (ignore object))
  37)

;;; serialize

(defmethod internal:serialize
    ((object api:uuid) (into vector) &key (start 0))
  (declare (vector<uint8> into)
           (ufixnum start))
  (let ((uuid (api:raw object)))
    (declare (rfc-4122-uuid uuid))
    (internal:serialize uuid into :start start)))

(defmethod internal:serialize
    ((object api:uuid) (into stream) &key)
  (let ((uuid (api:raw object)))
    (declare (rfc-4122-uuid uuid))
    (internal:serialize uuid into)))

(defmethod api:serialize
    ((object api:uuid)
     &rest initargs
     &key
       ((:single-object-encoding-p sp))
       (into (make-array (if sp 47 37) :element-type 'uint8))
       (start 0))
  (declare (ignore start))
  (values into (apply #'internal:serialize object into initargs)))

;;; deserialize

(defmethod api:deserialize
    ((schema (eql (find-class 'api:uuid))) (input vector) &key (start 0))
  (declare (ignore schema)
           (vector<uint8> input)
           (ufixnum start))
  (multiple-value-bind (uuid bytes-read)
      (api:deserialize 'api:string input :start start)
    (values (make-instance 'api:uuid :uuid uuid) bytes-read)))

(defmethod api:deserialize
    ((schema (eql (find-class 'api:uuid))) (input stream) &key)
  (declare (ignore schema))
  (multiple-value-bind (uuid bytes-read)
      (api:deserialize 'api:string input)
    (values (make-instance 'api:uuid :uuid uuid) bytes-read)))

;;; compare

(defmethod internal:skip
    ((schema (eql (find-class 'api:uuid))) (input vector) &optional start)
  (declare (ignore schema input start))
  37)

(defmethod internal:skip
    ((schema (eql (find-class 'api:uuid))) (input stream) &optional start)
  (declare (ignore schema start))
  (loop repeat 37 do (read-byte input))
  37)

(defmethod api:compare
    ((schema (eql (find-class 'api:uuid))) (left vector) (right vector)
     &key (left-start 0) (right-start 0))
  (declare (ignore schema))
  (api:compare
   'api:string left right :left-start left-start :right-start right-start))

(defmethod api:compare
    ((schema (eql (find-class 'api:uuid))) (left stream) (right stream) &key)
  (declare (ignore schema))
  (api:compare 'api:string left right))

;;; coerce

(defmethod api:coerce
    ((object api:uuid) (schema (eql (find-class 'api:uuid))))
  (declare (ignore schema))
  object)

(defmethod api:coerce
    ((object api:uuid) (schema (eql 'api:string)))
  (declare (ignore schema))
  (api:raw object))

(defmethod api:coerce
    ((object api:uuid) (schema (eql 'api:bytes)))
  (declare (ignore schema))
  (api:coerce (api:raw object) 'api:bytes))

(defmethod api:coerce
    ((object string) (schema (eql (find-class 'api:uuid))))
  (declare (ignore schema))
  (make-instance 'api:uuid :uuid (coerce object 'simple-string)))

(defmethod api:coerce
    ((object vector) (schema (eql (find-class 'api:uuid))))
  (declare (ignore schema))
  (api:coerce (api:coerce object 'api:string) (find-class 'api:uuid)))

;;; field default

(defmethod internal:serialize-field-default
    ((default api:uuid))
  (api:raw default))

(defmethod internal:deserialize-field-default
    ((schema (eql (find-class 'api:uuid))) (default string))
  (declare (ignore schema))
  (api:coerce default (find-class 'api:uuid)))

;;; jso

(define-pattern-method 'internal:read-jso
    '(lambda ((jso ("type" "string"
                    "logicalType" "uuid"))
              fullname->schema
              enclosing-namespace)
      (declare (ignore jso fullname->schema enclosing-namespace))
      (find-class 'api:uuid)))
