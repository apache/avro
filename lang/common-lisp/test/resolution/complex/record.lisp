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
(defpackage #:org.apache.avro/test/resolution/record
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro)))
(in-package #:org.apache.avro/test/resolution/record)

(defclass array<int> ()
  ()
  (:metaclass avro:array)
  (:items avro:int))

(defclass array<float> ()
  ()
  (:metaclass avro:array)
  (:items avro:float))

(defclass writer_schema? ()
  ()
  (:metaclass avro:union)
  (:schemas avro:null writer_schema))

(defclass writer_schema ()
  ((nums
    :type array<int>
    :initarg :nums
    :reader nums)
   (num
    :type avro:int
    :initarg :num
    :reader num)
   (str
    :type avro:string
    :initarg :str
    :reader str)
   (record
    :type writer_schema?
    :initarg :record
    :reader record)
   (extra
    :type avro:boolean
    :initarg :extra
    :reader extra))
  (:metaclass avro:record)
  (:default-initargs
   :record (make-instance 'writer_schema? :object nil)))

(defclass reader_schema? ()
  ()
  (:metaclass avro:union)
  (:schemas avro:null reader_schema))

(defclass reader_schema ()
  ((reader_num
    :type avro:long
    :reader reader_num
    :aliases ("NUM"))
   (reader_nums
    :type array<float>
    :reader reader_nums
    :aliases ("NUMS"))
   (str
    :type avro:string
    :reader str)
   (reader_record
    :type reader_schema?
    :reader reader_record
    :aliases ("RECORD"))
   (exclusive
    :type avro:string
    :reader exclusive
    :default "foo"))
  (:metaclass avro:record)
  (:aliases "WRITER_SCHEMA"))

(defclass reader_schema_no_default ()
  ((reader_nums
    :type array<float>
    :reader reader_nums
    :aliases ("NUMS"))
   (str
    :type avro:string
    :reader str)
   (reader_num
    :type avro:long
    :reader reader_num
    :aliases ("NUM"))
   (reader_record
    :type reader_schema?
    :reader reader_record
    :aliases ("RECORD"))
   (exclusive
    :type avro:string
    :reader exclusive))
  (:metaclass avro:record)
  (:aliases "WRITER_SCHEMA"))

(test record->record
  (let* ((writer
           (make-instance
            'writer_schema
            :nums (make-instance 'array<int> :initial-contents '(2 4 6))
            :num 8
            :str "foo"
            :extra 'avro:true
            :record (make-instance
                     'writer_schema?
                     :object (make-instance
                              'writer_schema
                              :nums (make-instance
                                     'array<int> :initial-contents '(8 10 12))
                              :num 14
                              :str "bar"
                              :extra 'avro:false))))
         (reader
           (avro:coerce
            (avro:deserialize 'writer_schema (avro:serialize writer))
            'reader_schema)))
    (is (typep writer 'writer_schema))
    (is (typep reader 'reader_schema))

    (is (every #'= (nums writer) (reader_nums reader)))
    (is (= (num writer) (reader_num reader)))
    (is (string= (str writer) (str reader)))
    (is (string= "foo" (exclusive reader)))

    (let* ((writer? (record writer))
           (reader? (reader_record reader))
           (writer (avro:object writer?))
           (reader (avro:object reader?)))
      (is (typep writer? 'writer_schema?))
      (is (typep reader? 'reader_schema?))
      (is (typep writer 'writer_schema))
      (is (typep reader 'reader_schema))

      (is (every #'= (nums writer) (reader_nums reader)))
      (is (= (num writer) (reader_num reader)))
      (is (string= (str writer) (str reader)))
      (is (string= "foo" (exclusive reader)))

      (let* ((writer? (record writer))
             (reader? (reader_record reader))
             (writer (avro:object writer?))
             (reader (avro:object reader?)))
        (is (typep writer? 'writer_schema?))
        (is (typep reader? 'reader_schema?))
        (is (typep writer 'avro:null))
        (is (typep reader 'avro:null))))))

(test no-default
  (let ((writer
          (make-instance
           'writer_schema
           :nums (make-instance 'array<int> :initial-contents '(2 4 6))
           :num 8
           :str "foo"
           :extra 'avro:true
           :record (make-instance
                    'writer_schema?
                    :object (make-instance
                             'writer_schema
                             :nums (make-instance
                                    'array<int> :initial-contents '(8 10 12))
                             :num 14
                             :str "bar"
                             :extra 'avro:false)))))
    (signals error
      (avro:coerce
       (avro:deserialize 'writer_schema (avro:serialize writer))
       'reader_schema_no_default))))
