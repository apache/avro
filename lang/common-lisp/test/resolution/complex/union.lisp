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
(defpackage #:org.apache.avro/test/resolution/union
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro)))
(in-package #:org.apache.avro/test/resolution/union)

(defclass array<int> ()
  ()
  (:metaclass avro:array)
  (:items avro:int))

(defclass array<double> ()
  ()
  (:metaclass avro:array)
  (:items avro:double))

(defclass writer-schema ()
  ()
  (:metaclass avro:union)
  (:schemas avro:null avro:string avro:int array<int>))

(defclass reader-schema ()
  ()
  (:metaclass avro:union)
  (:schemas avro:string avro:float avro:long array<double>))

(test both-unions
  (let ((null (make-instance 'writer-schema :object nil))
        (string (make-instance 'writer-schema :object "foobar"))
        (int (make-instance 'writer-schema :object 2))
        (array (make-instance
                'writer-schema
                :object (make-instance
                         'array<int> :initial-contents '(2 4 6)))))
    (is (typep null 'writer-schema))
    (signals error
      (avro:coerce
       (avro:deserialize 'writer-schema (avro:serialize null))
       'reader-schema))

    (let ((reader (avro:coerce
                   (avro:deserialize 'writer-schema (avro:serialize string))
                   'reader-schema)))
      (is (typep string 'writer-schema))
      (is (typep reader 'reader-schema))
      (is (= 0 (nth-value 1 (avro:which-one reader))))
      (is (string= (avro:object string) (avro:object reader))))

    (let ((reader (avro:coerce
                   (avro:deserialize 'writer-schema (avro:serialize int))
                   'reader-schema)))
      (is (typep int 'writer-schema))
      (is (typep reader 'reader-schema))
      (is (= 1 (nth-value 1 (avro:which-one reader))))
      (is (= (avro:object int) (avro:object reader))))

    (let ((reader (avro:coerce
                   (avro:deserialize 'writer-schema (avro:serialize array))
                   'reader-schema)))
      (is (typep array 'writer-schema))
      (is (typep reader 'reader-schema))
      (is (= 3 (nth-value 1 (avro:which-one reader))))
      (is (every #'= (avro:object array) (avro:object reader))))))

(test reader-union
  (let ((reader (avro:coerce
                 (avro:deserialize 'avro:int (avro:serialize 3))
                 'reader-schema)))
    (is (typep reader 'reader-schema))
    (is (= 1 (nth-value 1 (avro:which-one reader))))
    (is (= 3 (avro:object reader))))

  (signals error
    (avro:coerce
     (avro:deserialize 'avro:null (avro:serialize nil))
     'reader-schema)))

(test writer-union
  (let ((reader (avro:coerce
                 (avro:deserialize
                  'writer-schema
                  (avro:serialize (make-instance 'writer-schema :object 3)))
                 'avro:long)))
    (is (typep reader 'avro:long))
    (is (= 3 reader)))

  (signals error
    (avro:coerce
     (avro:deserialize
      'writer-schema
      (avro:serialize (make-instance 'writer-schema :object 3)))
     'avro:string)))
