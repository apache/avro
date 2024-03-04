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
(defpackage #:org.apache.avro/test/resolution/primitive
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/resolution/base
                #:find-schema
                #:bytes))
(in-package #:org.apache.avro/test/resolution/primitive)

(defmacro deftest (schema function expected)
  (declare (symbol schema function))
  (let ((schema (find-schema schema))
        (test-name (intern (format nil "~A->~:*~A" schema)))
        (expected-object (gensym))
        (serialized (gensym))
        (deserialized (gensym)))
    `(test ,test-name
       (let* ((,expected-object ,expected)
              (,serialized (avro:serialize ,expected-object))
              (,deserialized (avro:coerce
                              (avro:deserialize ',schema ,serialized)
                              ',schema)))
         (is (typep ,expected-object ',schema))
         (is (,function ,expected-object ,deserialized))))))

(deftest null eq nil)

(deftest boolean eq 'avro:true)

(deftest int = 3)

(deftest long = 4)

(deftest float = 3.7)

(deftest double = 3.7d0)

(deftest bytes equalp (bytes 2 4 6))

(deftest string string= "foobar")
