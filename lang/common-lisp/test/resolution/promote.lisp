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
(defpackage #:org.apache.avro/test/resolution/promote
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/resolution/base
                #:find-schema
                #:bytes))
(in-package #:org.apache.avro/test/resolution/promote)

(defmacro deftest (from to input function &optional postprocess)
  (declare (symbol from to function postprocess))
  (let* ((from (find-schema from))
         (to (find-schema to))
         (test-name (intern (format nil "~A->~A" from to)))
         (input-object (gensym))
         (serialized (gensym))
         (deserialized (gensym))
         (postprocessed (if postprocess
                            `(,postprocess ,input-object)
                            input-object)))
    `(test ,test-name
       (let* ((,input-object ,input)
              (,serialized (avro:serialize ,input-object))
              (,deserialized (avro:coerce
                              (avro:deserialize ',from ,serialized)
                              ',to)))
         (is (typep ,input-object ',from))
         (is (typep ,deserialized ',to))
         (is (,function ,postprocessed ,deserialized))))))

(deftest int long 3 =)

(deftest int float 4 =)

(deftest int double 5 =)

(deftest long float 6 =)

(deftest long double 7 =)

(deftest float double 8.7 =)

(deftest string bytes "foobar" equalp babel:string-to-octets)

(deftest bytes string (bytes #x61 #x62 #x63) string= babel:octets-to-string)
