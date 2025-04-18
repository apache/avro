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
(defpackage #:org.apache.avro/test/resolution/enum
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro)))
(in-package #:org.apache.avro/test/resolution/enum)

(test no-reader-default
  (let* ((writer-enum (make-instance
                       'avro:enum
                       :name "foo"
                       :aliases '("baz" "foo.bar")
                       :default "BAZ"
                       :symbols '("FOO" "BAR" "BAZ")))
         (reader-enum (make-instance
                       'avro:enum
                       :name "foobar"
                       :aliases '("bar.foo")
                       :symbols '("FOO" "BAR")))
         (foo (make-instance writer-enum :enum "FOO"))
         (bar (make-instance writer-enum :enum "BAR"))
         (baz (make-instance writer-enum :enum "BAZ")))
    (is (typep foo writer-enum))
    (let ((foo (avro:coerce
                (avro:deserialize writer-enum (avro:serialize foo))
                reader-enum)))
      (is (typep foo reader-enum))
      (is (string= "FOO" (avro:which-one foo))))

    (is (typep bar writer-enum))
    (let ((bar (avro:coerce
                (avro:deserialize writer-enum (avro:serialize bar))
                reader-enum)))
      (is (typep bar reader-enum))
      (is (string= "BAR" (avro:which-one bar))))

    (is (typep baz writer-enum))
    (signals error
      (avro:coerce
       (avro:deserialize writer-enum (avro:serialize baz))
       reader-enum))))

(test reader-default
  (let* ((writer-enum (make-instance
                       'avro:enum
                       :name "foo"
                       :aliases '("baz" "foo.bar")
                       :default "BAZ"
                       :symbols '("FOO" "BAR" "BAZ")))
         (reader-enum (make-instance
                       'avro:enum
                       :name "foobar"
                       :aliases '("bar.foo")
                       :symbols '("FOO" "BAR")
                       :default "BAR"))
         (foo (make-instance writer-enum :enum "FOO"))
         (bar (make-instance writer-enum :enum "BAR"))
         (baz (make-instance writer-enum :enum "BAZ")))
    (is (typep foo writer-enum))
    (let ((foo (avro:coerce
                (avro:deserialize writer-enum (avro:serialize foo))
                reader-enum)))
      (is (typep foo reader-enum))
      (is (string= "FOO" (avro:which-one foo))))

    (is (typep bar writer-enum))
    (let ((bar (avro:coerce
                (avro:deserialize writer-enum (avro:serialize bar))
                reader-enum)))
      (is (typep bar reader-enum))
      (is (string= "BAR" (avro:which-one bar))))

    (is (typep baz writer-enum))
    (let ((baz (avro:coerce
                (avro:deserialize writer-enum (avro:serialize baz))
                reader-enum)))
      (is (typep baz reader-enum))
      (is (string= "BAR" (avro:which-one baz))))))
