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
(defpackage #:org.apache.avro/test/name
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:use #:cl #:1am))
(in-package #:org.apache.avro/test/name)

(test both
  (let* ((class-name (gensym))
         (name "foo")
         (schema (make-instance
                  'avro:fixed :name class-name :name name :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) name))))

(test class-only
  (let* ((name "foo")
         (class-name (make-symbol name))
         (schema (make-instance 'avro:fixed :name class-name :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) name))))

(test none
  (signals error
    (make-instance 'avro:fixed :size 1)))

(test reinitialize-both
  (let* ((class-name (gensym))
         (name "foo")
         (other-name (format nil "~A_bar" name))
         (other-class-name (gensym))
         (schema (make-instance
                  'avro:fixed :name class-name :name name :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) name))
    (is (eq schema (reinitialize-instance
                    schema :name other-class-name :name other-name :size 1)))
    (is (eq (class-name schema) other-class-name))
    (is (string= (avro:name schema) other-name))))

(test reinitialize-class-only
  (let* ((name "foo")
         (class-name (make-symbol name))
         (other-name (format nil "~A_bar" name))
         (other-class-name (make-symbol other-name))
         (schema (make-instance
                  'avro:fixed :name class-name :name name :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) name))
    (is (eq schema (reinitialize-instance
                    schema :name other-class-name :size 1)))
    (is (eq (class-name schema) other-class-name))
    (is (string= (avro:name schema) other-name))))

(test reinitialize-name-only
  (let* ((name "foo")
         (class-name (make-symbol name))
         (other-name (format nil "~A_bar" name))
         (schema (make-instance
                  'avro:fixed :name class-name :name name :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) name))
    (is (eq schema (reinitialize-instance schema :name other-name :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) other-name))))

(test reinitialize-none
  (let* ((name "foo")
         (other-name (format nil "~A_bar" name))
         (class-name (make-symbol other-name))
         (schema (make-instance
                  'avro:fixed :name class-name :name name :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) name))
    (is (eq schema (reinitialize-instance schema :size 1)))
    (is (eq (class-name schema) class-name))
    (is (string= (avro:name schema) other-name))))
