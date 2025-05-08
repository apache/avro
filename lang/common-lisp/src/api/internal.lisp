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

(defpackage #:org.apache.avro.internal
  (:export #:fixed-size
           #:serialize-field-default
           #:deserialize-field-default
           #:skip
           #:underlying
           #:readers
           #:writers
           #:duplicates
           #:md5
           #:union<null-md5>
           #:union<null-string>
           #:union<null-map<bytes>>
           #:match
           #:request
           #:client-hash
           #:client-protocol
           #:server-hash
           #:meta
           #:response
           #:server-protocol
           #:make-declared-rpc-error
           #:to-record
           #:schema
           #:client
           #:add-methods
           #:serialize
           #:crc-64-avro-little-endian
           #:read-jso
           #:with-initargs
           #:write-jso
           #:write-json
           #:logical-name
           #:downcase-symbol))

(in-package #:org.apache.avro.internal)

(cl:defgeneric logical-name (schema))

(cl:defgeneric write-jso (schema seen canonical-form-p))

(cl:defgeneric write-json (jso-object into cl:&key cl:&allow-other-keys))

(cl:defgeneric crc-64-avro-little-endian (schema))

(cl:defgeneric serialize (object into cl:&key cl:&allow-other-keys))

(cl:defgeneric schema (error))

(cl:defgeneric writers (effective-field))

(cl:defgeneric readers (effective-field))

(cl:defgeneric underlying (schema))

(cl:defgeneric skip (schema input cl:&optional start))

(cl:defgeneric fixed-size (schema))

(cl:defgeneric serialize-field-default (default))

(cl:defgeneric deserialize-field-default (schema default))

(cl:in-package #:cl-user)
