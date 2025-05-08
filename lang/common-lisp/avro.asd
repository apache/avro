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

(cl:in-package #:asdf-user)

(defsystem #:avro
  :description "Implementation of the Apache Avro data serialization system."
  :version "0.0.1"
  :license "Apache-2.0"
  :pathname "src"
  :in-order-to ((test-op (test-op #:avro/test)))
  :depends-on (#:alexandria
               #:babel
               #:chipz
               #:closer-mop
               #:ieee-floats
               #:flexi-streams
               #:local-time
               #:local-time-duration
               #:md5
               #:salza2
               #:st-json
               #:time-interval
               #:trivial-extensible-sequences)
  :components ((:file "type")
               (:file "mop")
               (:file "ascii")
               (:file "little-endian" :depends-on ("type"))
               (:file "crc-64-avro" :depends-on ("type"))
               (:module "api"
                :components ((:file "public")
                             (:file "internal")))
               (:file "intern" :depends-on ("api"))
               (:module "recursive-descent"
                :depends-on ("api" "mop" "type")
                :components ((:file "pattern")
                             (:file "jso" :depends-on ("pattern"))))
               (:module "primitive"
                :depends-on ("type"
                             "little-endian"
                             "crc-64-avro"
                             "api"
                             "recursive-descent")
                :components ((:file "defprimitive")
                             (:file "compare")
                             (:file "ieee-754" :depends-on ("compare"))
                             (:file "zigzag" :depends-on ("compare"))
                             (:file "null" :depends-on ("defprimitive"))
                             (:file "boolean" :depends-on ("defprimitive"))
                             (:file "int" :depends-on ("zigzag"
                                                       "defprimitive"))
                             (:file "long" :depends-on ("zigzag"
                                                        "defprimitive"))
                             (:file "float" :depends-on ("ieee-754"
                                                         "defprimitive"))
                             (:file "double" :depends-on ("ieee-754"
                                                          "defprimitive"))
                             (:file "bytes" :depends-on ("long"
                                                         "defprimitive"
                                                         "compare"))
                             (:file "string" :depends-on ("bytes"
                                                          "long"
                                                          "defprimitive"))))
               (:file "schema"
                :depends-on ("primitive"
                             "api"
                             "mop"
                             "little-endian"
                             "type"
                             "crc-64-avro"
                             "recursive-descent"))
               (:module "name"
                :depends-on ("api"
                             "mop"
                             "recursive-descent"
                             "schema"
                             "type"
                             "ascii"
                             "intern")
                :components ((:file "type")
                             (:file "deduce" :depends-on ("type"))
                             (:file "class" :depends-on ("type" "deduce"))
                             (:file "schema" :depends-on ("type"
                                                          "deduce"
                                                          "class"))
                             (:file "coerce" :depends-on ("deduce" "schema"))
                             (:file "package" :depends-on ("type"
                                                           "deduce"
                                                           "class"
                                                           "schema"
                                                           "coerce"))))
               (:module "complex"
                :depends-on ("type"
                             "api"
                             "mop"
                             "recursive-descent"
                             "schema"
                             "name"
                             "intern")
                :components ((:file "count-and-size")
                             (:file "array" :depends-on ("count-and-size"))
                             (:file "map" :depends-on ("count-and-size"))
                             (:file "union")
                             (:file "fixed")
                             (:file "enum")
                             (:file "record")))
               (:module "logical"
                :depends-on ("type"
                             "mop"
                             "ascii"
                             "api"
                             "recursive-descent"
                             "primitive"
                             "schema"
                             "complex")
                :components ((:file "uuid")
                             (:file "datetime")
                             (:file "date" :depends-on ("datetime"))
                             (:file "time-millis" :depends-on ("datetime"))
                             (:file "time-micros" :depends-on ("datetime"))
                             (:file "timestamp-millis"
                              :depends-on ("datetime"))
                             (:file "timestamp-micros"
                              :depends-on ("datetime"))
                             (:file "local-timestamp-millis"
                              :depends-on ("datetime"))
                             (:file "local-timestamp-micros"
                              :depends-on ("datetime"))
                             (:file "big-endian")
                             (:file "decimal" :depends-on ("big-endian"))
                             (:file "duration")))
               (:module "file"
                :depends-on ("primitive" "complex" "logical")
                :components ((:file "file-header")
                             (:file "file-block" :depends-on ("file-header"))
                             (:file "file-reader" :depends-on ("file-block"))
                             (:file "file-writer"
                              :depends-on ("file-block"))))
               (:module "ipc"
                :depends-on ("primitive" "complex" "logical" "intern")
                :components ((:file "handshake")
                             (:file "error" :depends-on ("handshake"))
                             (:file "message" :depends-on ("error"))
                             (:file "framing" :depends-on ("handshake"))
                             (:file "parse" :depends-on ("error" "message"))
                             (:file "protocol" :depends-on ("parse"
                                                            "handshake"))
                             (:file "client" :depends-on ("framing"
                                                          "protocol"))
                             (:file "protocol-object" :depends-on ("client"))
                             (:file "server"
                              :depends-on ("framing"
                                           "protocol"
                                           "protocol-object"))))))

(defsystem #:avro/asdf
  :description "Utilities to define ASDF systems containing avro source files."
  :version "0.0.1"
  :license "Apache-2.0"
  :pathname "src"
  :depends-on (#:avro)
  :components ((:file "asdf")))

(defsystem #:avro/test
  :description "Tests for avro."
  :license "Apache-2.0"
  :pathname "test"
  :perform (test-op (op sys) (uiop:symbol-call :1am :run))
  :depends-on (#:avro #:1am #:flexi-streams #:named-readtables #:babel)
  :components ((:file "common")
               (:file "compare")
               (:file "file")
               (:file "name")
               (:file "reinitialization")
               (:file "intern")
               (:module "complex"
                :depends-on ("common")
                :components ((:file "array")
                             (:file "enum")
                             (:file "fixed")
                             (:file "map")
                             (:file "record")
                             (:file "union")))
               (:module "logical"
                :depends-on ("common")
                :components ((:file "date")
                             (:file "decimal")
                             (:file "duration")
                             (:file "local-timestamp-micros")
                             (:file "local-timestamp-millis")
                             (:file "time-micros")
                             (:file "time-millis")
                             (:file "timestamp-micros")
                             (:file "timestamp-millis")
                             (:file "uuid")))
               (:module "primitive"
                :depends-on ("common")
                :components ((:file "boolean")
                             (:file "bytes")
                             (:file "double")
                             (:file "float")
                             (:file "int")
                             (:file "long")
                             (:file "null")
                             (:file "string")))
               (:module "resolution"
                :components ((:file "base")
                             (:file "primitive"
                              :depends-on ("base"))
                             (:file "promote"
                              :depends-on ("base"))
                             (:module "complex"
                              :depends-on ("base")
                              :components ((:file "array")
                                           (:file "enum")
                                           (:file "fixed")
                                           (:file "map")
                                           (:file "record")
                                           (:file "union")))
                             (:module "logical"
                              :depends-on ("base")
                              :components ((:file "date")
                                           (:file "decimal")
                                           (:file "duration")
                                           (:file "local-timestamp-micros")
                                           (:file "local-timestamp-millis")
                                           (:file "time-micros")
                                           (:file "time-millis")
                                           (:file "timestamp-micros")
                                           (:file "timestamp-millis")
                                           (:file "uuid")))))
               (:module "ipc"
                :components ((:file "common")
                             (:file "stateless"
                              :depends-on ("common"))
                             (:file "stateful"
                              :depends-on ("common"))))))
