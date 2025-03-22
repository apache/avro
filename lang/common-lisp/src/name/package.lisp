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

(defpackage #:org.apache.avro.internal.name
  (:import-from #:org.apache.avro.internal.name.type
                #:name
                #:namespace
                #:fullname)
  (:import-from #:org.apache.avro.internal.name.deduce
                #:fullname->name
                #:deduce-namespace
                #:deduce-fullname)
  (:import-from #:org.apache.avro.internal.name.class
                #:named-class)
  (:import-from #:org.apache.avro.internal.name.schema
                #:named-schema
                #:valid-name
                #:valid-fullname)
  (:import-from #:org.apache.avro.internal.name.coerce
                #:assert-matching-names)
  (:export #:name
           #:namespace
           #:fullname
           #:valid-name
           #:valid-fullname
           #:fullname->name
           #:deduce-namespace
           #:deduce-fullname
           #:named-class
           #:named-schema
           #:assert-matching-names))
