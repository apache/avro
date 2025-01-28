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
(defpackage #:org.apache.avro.internal.ipc.handshake
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)))
(in-package #:org.apache.avro.internal.ipc.handshake)

(defclass internal:md5 ()
  ()
  (:metaclass api:fixed)
  (:size 16)
  (:enclosing-namespace "org.apache.avro.ipc"))

(defclass internal:union<null-md5> ()
  ()
  (:metaclass api:union)
  (:schemas api:null internal:md5))

(defclass internal:union<null-string> ()
  ()
  (:metaclass api:union)
  (:schemas api:null api:string))

(defclass api:map<bytes> ()
  ()
  (:metaclass api:map)
  (:values api:bytes)
  (:documentation
   "An avro map schema associating strings to bytes."))

(defclass internal:union<null-map<bytes>> ()
  ()
  (:metaclass api:union)
  (:schemas api:null api:map<bytes>))

(defclass internal:match ()
  ()
  (:metaclass api:enum)
  (:symbols "BOTH" "CLIENT" "NONE")
  (:name "HandshakeMatch")
  (:enclosing-namespace "org.apache.avro.ipc"))

;;; request

(defclass internal:request ()
  ((|clientHash|
    :initarg :client-hash
    :type internal:md5
    :reader internal:client-hash)
   (|clientProtocol|
    :initarg :client-protocol
    :type internal:union<null-string>
    :reader internal:client-protocol)
   (|serverHash|
    :initarg :server-hash
    :type internal:md5
    :reader internal:server-hash)
   (|meta|
    :initarg :meta
    :type internal:union<null-map<bytes>>
    :reader internal:meta))
  (:metaclass api:record)
  (:name "HandshakeRequest")
  (:namespace "org.apache.avro.ipc")
  (:default-initargs
   :client-protocol (make-instance 'internal:union<null-string> :object nil)
   :meta (make-instance 'internal:union<null-map<bytes>> :object nil)))

;;; response

(defclass internal:response ()
  ((|match|
    :initarg :match
    :type internal:match
    :reader internal:match)
   (|serverProtocol|
    :initarg :server-protocol
    :type internal:union<null-string>
    :reader internal:server-protocol)
   (|serverHash|
    :initarg :server-hash
    :type internal:union<null-md5>
    :reader internal:server-hash)
   (|meta|
    :initarg :meta
    :type internal:union<null-map<bytes>>
    :reader internal:meta))
  (:metaclass api:record)
  (:name "HandshakeResponse")
  (:namespace "org.apache.avro.ipc")
  (:default-initargs
   :server-protocol (make-instance 'internal:union<null-string> :object nil)
   :server-hash (make-instance 'internal:union<null-md5> :object nil)
   :meta (make-instance 'internal:union<null-map<bytes>> :object nil)))
