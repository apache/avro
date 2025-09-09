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
(defpackage #:org.apache.avro.internal.ipc.protocol-object
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal)))
(in-package #:org.apache.avro.internal.ipc.protocol-object)

(defclass api:protocol-object ()
  ((transceiver
    :initarg :transceiver
    :reader api:transceiver
    :type (or internal:client api:server)
    :documentation "Protocol transceiver."))
  (:default-initargs
   :transceiver (error "Must supply TRANSCEIVER"))
  (:documentation
   "Base class of avro protocols."))

(defmethod initialize-instance :after
    ((instance api:protocol-object) &key)
  (with-slots (transceiver) instance
    (when (typep transceiver 'internal:client)
      (let* ((protocol (class-of instance))
             (messages (api:messages protocol)))
        (internal:add-methods protocol transceiver messages)
        (setf (internal:server-hash transceiver) (internal:md5 protocol)
              (internal:server-protocol transceiver) protocol)))))
