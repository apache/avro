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
(defpackage #:org.apache.avro/asdf
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro))
  (:export #:avro-file)
  (:documentation
   "Integrates avro files with asdf.

This package provides an AVRO-FILE asdf component which integrates avro schema
and protocol json files with asdf. Avro files can be specified alongside lisp
source files, and AVRO-FILE will \"generate code\" based on those avro files:
schemas and protocols will be deserialized from their json forms and the
resulting classes interned in the deduced packages.

An example project looks like:

  ;;;
  ;;; file example.asd
  ;;;

  (in-package #:asdf-user)

  (defsystem #:example
    :defsystem-depends-on (#:org.apache.avro/asdf)
    :depends-on (#:org.apache.avro)
    :components ((:file \"example\")
                 (:avro-file \"example.avsc\")))

  ;;;
  ;;; file foo.avsc
  ;;;

  {
      \"name\": \"EXAMPLE.CARTESIAN_POINT\",
      \"type\": \"record\",
      \"fields\": [
           {\"name\": \"X\", \"type\": \"int\"},
           {\"name\": \"Y\", \"type\": \"int\"},
      ],
  }

  ;;;
  ;;; file example.lisp
  ;;;

  (in-package #:cl-user)

  (let ((point (make-instance 'example:cartesian_point :x 3 :y 4)))
    (example:x point)  ;; => 3
    (example:y point)  ;; => 4
    (setf (example:y point) 7)
    (example:y point)) ;; => 7"))
(in-package #:org.apache.avro/asdf)

(defclass avro-file (asdf:file-component)
  ((processed-p
    :type boolean
    :accessor processed-p
    :initarg :processed-p))
  (:default-initargs
   :type nil
   :processed-p nil)
  (:documentation
   "ASDF component representing avro schema or protocol files.

Only json files are supported."))

(deftype filespec ()
  '(or string pathname))

(declaim
 (ftype (function (filespec) (values simple-string &optional)) read-file))
(defun read-file (filespec)
  (with-open-file (stream filespec :external-format :utf-8)
    (let* ((length (file-length stream))
           (string (make-string length)))
      (assert (= (read-sequence string stream) length))
      string)))

(declaim
 (ftype (function (filespec) (values (or api:schema api:protocol) &optional))
        deserialize))
(defun deserialize (filespec)
  (let* ((unparsed (read-file filespec))
         (jso (st-json:read-json unparsed t)))
    (if (nth-value 1 (st-json:getjso "protocol" jso))
        (api:deserialize 'api:protocol unparsed)
        (api:deserialize 'api:schema unparsed))))

(declaim
 (ftype (function (asdf:operation avro-file) (values &optional)) process))
(defun process (operation component)
  (unless (processed-p component)
    (loop
      for input-file in (asdf:input-files operation component)
      do (api:intern (deserialize input-file)))
    (setf (processed-p component) t))
  (values))

(defmethod asdf:perform ((operation asdf:compile-op) (component avro-file))
  (process operation component))

(defmethod asdf:perform ((operation asdf:load-op) (component avro-file))
  (process operation component))

(defmethod asdf:perform ((operation asdf:load-source-op) (component avro-file))
  (process operation component))

(setf (find-class 'asdf::avro-file) (find-class 'avro-file))
