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
(defpackage #:org.apache.avro/test/file
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:use #:cl #:1am))
(in-package #:org.apache.avro/test/file)

(defparameter *weather-filespec*
  (asdf:system-relative-pathname 'avro/test "test/weather.avro"))

(declaim
 (ftype (function (avro:record-object simple-string)
                  (values avro:object &optional))
        field))
(defun field (record field)
  (let ((found-field (find field (avro:fields (class-of record))
                           :key #'avro:name :test #'string=)))
    (unless found-field
      (error "No such field ~S" field))
    (slot-value record (nth-value 1 (avro:name found-field)))))

(test read-file
  (with-open-file (stream *weather-filespec* :element-type '(unsigned-byte 8))
    (let ((expected '(("011990-99999" -619524000000 0)
                      ("011990-99999" -619506000000 22)
                      ("011990-99999" -619484400000 -11)
                      ("012650-99999" -655531200000 111)
                      ("012650-99999" -655509600000 78)))
          (actual (loop
                    with records = nil
                    with reader = (make-instance
                                   'avro:file-reader :input stream)
                    for block = (avro:read-block reader)
                    while block
                    do (setf records (concatenate 'list records block))
                    finally (return records))))
      (map nil
           (lambda (lhs rhs)
             (let ((station (field rhs "station"))
                   (time (field rhs "time"))
                   (temp (field rhs "temp")))
               (is (equal lhs (list station time temp)))))
           expected
           actual))))

(test write-file
  (let ((bytes (flexi-streams:make-in-memory-output-stream)))
    (with-open-file
        (stream *weather-filespec* :element-type '(unsigned-byte 8))
      (loop
        with reader = (make-instance 'avro:file-reader :input stream)
        with writer = (make-instance
                       'avro:file-writer
                       :meta (make-instance
                              'avro:meta
                              :schema (avro:schema (avro:file-header reader)))
                       :output bytes)
        for block = (avro:read-block reader)
        while block
        do (avro:write-block writer block)))
    (let ((expected '(("011990-99999" -619524000000 0)
                      ("011990-99999" -619506000000 22)
                      ("011990-99999" -619484400000 -11)
                      ("012650-99999" -655531200000 111)
                      ("012650-99999" -655509600000 78)))
          (actual (loop
                    with records = nil
                    with reader
                      = (make-instance
                         'avro:file-reader
                         :input (flexi-streams:get-output-stream-sequence
                                 bytes))
                    for block = (avro:read-block reader)
                    while block
                    do (setf records (concatenate 'list records block))
                    finally (return records))))
      (map nil
           (lambda (lhs rhs)
             (let ((station (field rhs "station"))
                   (time (field rhs "time"))
                   (temp (field rhs "temp")))
               (is (equal lhs (list station time temp)))))
           expected
           actual))))

(test skip-block
  (with-open-file (stream *weather-filespec* :element-type '(unsigned-byte 8))
    (let ((reader (make-instance 'avro:file-reader :input stream)))
      (is (avro:skip-block reader))
      (is (null (avro:skip-block reader)))
      (is (null (avro:read-block reader))))))
