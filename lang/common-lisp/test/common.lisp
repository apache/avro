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
(defpackage #:org.apache.avro/test/common
  (:use #:cl)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:export #:field
           #:json=
           #:json-string=
           #:define-schema-test
           #:define-io-test
           #:json-syntax))
(in-package #:org.apache.avro/test/common)

;;; field

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

;;; json=

(declaim
 (ftype (function (st-json:jso st-json:jso) (values boolean &optional))
        same-fields-p))
(defun same-fields-p (lhs rhs)
  (flet ((fields (jso)
           (let (fields)
             (flet ((fill-fields (field value)
                      (declare (ignore value))
                      (push field fields)))
               (st-json:mapjso #'fill-fields jso))
             fields)))
    (let ((lhs (fields lhs))
          (rhs (fields rhs)))
      (null (nset-difference lhs rhs :test #'string=)))))

(declaim
 (ftype (function (st-json:jso st-json:jso) (values boolean &optional)) jso=))
(defun jso= (lhs rhs)
  (when (same-fields-p lhs rhs)
    (st-json:mapjso (lambda (field lhs)
                      (let ((rhs (st-json:getjso field rhs)))
                        (unless (json= lhs rhs)
                          (return-from jso= nil))))
                    lhs)
    t))

(declaim (ftype (function (t t) (values boolean &optional)) json=))
(defun json= (lhs rhs)
  (when (equal (type-of lhs) (type-of rhs))
    (typecase lhs
      (st-json:jso (jso= lhs rhs))
      (cons (and (= (length lhs) (length rhs))
                 (every #'json= lhs rhs)))
      (t (equal lhs rhs)))))

(declaim
 (ftype (function (string string) (values boolean &optional)) json-string=))
(defun json-string= (lhs rhs)
  (apply #'json= (mapcar #'st-json:read-json (list lhs rhs))))

;;; define-schema-test

(defmacro define-schema-test
    (name json canonical-form fingerprint make-instance-schema
     &rest defclass-schema)
  (declare (symbol name)
           (string json canonical-form)
           ((unsigned-byte 64) fingerprint)
           (cons defclass-schema)
           (cons make-instance-schema))
  `(1am:test ,name
     (let* ((expected-json ,json)
            (canonical-form ,canonical-form)
            (expected-fingerprint ,fingerprint)
            (make-instance-schema ,make-instance-schema)
            (defclass-schema (progn ,@defclass-schema))
            (expected-jso (st-json:read-json expected-json))
            (actual-schema (avro:deserialize 'avro:schema expected-json)))
       (1am:is (json= expected-jso (st-json:read-json
                                    (avro:serialize defclass-schema))))
       (1am:is (json= expected-jso (st-json:read-json
                                    (avro:serialize make-instance-schema))))
       (1am:is (json= expected-jso (st-json:read-json
                                    (avro:serialize actual-schema))))

       (1am:is (string= canonical-form (avro:serialize
                                        defclass-schema :canonical-form-p t)))
       (1am:is
        (string= canonical-form (avro:serialize
                                 make-instance-schema :canonical-form-p t)))
       (1am:is (string= canonical-form (avro:serialize
                                        actual-schema :canonical-form-p t)))

       (1am:is (= expected-fingerprint (avro:fingerprint defclass-schema)))
       (1am:is
        (= expected-fingerprint (avro:fingerprint make-instance-schema)))
       (1am:is (= expected-fingerprint (avro:fingerprint actual-schema))))))

;;; define-io-test

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun schema-pair-p (schema)
    (declare ((or symbol cons) schema))
    (and (consp schema)
         (not
          (member (first schema) '(make-instance closer-mop:ensure-class))))))

(declaim
 (ftype (function (avro:schema)
                  (values (simple-array (unsigned-byte 8) (8)) &optional))
        little-endian-fingerprint))
(defun little-endian-fingerprint (schema)
  (loop
    with vector = (make-array 8 :element-type '(unsigned-byte 8))
    and fingerprint = (avro:fingerprint schema)

    for shift from 0 above -64 by 8
    for byte = (logand #xff (ash fingerprint shift))
    for index from 0 below 8

    do (setf (elt vector index) byte)

    finally
       (return vector)))

(defmacro define-io-test
    (name (&rest context) schema object (&rest serialized) &body check)
  (declare (symbol name))
  (flet ((assert-binding (binding)
           (etypecase binding
             (cons
              (unless (= (length binding) 2)
                (error "Expected a binding pair ~S" binding))
              (check-type (first binding) symbol))
             (symbol))))
    (map nil #'assert-binding context))
  (let ((schema-symbol (intern "SCHEMA"))
        (object-symbol (intern "OBJECT"))
        (arg (intern "ARG"))
        (schema (if (schema-pair-p schema) (first schema) schema))
        (schema-to-check (when (schema-pair-p schema) (second schema))))
    `(1am:test ,name
       (let* (,@context
              (,schema-symbol ,(let ((schema schema))
                                 (if (symbolp schema)
                                     (if (typep schema 'avro:schema)
                                         `',schema
                                         `(find-class ',schema))
                                     schema)))
              (,object-symbol ,object)
              (serialized (make-array ,(length serialized)
                                      :element-type '(unsigned-byte 8)
                                      :initial-contents ',serialized))
              (schema-to-check
                ,(if schema-to-check
                     (if (symbolp schema-to-check)
                         (if (typep schema-to-check 'avro:schema)
                             `',schema-to-check
                             (find-class schema-to-check))
                         schema-to-check)
                     schema-symbol)))
         (flet ((check (,arg)
                  (declare (ignorable ,arg))
                  ,@check))
           (1am:is (eq schema-to-check (avro:schema-of ,object-symbol)))
           (check ,object-symbol)

           (multiple-value-bind (bytes size)
               (avro:serialize ,object-symbol)
             (1am:is (= (length serialized) size))
             (1am:is (equalp serialized bytes)))

           (let ((into (make-array (1+ (length serialized))
                                   :element-type '(unsigned-byte 8))))
             (multiple-value-bind (bytes size)
                 (avro:serialize ,object-symbol :into into :start 1)
               (1am:is (eq into bytes))
               (1am:is (= (length serialized) size))
               (1am:is (equalp serialized (subseq into 1))))
             (1am:signals error
               (avro:serialize ,object-symbol :into into :start 2)))

           (let ((bytes (flexi-streams:with-output-to-sequence (stream)
                          (multiple-value-bind (returned size)
                              (avro:serialize ,object-symbol :into stream)
                            (1am:is (eq stream returned))
                            (1am:is (= (length serialized) size))))))
             (1am:is (equalp serialized bytes)))

           (multiple-value-bind (deserialized size)
               (avro:deserialize ,schema-symbol serialized)
             (1am:is (= (length serialized) size))
             (1am:is (eq schema-to-check (avro:schema-of deserialized)))
             (check deserialized))

           (let ((input (make-array (1+ (length serialized))
                                    :element-type '(unsigned-byte 8))))
             (replace input serialized :start1 1)
             (multiple-value-bind (deserialized size)
                 (avro:deserialize ,schema-symbol input :start 1)
               (1am:is (= (length serialized) size))
               (1am:is (eq schema-to-check (avro:schema-of deserialized)))
               (check deserialized)))

           (multiple-value-bind (deserialized size)
               (flexi-streams:with-input-from-sequence (stream serialized)
                 (avro:deserialize ,schema-symbol stream))
             (1am:is (= (length serialized) size))
             (1am:is (eq schema-to-check (avro:schema-of deserialized)))
             (check deserialized))

           (multiple-value-bind (bytes size)
               (avro:serialize
                ,object-symbol :single-object-encoding-p ,schema-symbol)
             (1am:is (= (+ 10 (length serialized)) size))
             (1am:is (equalp #(#xc3 #x01) (subseq bytes 0 2)))
             (1am:is (equalp (little-endian-fingerprint ,schema-symbol)
                             (subseq bytes 2 10)))
             (1am:is (equalp serialized (subseq bytes 10)))
             (multiple-value-bind (fingerprint size)
                 (avro:deserialize 'avro:fingerprint bytes)
               (1am:is (= 10 size))
               (1am:is (= (avro:fingerprint ,schema-symbol) fingerprint))
               (multiple-value-bind (deserialized size)
                   (avro:deserialize ,schema-symbol bytes :start size)
                 (1am:is (= (length serialized) size))
                 (1am:is (eq schema-to-check (avro:schema-of deserialized)))
                 (check deserialized))))

           (let ((into (make-array (+ 11 (length serialized))
                                   :element-type '(unsigned-byte 8))))
             (multiple-value-bind (bytes size)
                 (avro:serialize
                  ,object-symbol
                  :into into :start 1 :single-object-encoding-p ,schema-symbol)
               (1am:is (eq into bytes))
               (1am:is (= (+ 10 (length serialized)) size))
               (1am:is (equalp #(#xc3 #x01) (subseq bytes 1 3)))
               (1am:is (equalp (little-endian-fingerprint ,schema-symbol)
                               (subseq bytes 3 11)))
               (1am:is (equalp serialized (subseq bytes 11)))
               (multiple-value-bind (fingerprint size)
                   (avro:deserialize 'avro:fingerprint bytes :start 1)
                 (1am:is (= 10 size))
                 (1am:is (= (avro:fingerprint ,schema-symbol) fingerprint))
                 (multiple-value-bind (deserialized size)
                     (avro:deserialize ,schema-symbol bytes :start (1+ size))
                   (1am:is (= (length serialized) size))
                   (1am:is (eq schema-to-check (avro:schema-of deserialized)))
                   (check deserialized))))
             (1am:signals error
               (avro:serialize
                ,object-symbol
                :into into :start 2 :single-object-encoding-p ,schema-symbol)))

           (let ((bytes
                   (flexi-streams:with-output-to-sequence (stream)
                     (multiple-value-bind (returned size)
                         (avro:serialize
                          ,object-symbol
                          :into stream
                          :single-object-encoding-p ,schema-symbol)
                       (1am:is (eq stream returned))
                       (1am:is (= (+ 10 (length serialized)) size))))))
             (setf bytes (coerce bytes '(simple-array (unsigned-byte 8) (*))))
             (1am:is (equalp #(#xc3 #x01) (subseq bytes 0 2)))
             (1am:is (equalp (little-endian-fingerprint ,schema-symbol)
                             (subseq bytes 2 10)))
             (1am:is (equalp serialized (subseq bytes 10)))
             (multiple-value-bind (fingerprint size)
                 (avro:deserialize 'avro:fingerprint bytes)
               (1am:is (= 10 size))
               (1am:is (= (avro:fingerprint ,schema-symbol) fingerprint))
               (multiple-value-bind (deserialized size)
                   (avro:deserialize ,schema-symbol bytes :start size)
                 (1am:is (= (length serialized) size))
                 (1am:is (eq schema-to-check (avro:schema-of deserialized)))
                 (check deserialized)))))))))

;;; json-syntax

(defun read-{ (stream char)
  (unread-char char stream)
  (st-json:write-json-to-string (st-json:read-json stream)))

(defun read-[ (stream char)
  (unread-char char stream)
  (st-json:write-json-to-string (st-json:read-json stream)))

(named-readtables:defreadtable json-syntax
  (:merge :standard)
  (:macro-char #\{ #'read-{)
  (:macro-char #\[ #'read-[))
