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
(defpackage #:org.apache.avro.internal.ipc.parse
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:name #:org.apache.avro.internal.name))
  (:export #:classes
           #:classes?
           #:symbol/class
           #:symbol/classes
           #:symbol/classes?
           #:messages
           #:messages?
           #:conses
           #:conses?
           #:early-types
           #:late-types
           #:early-messages
           #:late-messages))
(in-package #:org.apache.avro.internal.ipc.parse)

;;; types

(deftype classes ()
  '(simple-array class (*)))

(deftype classes? ()
  '(or null classes))

(deftype symbol/class ()
  '(or symbol class))

(deftype symbol/classes ()
  '(simple-array symbol/class (*)))

(deftype symbol/classes? ()
  '(or null symbol/classes))

(deftype messages ()
  '(simple-array api:message (*)))

(deftype messages? ()
  '(or null messages))

(deftype conses ()
  '(simple-array cons (*)))

(deftype conses? ()
  '(or null conses))

;;; early-types

(declaim
 (ftype (function (t boolean) (values symbol/classes? &optional)) early-types))
(defun early-types (types typesp)
  (when typesp
    (map 'symbol/classes #'early-type types)))

(declaim (ftype (function (t) (values symbol/class &optional)) early-type))
(defun early-type (type)
  (check-type type symbol/class)
  type)

;;; late-types

(declaim
 (ftype (function (symbol/classes?) (values classes? &optional)) late-types))
(defun late-types (types)
  (when types
    (map 'classes #'late-type types)))

(declaim (ftype (function (symbol/class) (values class &optional)) late-type))
(defun late-type (class)
  (let ((class (if (symbolp class)
                   (find-class class)
                   class)))
    (unless (subtypep class 'api:declared-rpc-error)
      (check-type class name:named-schema))
    class))

;;; early-messages

(declaim
 (ftype (function (t boolean) (values conses? &optional)) early-messages))
(defun early-messages (messages messagesp)
  (when messagesp
    (let ((messages (map 'conses #'early-message messages)))
      (assert-distinct-names messages)
      messages)))

(deftype schema? ()
  '(or api:schema symbol))

(declaim (ftype (function (t) (values cons &optional)) early-message))
(defun early-message (message)
  (assert-plist
   message :name :one-way :documentation :request :response :errors)
  (assert (member :name message))
  (check-type (getf message :name) symbol)
  (check-type (getf message :one-way) boolean)
  (check-type (getf message :documentation "") string)
  (assert (member :response message))
  (check-type (getf message :response) schema?)
  (assert-request message)
  (assert-errors message)
  message)

(declaim (ftype (function (cons) (values &optional)) assert-request))
(defun assert-request (message)
  (assert (member :request message))
  (map nil #'%assert-request (getf message :request))
  (values))

(declaim (ftype (function (t) (values &optional)) %assert-request))
(defun %assert-request (request)
  (assert-plist request :name :documentation :type :default :order :aliases)
  (apply #'make-instance 'api:field request)
  (values))

(declaim (ftype (function (cons) (values &optional)) assert-errors))
(defun assert-errors (message)
  (when (member :errors message)
    (let ((errors (getf message :errors)))
      (assert (plusp (length errors)))
      (map nil #'%assert-error errors)))
  (values))

(declaim (ftype (function (t) (values &optional)) %assert-error))
(defun %assert-error (error)
  (check-type error symbol/class)
  (values))

(declaim (ftype (function (conses) (values &optional)) assert-distinct-names))
(defun assert-distinct-names (messages)
  (flet ((name (message)
           (symbol-name (getf message :name))))
    (loop
      with names = (map '(simple-array string (*)) #'name messages)
      with distinct-names = (remove-duplicates names :test #'string=)
        initially
           (when (= (length names) (length distinct-names))
             (return))

      for distinct-name across distinct-names
      do (setf names (delete distinct-name names :count 1 :test #'string=))
      finally
         (error "Duplicate message names: ~S" names)))
  (values))

(declaim (ftype (function (t &rest symbol) (values &optional)) assert-plist))
(defun assert-plist (plist &rest expected-keys)
  (check-type plist cons)
  (assert (evenp (length plist)) (plist)
          "Expected a plist with an even number of elements: ~S" plist)
  (loop
    for key in plist by #'cddr
    when (member key expected-keys)
      collect key into keys
    finally
       (loop
         for expected-key in expected-keys
         do (setf keys (delete expected-key keys :count 1))
         finally
            (when keys
              (error "Duplicate keys: ~S" keys))))
  (values))

;;; late-messages

(declaim
 (ftype (function (classes? conses?) (values messages? &optional))
        late-messages))
(defun late-messages (types messages)
  (when messages
    (flet ((late-message (message)
             (late-message types message)))
      (map 'messages #'late-message messages))))

(declaim
 (ftype (function (classes? cons) (values api:message &optional))
        late-message))
(defun late-message (types message)
  (let* ((name (getf message :name))
         (initargs (list
                    :name name
                    :request (late-request types (getf message :request))
                    :response (late-response types (getf message :response)))))
    (when (member :one-way message)
      (push (getf message :one-way) initargs)
      (push :one-way initargs))
    (when (member :documentation message)
      (push (getf message :documentation) initargs)
      (push :documentation initargs))
    (when (member :errors message)
      (push (late-errors types (getf message :errors)) initargs)
      (push :errors initargs))
    (setf (symbol-function name)
          (apply #'make-instance 'api:message initargs))))

(declaim
 (ftype (function (classes? sequence) (values api:record &optional))
        late-request))
(defun late-request (types request)
  (flet ((%late-request (request)
           (%late-request types request)))
    (make-instance
     'api:record
     :name (make-symbol "anonymous")
     :direct-slots (map 'list #'%late-request request))))

(declaim
 (ftype (function (classes? cons) (values cons &optional)) %late-request))
(defun %late-request (types request)
  (let* ((type (getf request :type))
         (schema (if (and (symbolp type)
                          (not (typep type 'api:schema)))
                     (find-class type)
                     type)))
    (if (or (eq type schema)            ; when schema is defined inline
            (and (typep schema 'api:schema)
                 (not (typep schema 'name:named-schema))))
        request
        (let ((request (copy-list request)))
          (setf (getf request :type) schema)
          (assert (find schema types) ()
                  "Named schema ~S not found in types ~S" schema types)
          request))))

(declaim
 (ftype (function (classes? schema?) (values api:schema &optional))
        late-response))
(defun late-response (types response)
  (let ((schema (if (and (symbolp response)
                         (not (typep response 'api:schema)))
                    (find-class response)
                    response)))
    (when (and (not (eq response schema))
               (typep schema 'name:named-schema)
               (not (find schema types)))
      (error "Named schema ~S not found in types ~S" schema types))
    schema))

(declaim
 (ftype (function (classes? cons) (values classes &optional)) late-errors))
(defun late-errors (types errors)
  (flet ((late-error (error)
           (late-error types error)))
    (map 'classes #'late-error errors)))

(declaim
 (ftype (function (classes? symbol/class) (values class &optional))
        late-error))
(defun late-error (types error?)
  (let ((error (if (symbolp error?)
                   (find-class error?)
                   error?)))
    (unless (or (eq error? error)
                (find error types))
      (error "Error ~S not found in types ~S" error types))
    error))
