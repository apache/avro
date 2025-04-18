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
(defpackage #:org.apache.avro/test/map
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro))
  (:import-from #:org.apache.avro/test/common
                #:define-schema-test
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/map)

(named-readtables:in-readtable json-syntax)

;; TODO need to support default
(define-schema-test long-map
  {
    "type": "map",
    "values": "long"
  }
  {
    "type": "map",
    "values": "long"
  }
  #x4e33b109e4f4746f
  (make-instance
   'avro:map
   :values 'avro:long)
  (defclass long_map ()
    ()
    (:metaclass avro:map)
    (:values avro:long)))

(define-schema-test enum-map
  {
    "type": "map",
    "values": {
      "type": "enum",
      "name": "Test",
      "symbols": ["A", "B"]
    }
  }
  {
    "type": "map",
    "values": {
      "name": "Test",
      "type": "enum",
      "symbols": ["A", "B"]
    }
  }
  #x2d816b6f62b02adf
  (make-instance
   'avro:map
   :values (make-instance
            'avro:enum
            :name '|Test|
            :symbols '("A" "B")))
  (defclass |Test| ()
    ()
    (:metaclass avro:enum)
    (:symbols "A" "B"))
  (defclass enum_map ()
    ()
    (:metaclass avro:map)
    (:values |Test|)))

(define-schema-test map<enum-name>
  {
    "type": "map",
    "values": {
      "type": "enum",
      "name": "EnumName",
      "default": "BAR",
      "symbols": [
        "FOO",
        "BAR",
        "BAZ"
      ]
    }
  }
  {
    "type": "map",
    "values": {
      "name": "EnumName",
      "type": "enum",
      "symbols": [
        "FOO",
        "BAR",
        "BAZ"
      ]
    }
  }
  #x94b9ced2264892b3
  (make-instance
   'avro:map
   :values (make-instance
            'avro:enum
            :name "EnumName"
            :symbols '("FOO" "BAR" "BAZ")
            :default "BAR"))
  (defclass |EnumName| ()
    ()
    (:metaclass avro:enum)
    (:symbols "FOO" "BAR" "BAZ")
    (:default "BAR"))
  (defclass map<enum-name> ()
    ()
    (:metaclass avro:map)
    (:values |EnumName|)))

(define-io-test io
    ((enum-schema (make-instance 'avro:enum :name "Test" :symbols '("A" "B")))
     (expected '(("a" . "A") ("aa" . "A") ("b" . "B"))))
    (make-instance 'avro:map :values enum-schema)
    (let ((map (make-instance schema)))
      (dolist (cons expected)
        (destructuring-bind (key . value) cons
          (let ((enum (make-instance enum-schema :enum value)))
            (setf (avro:gethash key map) enum))))
      map)
    (5 20 2 #x61 0 4 #x61 #x61 0 2 #x62 2 0)
  (let (sorted-alist)
    (flet ((fill-alist (key value)
             (let ((cons (cons key (avro:which-one value))))
               (push cons sorted-alist))))
      (avro:maphash #'fill-alist arg))
    (setf sorted-alist (sort sorted-alist #'string< :key #'car))
    (is (equal expected sorted-alist))))

(test late-type-check
  (setf (find-class 'late_map) nil
        (find-class 'late_enum) nil)

  (defclass late_map ()
    ()
    (:metaclass avro:map)
    (:values late_enum))

  (signals error
    (avro:values (find-class 'late_map)))

  (defclass late_enum ()
    ()
    (:metaclass avro:enum)
    (:symbols "FOO" "BAR"))

  (is (eq (find-class 'late_enum) (avro:values (find-class 'late_map)))))
