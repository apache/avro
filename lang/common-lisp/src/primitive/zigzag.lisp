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
(defpackage #:org.apache.avro.internal.zigzag
  (:use #:cl)
  (:import-from #:org.apache.avro.internal.type
                #:vector<uint8>
                #:ufixnum
                #:comparison)
  (:import-from #:org.apache.avro.internal.compare
                #:compare-reals)
  (:export #:implement))
(in-package #:org.apache.avro.internal.zigzag)

;;; size

(defmacro define-size (bits)
  (declare ((member 32 64) bits))
  (let* ((max-size-name (intern "+MAX-SIZE+"))
         (size-name (intern "SIZE"))
         (max-size (ceiling bits 7)))
    `(progn
       (declaim (ufixnum ,max-size-name))
       (defconstant ,max-size-name ,max-size)

       (deftype ,size-name ()
         '(integer 1 ,max-size)))))

;;; zigzag

(defmacro define-to-zigzag (bits)
  (declare ((member 32 64) bits))
  (let ((name (intern "TO-ZIGZAG"))
        (uintN `(unsigned-byte ,bits))
        (sintN `(signed-byte ,bits)))
    `(progn
       (declaim (ftype (function (,sintN) (values ,uintN &optional)) ,name))

       (defun ,name (object)
         (logxor (ash object 1)
                 (ash object ,(* -1 (1- bits))))))))

(defmacro define-from-zigzag (bits)
  (declare ((member 32 64) bits))
  (let ((name (intern "FROM-ZIGZAG"))
        (uintN `(unsigned-byte ,bits))
        (sintN `(signed-byte ,bits)))
    `(progn
       (declaim (ftype (function (,uintN) (values ,sintN &optional)) ,name))
       (defun ,name (zigzag)
         (logxor (ash zigzag -1) (- (logand zigzag 1)))))))

(defmacro define-to/from-zigzag (bits)
  (declare ((member 32 64) bits))
  `(progn
     (define-to-zigzag ,bits)
     (define-from-zigzag ,bits)))

;;; varint

(defmacro define-read-varint (bits vector/stream)
  (declare ((member 32 64) bits)
           ((member vector stream) vector/stream))
  (let* ((name (intern (format nil "READ-VARINT-FROM-~A" vector/stream)))
         (vectorp (eq 'vector vector/stream))
         (arg-types (if vectorp '(vector<uint8> ufixnum) '(stream)))
         (args (if vectorp '(vector start) '(stream)))
         (uintN `(unsigned-byte ,bits))
         (+max-size+ (intern "+MAX-SIZE+"))
         (size (intern "SIZE"))
         (error-message
           (format nil "Too many bytes for ~A, expected ~A bytes max"
                   (ecase bits (32 "int") (64 "long"))
                   +max-size+)))
    `(progn
       (declaim
        (ftype (function ,arg-types (values ,uintN ,size &optional)) ,name))
       (defun ,name ,args
         (loop
           with zigzag of-type ,uintN = 0

           for shift below (* 7 ,+max-size+) by 7
           ,@(when vectorp
               `(for index from start below (+ start ,+max-size+)))
           ;; TODO read-byte can return any integer, so use
           ;; stream-element-type
           for byte = ,(if vectorp '(elt vector index) '(read-byte stream))
           count 1 into count

           do (setf zigzag
                    (logior zigzag
                            (the ,uintN (ash (logand byte #x7f) shift))))
           when (zerop (logand byte #x80))
             return (values zigzag count)

           finally
              (error ,error-message))))))

(defmacro define-write-varint (bits vector/stream)
  (declare ((member 32 64) bits)
           ((member vector stream) vector/stream))
  (let* ((name (intern (format nil "WRITE-VARINT-TO-~A" vector/stream)))
         (vectorp (eq 'vector vector/stream))
         (arg-types (if vectorp '(vector<uint8> ufixnum) '(stream)))
         (args (if vectorp '(vector start) '(stream)))
         (uintN `(unsigned-byte ,bits))
         (+max-size+ (intern "+MAX-SIZE+"))
         (size (intern "SIZE")))
    `(progn
       (declaim
        (ftype (function (,uintN ,@arg-types) (values ,size &optional)) ,name))
       (defun ,name (varint ,@args)
         (loop
           for zigzag of-type ,uintN = varint then (ash zigzag -7)
           ,@(when vectorp
               `(for index from start below (+ start ,+max-size+)))

           until (zerop (logand zigzag (lognot #x7f)))
           for byte = (logior (logand zigzag #x7f) #x80)
           ,@(unless vectorp
               '(count 1 into count))

           do ,(if vectorp
                   '(setf (elt vector index) byte)
                   '(write-byte byte stream))

           finally
           ,@(if vectorp
                 '((setf (elt vector index) (logand zigzag #xff))
                   (return (1+ (- index start))))
                 '((write-byte (logand zigzag #xff) stream)
                   (return (1+ count)))))))))

(defmacro define-read-varints (bits)
  (declare ((member 32 64) bits))
  `(progn
     (define-read-varint ,bits vector)
     (define-read-varint ,bits stream)))

(defmacro define-write-varints (bits)
  (declare ((member 32 64) bits))
  `(progn
     (define-write-varint ,bits vector)
     (define-write-varint ,bits stream)))

(defmacro define-read/write-varint (bits)
  (declare ((member 32 64) bits))
  `(progn
     (define-read-varints ,bits)
     (define-write-varints ,bits)))

;;; serialized-size

(defmacro define-serialized-size (bits)
  (declare ((member 32 64) bits))
  (let ((name (intern "SERIALIZED-SIZE"))
        (to-zigzag (intern "TO-ZIGZAG"))
        (size (intern "SIZE"))
        (uintN `(unsigned-byte ,bits))
        (sintN `(signed-byte ,bits)))
    `(progn
       (declaim (ftype (function (,sintN) (values ,size &optional)) ,name))
       (defun ,name (object)
         (loop
           for zigzag of-type ,uintN = (,to-zigzag object)
             then (ash zigzag -7)

           until (zerop (logand zigzag (lognot #x7f)))
           count zigzag into size

           finally (return (1+ size)))))))

;;; serialize

(defmacro define-serialize-into (bits vector/stream)
  (declare ((member 32 64) bits)
           ((member vector stream) vector/stream))
  (let* ((name (intern (format nil "SERIALIZE-INTO-~A" vector/stream)))
         (sintN `(signed-byte ,bits))
         (size (intern "SIZE"))
         (to-zigzag (intern "TO-ZIGZAG"))
         (write-varint (intern
                        (format nil "WRITE-VARINT-TO-~A" vector/stream)))
         (vectorp (eq 'vector vector/stream))
         (arg-types (if vectorp '(vector<uint8> ufixnum) '(stream)))
         (args (if vectorp '(vector start) '(stream))))
    `(progn
       (declaim
        (ftype (function (,sintN ,@arg-types) (values ,size &optional)) ,name))
       (defun ,name (object ,@args)
         (,write-varint (,to-zigzag object) ,@args)))))

(defmacro define-serialize-intos (bits)
  (declare ((member 32 64) bits))
  `(progn
     (define-serialize-into ,bits vector)
     (define-serialize-into ,bits stream)))

;;; deserialize

(defmacro define-deserialize-from (bits vector/stream)
  (declare ((member 32 64) bits)
           ((member vector stream) vector/stream))
  (let* ((name (intern (format nil "DESERIALIZE-FROM-~A" vector/stream)))
         (sintN `(signed-byte ,bits))
         (size (intern "SIZE"))
         (from-zigzag (intern "FROM-ZIGZAG"))
         (read-varint (intern
                       (format nil "READ-VARINT-FROM-~A" vector/stream)))
         (vectorp (eq 'vector vector/stream))
         (arg-types (if vectorp '(vector<uint8> ufixnum) '(stream)))
         (args (if vectorp '(vector start) '(stream)))
         (return-type `(values ,sintN ,size &optional)))
    `(progn
       (declaim (ftype (function ,arg-types ,return-type) ,name))
       (defun ,name ,args
         (multiple-value-bind (zigzag count)
             (,read-varint ,@args)
           (values (,from-zigzag zigzag) count))))))

(defmacro define-deserialize-froms (bits)
  (declare ((member 32 64) bits))
  `(progn
     (define-deserialize-from ,bits vector)
     (define-deserialize-from ,bits stream)))

;;; compare

(defmacro define-compare (vector/stream)
  (declare ((member vector stream) vector/stream))
  (let* ((name (intern (format nil "COMPARE-~AS" vector/stream)))
         (deserialize (intern
                       (format nil "DESERIALIZE-FROM-~A" vector/stream)))
         (vectorp (eq 'vector vector/stream))
         (arg-types (if vectorp
                        '(vector<uint8> vector<uint8> ufixnum ufixnum)
                        '(stream stream)))
         (args (when vectorp '(left-start right-start)))
         (left-arg (when vectorp (list (car args))))
         (right-arg (when vectorp (cdr args))))
    `(progn
       (declaim
        (ftype (function ,arg-types
                         (values comparison ufixnum ufixnum &optional))
               ,name))
       (defun ,name (left right ,@args)
         (multiple-value-bind (left left-bytes-read)
             (,deserialize left ,@left-arg)
           (multiple-value-bind (right right-bytes-read)
               (,deserialize right ,@right-arg)
             (values (compare-reals left right)
                     left-bytes-read
                     right-bytes-read)))))))

(defmacro define-compares ()
  `(progn
     (define-compare vector)
     (define-compare stream)))

;;; implement

(defmacro implement (bits)
  (declare ((member 32 64) bits))
  `(progn
     (define-size ,bits)
     (define-to/from-zigzag ,bits)
     (define-read/write-varint ,bits)
     (define-serialized-size ,bits)
     (define-serialize-intos ,bits)
     (define-deserialize-froms ,bits)
     (define-compares)))
