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
(defpackage #:org.apache.avro.internal.ipc.framing
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro)
   (#:internal #:org.apache.avro.internal))
  (:import-from #:org.apache.avro.internal.type
                #:uint8
                #:uint32
                #:ufixnum
                #:array<uint8>
                #:vector<uint8>)
  (:export #:frame
           #:to-input-stream
           #:input-stream
           #:buffer-object
           #:buffers
           #:buffer))
(in-package #:org.apache.avro.internal.ipc.framing)

;;; buffer

(declaim (uint32 +buffer-size+))
(defconstant +buffer-size+ (* 8 1024))

(declaim (uint32 +max-object-size+))
(defconstant +max-object-size+ (- +buffer-size+ 4))

(deftype buffer-size ()
  `(integer 0 ,+max-object-size+))

(deftype buffer ()
  'array<uint8>)

(deftype buffers ()
  '(simple-array buffer (*)))

;;; frame

(declaim
 (ftype (function (buffer-size) (values buffer &optional)) make-buffer))
(defun make-buffer (size)
  (loop
    with buffer = (make-array (+ 4 size) :element-type 'uint8)

    for i below 4
    for shift = 24 then (- shift 8)
    for byte = (logand #xff (ash size (- shift)))

    do (setf (elt buffer i) byte)

    finally
       (return
         buffer)))

(declaim
 (ftype (function (api:object) (values buffer &optional)) buffer-object))
(defun buffer-object (object)
  (let ((buffer (make-buffer (api:serialized-size object))))
    (api:serialize object :into buffer :start 4)
    buffer))

(deftype object-sizes ()
  '(simple-array fixnum (*)))

(declaim
 (ftype (function (object-sizes &optional ufixnum) (values buffers &optional))
        allocate-buffers))
(defun allocate-buffers (object-sizes &optional (prefix-pad 0))
  (loop
    with total-object-size = (reduce #'+ object-sizes :initial-value 0)
    with (filled-buffers remaining-bytes) = (multiple-value-list
                                             (truncate total-object-size
                                                       +max-object-size+))
    with buffers = (make-array (if (zerop remaining-bytes)
                                   (+ filled-buffers 1 prefix-pad)
                                   (+ filled-buffers 2 prefix-pad)))
      initially
         (let ((index (+ filled-buffers prefix-pad)))
           (if (zerop remaining-bytes)
               (setf (elt buffers index) (make-buffer 0))
               (setf (elt buffers index) (make-buffer remaining-bytes)
                     (elt buffers (1+ index)) (make-buffer 0))))

    for i from prefix-pad below filled-buffers
    do (setf (elt buffers i) (make-buffer +max-object-size+))

    finally
       (return
         buffers)))

(defclass output-stream (trivial-gray-streams:fundamental-binary-output-stream
                         trivial-gray-streams:trivial-gray-stream-mixin)
  ((buffers
    :initarg :buffers
    :reader buffers
    :type buffers)
   (buffers-index
    :accessor outer-index
    :type ufixnum)
   (buffer-index
    :accessor inner-index
    :type ufixnum))
  (:default-initargs
   :buffers (error "Must supply BUFFERS")))

(defmethod initialize-instance :after
    ((instance output-stream) &key (start 0))
  (with-accessors
        ((outer-index outer-index)
         (inner-index inner-index))
      instance
    (setf outer-index start
          inner-index 4)))

(defmethod stream-element-type
    ((instance output-stream))
  'uint8)

(defmethod trivial-gray-streams:stream-write-byte
    ((instance output-stream) (byte fixnum))
  (declare (uint8 byte))
  (with-accessors
        ((buffers buffers)
         (outer-index outer-index)
         (inner-index inner-index))
      instance
    (tagbody
     begin
       (if (= outer-index (length buffers))
           (error 'end-of-file :stream *error-output*)
           (let ((buffer (elt buffers outer-index)))
             (when (= inner-index (length buffer))
               (incf outer-index)
               (setf inner-index 4)
               (go begin))
             (setf (elt buffer inner-index) byte)
             (incf inner-index)))))
  byte)

(defmethod trivial-gray-streams:stream-write-sequence
    ((stream output-stream)
     (vector simple-array)
     (start fixnum)
     (end fixnum)
     &key)
  (declare (buffer vector)
           (ufixnum start end))
  (with-accessors
        ((buffers buffers)
         (outer-index outer-index)
         (inner-index inner-index))
      stream
    (loop
      until (or (>= start end)
                (= outer-index (length buffers)))
      for buffer = (elt buffers outer-index)

      if (= inner-index (length buffer)) do
        (incf outer-index)
        (setf inner-index 4)
      else do
        (let* ((needed (- end start))
               (remaining (- (length buffer) inner-index))
               (count (min needed remaining)))
          (replace buffer vector :start1 inner-index :start2 start :end2 end)
          (incf inner-index count)
          (incf start count))

      finally
         (return
           vector))))

(declaim (ftype (function (list output-stream) (values &optional)) frame-into))
(defun frame-into (objects output-stream)
  (flet ((serialize (object)
           (api:serialize object :into output-stream)))
    (map nil #'serialize objects))
  (values))

(declaim
 (ftype (function (list) (values object-sizes &optional)) object-sizes))
(defun object-sizes (objects)
  (map 'object-sizes #'api:serialized-size objects))

(declaim
 (ftype (function (internal:request list) (values buffers &optional))
        frame-with-handshake))
(defun frame-with-handshake (handshake objects)
  (let* ((object-sizes (object-sizes objects))
         (buffers (allocate-buffers object-sizes 1)))
    (frame-into objects (make-instance
                         'output-stream :buffers buffers :start 1))
    (setf (elt buffers 0) (buffer-object handshake))
    buffers))

(declaim
 (ftype (function (&rest api:object) (values buffers &optional)) frame))
(defun frame (&rest objects)
  (if (typep (first objects) 'internal:request)
      (frame-with-handshake (first objects) (rest objects))
      (let* ((object-sizes (object-sizes objects))
             (buffers (allocate-buffers object-sizes)))
        (frame-into objects (make-instance 'output-stream :buffers buffers))
        buffers)))

;;; to-input-stream

(defclass input-stream (trivial-gray-streams:fundamental-binary-input-stream
                        trivial-gray-streams:trivial-gray-stream-mixin)
  ((buffers
    :initarg :buffers
    :reader buffers
    :type (vector vector<uint8>))
   (buffers-index
    :accessor outer-index
    :type ufixnum)
   (buffer-index
    :accessor inner-index
    :type ufixnum))
  (:default-initargs
   :buffers (error "Must supply BUFFERS")))

(defmethod initialize-instance :after
    ((instance input-stream) &key)
  (with-accessors
        ((outer-index outer-index)
         (inner-index inner-index))
      instance
    (setf outer-index 0
          inner-index 0)))

(defmethod stream-element-type
    ((instance input-stream))
  'uint8)

(defmethod trivial-gray-streams:stream-read-byte
    ((instance input-stream))
  (with-accessors
        ((buffers buffers)
         (outer-index outer-index)
         (inner-index inner-index))
      instance
    (tagbody
     begin
       (if (= outer-index (length buffers))
           (return-from trivial-gray-streams:stream-read-byte :eof)
           (let ((buffer (elt buffers outer-index)))
             (when (= inner-index (length buffer))
               (incf outer-index)
               (setf inner-index 0)
               (go begin))
             (let ((byte (elt buffer inner-index)))
               (incf inner-index)
               (return-from trivial-gray-streams:stream-read-byte byte)))))))

(defmethod trivial-gray-streams:stream-read-sequence
    ((stream input-stream)
     (vector simple-array)
     (start fixnum)
     (end fixnum)
     &key)
  (declare (buffer vector)
           (ufixnum start end))
  (with-accessors
        ((buffers buffers)
         (outer-index outer-index)
         (inner-index inner-index))
      stream
    (loop
      until (or (>= start end)
                (= outer-index (length buffers)))
      for buffer = (elt buffers outer-index)

      if (= inner-index (length buffer)) do
        (incf outer-index)
        (setf inner-index 0)
      else do
        (let* ((needed (- end start))
               (remaining (- (length buffer) inner-index))
               (count (min needed remaining)))
          (replace vector buffer :start1 start :start2 inner-index :end1 end)
          (incf inner-index count)
          (incf start count))

      finally
         (return
           start))))

(declaim
 (ftype (function (buffer &optional ufixnum) (values uint32 &optional))
        parse-big-endian))
(defun parse-big-endian (buffer &optional (start 0))
  (loop
    with integer = 0

    repeat 4
    for i = start then (1+ i)
    for byte = (elt buffer i)
    for shift = 24 then (- shift 8)

    do (setf integer (logior integer (ash byte shift)))

    finally
       (return
         integer)))

(declaim
 (ftype (function (stream (array<uint8> 4)) (values uint32 &optional))
        read-size))
(defun read-size (stream buffer)
  (unless (= (read-sequence buffer stream) 4)
    (error 'end-of-file :stream *error-output*))
  (parse-big-endian buffer))

(declaim
 (ftype (function (stream) (values input-stream &optional))
        stream->input-stream))
(defun stream->input-stream (stream)
  (loop
    with size-buffer = (make-array 4 :element-type 'uint8)
    and buffers = (make-array 0 :element-type 'buffer :adjustable t
                                :fill-pointer t)

    for size = (read-size stream size-buffer)
    until (zerop size)

    for buffer = (make-array size :element-type 'uint8)

    if (= (read-sequence buffer stream) size) do
      (vector-push-extend buffer buffers)
    else do
      (error 'end-of-file :stream *error-output*)

    finally
       (return
         (make-instance 'input-stream :buffers buffers))))

(declaim
 (ftype (function (vector<uint8>) (values input-stream &optional))
        bytes->input-stream))
(defun bytes->input-stream (bytes)
  (loop
    with index = 0
    and buffers = (make-array 0 :element-type 'vector<uint8>
                                :adjustable t :fill-pointer t)

    for size = (prog1 (parse-big-endian bytes index)
                 (incf index 4))
    until (zerop size)

    for slice = (prog1 (make-array size
                                   :element-type 'uint8
                                   :displaced-to bytes
                                   :displaced-index-offset index)
                  (incf index size))

    do (vector-push-extend slice buffers)

    finally
       (return
         (make-instance 'input-stream :buffers buffers))))

(declaim
 (ftype (function ((or stream vector<uint8>)) (values input-stream &optional))
        to-input-stream))
(defun to-input-stream (input)
  (if (streamp input)
      (stream->input-stream input)
      (bytes->input-stream input)))
