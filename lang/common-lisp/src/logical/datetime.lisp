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
(defpackage #:org.apache.avro.internal.logical.datetime
  (:use #:cl)
  (:local-nicknames
   (#:api #:org.apache.avro))
  (:import-from #:alexandria
                #:define-constant)
  (:export #:unix-epoch
           #:+utc-unix-epoch+
           #:local-timezone
           #:local-date
           #:local-hour-minute
           #:local-second-millis
           #:local-second-micros
           #:global-date
           #:global-hour-minute
           #:global-second-millis
           #:global-second-micros
           #:adjust-for-daylight-savings
           #:local-unix-epoch-diff))
(in-package #:org.apache.avro.internal.logical.datetime)

;;; unix-epoch

(eval-when (:compile-toplevel :load-toplevel :execute)
  (declaim
   (ftype (function (local-time::timezone)
                    (values local-time:timestamp &optional))
          unix-epoch))
  (defun unix-epoch (timezone)
    "Return 1970-01-01T00:00:00 in TIMEZONE."
    (local-time:encode-timestamp 0 0 0 0 1 1 1970 :timezone timezone)))

;;; +utc-unix-epoch+

(declaim (local-time:timestamp +utc-unix-epoch+))
(define-constant +utc-unix-epoch+
    (unix-epoch local-time:+utc-zone+)
  :test #'local-time:timestamp=)

;;; local-timezone

(defclass local-timezone ()
  ((local-timezone
    :type local-time::timezone
    :reader local-timezone
    :initform local-time:*default-timezone*)))

;;; local-date

(defclass local-date (local-timezone local-time:timestamp)
  ())

(defmethod api:year
    ((object local-date) &key)
  "Return year of OBJECT in OBJECT's local timezone, defaulting to
local-time:*default-timezone*."
  (local-time:timestamp-year object :timezone (local-timezone object)))

(defmethod api:month
    ((object local-date) &key)
  "Return the month of OBJECT in OBJECT's local timezone, defaulting to
local-time:*default-timezone*."
  (local-time:timestamp-month object :timezone (local-timezone object)))

(defmethod api:day
    ((object local-date) &key)
  "Return the day of OBJECT in OBJECT's local timezone, defaulting to
local-time:*default-timezone*."
  (local-time:timestamp-day object :timezone (local-timezone object)))

;;; local-hour-minute

(defclass local-hour-minute (local-timezone local-time:timestamp)
  ())

(defmethod api:hour
    ((object local-hour-minute) &key)
  "Return the hour of OBJECT in OBJECT's local timezone, defaulting to
local-time:*default-timezone*."
  (local-time:timestamp-hour object :timezone (local-timezone object)))

(defmethod api:minute
    ((object local-hour-minute) &key)
  "Return the minute of OBJECT in OBJECT's local timezone, defaulting to
local-time:*default-timezone*."
  (local-time:timestamp-minute object :timezone (local-timezone object)))

;;; local-second-millis

(defclass local-second-millis (local-timezone local-time:timestamp)
  ())

(defmethod api:second
    ((object local-second-millis) &key)
  "Return seconds of OBJECT in OBJECT's local timezone, defaulting to
local-time:*default-timezone*."
  (let ((second (local-time:timestamp-second
                 object :timezone (local-timezone object)))
        (millisecond (local-time:timestamp-millisecond object)))
    (declare ((mod 60) second)
             ((mod 1000) millisecond))
    (values second (/ millisecond 1000))))

(defmethod api:millisecond
    ((object local-second-millis) &key)
  "Return milliseconds of OBJECT in OBJECT's local timezone, defaulting to
local-time:*default-timezone*."
  (let ((second (local-time:timestamp-second
                 object :timezone (local-timezone object)))
        (millisecond (local-time:timestamp-millisecond object)))
    (declare ((mod 60) second)
             ((mod 1000) millisecond))
    (+ (* second 1000) millisecond)))

;;; local-second-micros

(defclass local-second-micros (local-timezone local-time:timestamp)
  ())

(defmethod api:second
    ((object local-second-micros) &key)
  "Return seconds of OBJECT in OBJECT'S local timezone, defaulting to
local-time:*default-timezone*."
  (let ((second (local-time:timestamp-second
                 object :timezone (local-timezone object)))
        (microsecond (local-time:timestamp-microsecond object)))
    (declare ((mod 60) second)
             ((mod 1000000) microsecond))
    (values second (/ microsecond (* 1000 1000)))))

(defmethod api:microsecond
    ((object local-second-micros) &key)
  "Return microseconds of OBJECT in OBJECT's local timezone, defaulting to
local-time:*default-timezone*."
  (let ((second (local-time:timestamp-second
                 object :timezone (local-timezone object)))
        (microsecond (local-time:timestamp-microsecond object)))
    (declare ((mod 60) second)
             ((mod 1000000) microsecond))
    (+ (* second 1000 1000) microsecond)))

;;; global-date

(defclass global-date (local-time:timestamp)
  ())

(defmethod api:year
    ((object global-date) &key (timezone local-time:*default-timezone*))
  "Return the year of OBJECT in TIMEZONE, defaulting to
local-time:*default-timezone*."
  (local-time:timestamp-year object :timezone timezone))

(defmethod api:month
    ((object global-date) &key (timezone local-time:*default-timezone*))
  "Return the month of OBJECT in TIMEZONE, defaulting to
local-time:*default-timezone*."
  (local-time:timestamp-month object :timezone timezone))

(defmethod api:day
    ((object global-date) &key (timezone local-time:*default-timezone*))
  "Return the day of OBJECT in TIMEZONE, defaulting to
local-time:*default-timezone*."
  (local-time:timestamp-day object :timezone timezone))

;;; global-hour-minute

(defclass global-hour-minute (local-time:timestamp)
  ())

(defmethod api:hour
    ((object global-hour-minute) &key (timezone local-time:*default-timezone*))
  "Return the hour of OBJECT in TIMEZONE, defaulting to
local-time:*default-timezone*."
  (local-time:timestamp-hour object :timezone timezone))

(defmethod api:minute
    ((object global-hour-minute) &key (timezone local-time:*default-timezone*))
  "Return the minute of OBJECT in TIMEZONE, defaulting to
local-time:*default-timezone*."
  (local-time:timestamp-minute object :timezone timezone))

;;; global-second-millis

(defclass global-second-millis (local-time:timestamp)
  ())

(defmethod api:second
    ((object global-second-millis)
     &key (timezone local-time:*default-timezone*))
  "Return seconds of OBJECT in TIMEZONE, defaulting to
local-time:*default-timezone*."
  (let ((second (local-time:timestamp-second object :timezone timezone))
        (millisecond (local-time:timestamp-millisecond object)))
    (declare ((mod 60) second)
             ((mod 1000) millisecond))
    (values second (/ millisecond 1000))))

(defmethod api:millisecond
    ((object global-second-millis)
     &key (timezone local-time:*default-timezone*))
  "Return milliseconds of OBJECT in TIMEZONE, defaulting to
local-time:*default-timezone*."
  (let ((second (local-time:timestamp-second object :timezone timezone))
        (millisecond (local-time:timestamp-millisecond object)))
    (declare ((mod 60) second)
             ((mod 1000) millisecond))
    (+ (* second 1000) millisecond)))

;;; global-second-micros

(defclass global-second-micros (local-time:timestamp)
  ())

(defmethod api:second
    ((object global-second-micros)
     &key (timezone local-time:*default-timezone*))
  "Return seconds of OBJECT in TIMEZONE, defaulting to
local-time:*default-timezone*."
  (let ((second (local-time:timestamp-second object :timezone timezone))
        (microsecond (local-time:timestamp-microsecond object)))
    (declare ((mod 60) second)
             ((mod 1000000) microsecond))
    (values second (/ microsecond (* 1000 1000)))))

(defmethod api:microsecond
    ((object global-second-micros)
     &key (timezone local-time:*default-timezone*))
  "Return microseconds of OBJECT in TIMEZONE, defaulting to
local-time:*default-timezone*."
  (let ((second (local-time:timestamp-second object :timezone timezone))
        (microsecond (local-time:timestamp-microsecond object)))
    (declare ((mod 60) second)
             ((mod 1000000) microsecond))
    (+ (* second 1000 1000) microsecond)))

;;; local daylight savings

(declaim
 (ftype (function (local-time:timestamp local-time::timezone)
                  (values boolean &optional))
        daylight-savings-p))
(defun daylight-savings-p (timestamp timezone)
  (nth-value 1 (local-time:timestamp-subtimezone timestamp timezone)))

(declaim
 (ftype (function (local-time:timestamp) (values &optional))
        adjust-for-daylight-savings))
(defun adjust-for-daylight-savings (timestamp)
  (when (daylight-savings-p timestamp local-time:*default-timezone*)
    (local-time:adjust-timestamp!
        timestamp
      (offset :hour -1)))
  (values))

(declaim
 (ftype (function ((and local-timezone local-time:timestamp))
                  (values local-time-duration:duration &optional))
        local-unix-epoch-diff))
(defun local-unix-epoch-diff (local-timestamp)
  (let* ((timezone (local-timezone local-timestamp))
         (unix-epoch (unix-epoch timezone))
         (local-timestamp
           (if (not (daylight-savings-p local-timestamp timezone))
               local-timestamp
               (local-time:adjust-timestamp
                   local-timestamp
                 (offset :hour 1)))))
    (local-time-duration:timestamp-difference local-timestamp unix-epoch)))
