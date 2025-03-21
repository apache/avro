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

(defpackage #:org.apache.avro
  (:export #:schema
           #:object
           #:intern
           #:*null-namespace*
           #:schema-of
           #:fingerprint
           #:*default-fingerprint-algorithm*
           #:primitive-schema
           #:primitive-object
           #:boolean
           #:true
           #:false
           #:bytes
           #:double
           #:float
           #:int
           #:long
           #:null
           #:string
           #:complex-schema
           #:complex-object
           #:array
           #:items
           #:array-object
           #:raw
           #:push
           #:pop
           #:map
           #:values
           #:map-object
           #:hash-table-count
           #:hash-table-size
           #:clrhash
           #:maphash
           #:gethash
           #:remhash
           #:union
           #:union-object
           #:schemas
           #:which-one
           #:fixed
           #:fixed-object
           #:size
           #:enum
           #:enum-object
           #:symbols
           #:default
           #:record
           #:*add-accessors-and-initargs-p*
           #:record-object
           #:fields
           #:field
           #:name
           #:aliases
           #:type
           #:order
           #:ascending
           #:descending
           #:ignore
           #:logical-schema
           #:logical-object
           #:uuid
           #:date
           #:year
           #:month
           #:day
           #:time-millis
           #:time-micros
           #:hour
           #:minute
           #:second
           #:millisecond
           #:microsecond
           #:timestamp-millis
           #:timestamp-micros
           #:local-timestamp-millis
           #:local-timestamp-micros
           #:decimal
           #:decimal-object
           #:precision
           #:unscaled
           #:scale
           #:duration
           #:duration-object
           #:months
           #:days
           #:milliseconds
           #:name-return-type
           #:namespace
           #:namespace-return-type
           #:fullname
           #:rpc-error
           #:metadata
           #:undeclared-rpc-error
           #:message
           #:declared-rpc-error
           #:define-error
           #:request
           #:response
           #:errors
           #:one-way
           #:protocol
           #:types
           #:messages
           #:protocol-object
           #:transceiver
           #:stateless-client
           #:stateful-client
           #:send
           #:send-and-receive
           #:sent-handshake-p
           #:server
           #:client-protocol
           #:receive-from-unconnected-client
           #:receive-from-connected-client
           #:serialize
           #:deserialize
           #:serialized-size
           #:magic
           #:meta
           #:sync
           #:file-header
           #:codec
           #:file-block
           #:count
           #:*decompress-deflate*
           #:*decompress-bzip2*
           #:*decompress-snappy*
           #:*decompress-xz*
           #:*decompress-zstandard*
           #:*compress-deflate*
           #:*compress-bzip2*
           #:*compress-snappy*
           #:*compress-xz*
           #:*compress-zstandard*
           #:file-reader
           #:skip-block
           #:read-block
           #:file-writer
           #:write-block
           #:compare
           #:coerce
           #:map<bytes>)
  (:documentation
   "The public interface for avro, all other packages are internal details.

Avro specifies schemas, serialization, and protocols:

  * schemas define behavior of values

  * serialization defines a structure for values according to their schema

  * protocols define the transport of values

These concepts map intuitively to the type and CLOS system, which this
implementation utilizes as follows:

  * primitive schemas are deftypes, whose objects adhere to those types

  * complex and logical schemas are classes, whose objects are instances of
    those classes

  * protocols, like complex/logical schemas, are classes whose objects define
    transport and stateful/stateless specific details

  * serialization is determined by the class or type of the specific object

Each specific complex and logical schema, as well as each protocol, has a
common structure with each of the instances of its kind. This implementation
captures these commonalities with the CLOS metaobject protocol as follows:

  * each definition of a complex schema is an instance of the COMPLEX-SCHEMA
    class metaobject...or more specifically, an instance of one of its
    subclasses, like ARRAY, MAP, RECORD, etc.

  * each definition of a logical schema is an instance of the LOGICAL-SCHEMA
    class metaobject...or more specifically, an instance of one of its
    subclasses, like DECIMAL, DURATION, DATE, etc.

  * each definition of a protocol is an instance of the PROTOCOL class
    metaobject

With that structure in mind, there are two broad api layers this implementation
provides to create schemas and protocols:

  * the object layer, which utilizes mop to define schemas and protocols

  * the json layer, which parses json before using the prior layer to define
    schemas and protocols

As an example, each of these create the same schema:

  * (DESERIALIZE 'SCHEMA \"{
       \"type\": \"array\",
       \"items\": \"int\"
     }\")

  * (make-instance 'ARRAY :items 'INT)

  * (defclass array<int> ()
      ()
      (:metaclass ARRAY)
      (:items INT))

The bulk of this package follows this general structure. For a higher-level
interface that most users are expected to use with asdf, see the
ORG.APACHE.AVRO/ASDF system."))

(in-package #:org.apache.avro)

(cl:defgeneric coerce (object schema)
  (:documentation
   "Use Avro Schema Resolution to coerce OBJECT into SCHEMA.

OBJECT may be recursively mutated."))

(cl:defgeneric compare (schema left right cl:&key cl:&allow-other-keys)
  (:documentation
   "Return 0, -1, or 1 if LEFT is equal to, less than, or greater than RIGHT.

LEFT and RIGHT should be avro serialized data.

LEFT and RIGHT may not necessarily be fully consumed."))

(cl:defgeneric codec (instance))

(cl:defgeneric schema (instance))

(cl:defgeneric intern (instance cl:&key null-namespace)
  (:documentation
   "Intern INSTANCE in the package deduced from its namespace.

The package will be created if necessary and if the null namespace is deduced,
the package described by NULL-NAMESPACE will be used. If not specified,
NULL-NAMESPACE defaults to *NULL-NAMESPACE*."))

(cl:defgeneric serialized-size (object)
  (:documentation
   "Returns the serialized size of OBJECT in octets."))

(cl:defgeneric deserialize (schema input cl:&key cl:&allow-other-keys)
  (:documentation
   "Deserialize INPUT according to SCHEMA.

If SCHEMA is eql to the symbol SCHEMA or PROTOCOL, then INPUT will be
interpreted as its json representation. Otherwise, INPUT will be interpreted as
the octets of an OBJECT adhering to SCHEMA. SCHEMA may also be eql to
FINGERPRINT, indicating that INPUT is in single object encoding form. In this
latter case, only the fingerprint from INPUT is deserialized.

INPUT may be a stream or vector. If INPUT is a vector, then :START indicates
where to start deserializing from."))

(cl:defgeneric serialize (object cl:&key cl:&allow-other-keys)
  (:documentation
   "Serialize OBJECT, which may be an avro SCHEMA, PROTOCOL, or OBJECT.

If OBJECT is an avro SCHEMA or PROTOCOL, then the json representation of OBJECT
is serialized. Otherwise, if OBJECT is an avro OBJECT, then it's serialized
according to its avro SCHEMA.

:INTO can be a vector or stream, defaulting to a vector if not provided. :INTO
should accept characters for avro SCHEMAs or PROTOCOLs, and octets for avro
OBJECTs. If :INTO is a vector, then :START specifies where to start storing the
output.

For avro SCHEMAs or PROTOCOLs, :CANONICAL-FORM-P will determine if the json
output is in avro canonical form.

For avro OBJECTs, :SINGLE-OBJECT-ENCODING-P will determine if the octet output
adheres to avro single object encoding."))

(cl:defgeneric one-way (message))

(cl:defgeneric errors (message))

(cl:defgeneric namespace (named-class))

(cl:defgeneric name (named-class))

(cl:defgeneric milliseconds (object))

(cl:defgeneric days (object))

(cl:defgeneric months (object))

(cl:defgeneric scale (schema))

(cl:defgeneric microsecond (object cl:&key cl:&allow-other-keys))

(cl:defgeneric millisecond (object cl:&key cl:&allow-other-keys))

(cl:defgeneric second (object cl:&key cl:&allow-other-keys)
  (:documentation "Return (values second remainder)."))

(cl:defgeneric minute (object cl:&key cl:&allow-other-keys))

(cl:defgeneric hour (object cl:&key cl:&allow-other-keys))

(cl:defgeneric day (object cl:&key cl:&allow-other-keys))

(cl:defgeneric month (object cl:&key cl:&allow-other-keys))

(cl:defgeneric year (object cl:&key cl:&allow-other-keys))

(cl:defgeneric schema-of (object)
  (:documentation
   "Return the schema of OBJECT.

The set of schemas (almost) disjointly partitions the set of objects: that is
to say, no two schemas can describe the same object. This is only false for
the INT and LONG schemas: when called with an integer OBJECT, SCHEMA-OF will
return INT if OBJECT can be described as a (signed-byte 32). In other words,
LONG is a superset of INT."))

(cl:defgeneric push (element array))

(cl:defgeneric pop (array))

(cl:defgeneric hash-table-count (map))

(cl:defgeneric hash-table-size (map))

(cl:defgeneric clrhash (map))

(cl:defgeneric maphash (function map))

(cl:defgeneric gethash (key map cl:&optional default))

(cl:defgeneric (cl:setf gethash) (value key map))

(cl:defgeneric remhash (key map))

(cl:defgeneric which-one (object))

(cl:defgeneric default (object))

(cl:defgeneric fields (object))

(cl:defgeneric order (object))

(cl:defgeneric type (object))

(cl:in-package #:cl-user)
