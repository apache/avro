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
(defpackage #:org.apache.avro/test/record
  (:use #:cl #:1am)
  (:local-nicknames
   (#:avro #:org.apache.avro)
   (#:internal #:org.apache.avro.internal))
  (:import-from #:org.apache.avro/test/common
                #:define-schema-test
                #:json-syntax
                #:define-io-test))
(in-package #:org.apache.avro/test/record)

(named-readtables:in-readtable json-syntax)

(define-schema-test python-test
  {
    "type": "record",
    "name": "Test",
    "fields": [
      {
        "name": "f",
        "type": "long"
      }
    ]
  }
  {
    "name": "Test",
    "type": "record",
    "fields": [
      {
        "name": "f",
        "type": "long"
      }
    ]
  }
  #x8e58ebe6f5e594ed
  (make-instance
   'avro:record
   :name "Test"
   :direct-slots '((:name |f| :type avro:long)))
  (defclass |Test| ()
    ((|f| :type avro:long))
    (:metaclass avro:record)))

(define-schema-test python-node
  {
    "type": "record",
    "name": "Node",
    "fields": [
      {
        "name": "label",
        "type": "string"
      },
      {
        "name": "children",
        "type": {
          "type": "array",
          "items": "Node"
        }
      }
    ]
  }
  {
    "name": "Node",
    "type": "record",
    "fields": [
      {
        "name": "label",
        "type": "string"
      },
      {
        "name": "children",
        "type": {
          "type": "array",
          "items": "Node"
        }
      }
    ]
  }
  #xb756e7c344a5cb52
  (closer-mop:ensure-class
   '|Node|
   :metaclass 'avro:record
   :direct-slots
   `((:name |label| :type avro:string)
     (:name |children|
      :type ,(closer-mop:ensure-class
              'array<node>
              :metaclass 'avro:array
              :items '|Node|))))
  (defclass node_array ()
    ()
    (:metaclass avro:array)
    (:items |Node|))
  (defclass |Node| ()
    ((|label| :type avro:string)
     (|children| :type node_array))
    (:metaclass avro:record)))

(define-schema-test python-lisp
  {
    "type": "record",
    "name": "Lisp",
    "fields": [
      {
        "name": "value",
        "type": [
          "null",
          "string",
          {
            "type": "record",
            "name": "Cons",
            "fields": [
              {
                "name": "car",
                "type": "Lisp"
              },
              {
                "name": "cdr",
                "type": "Lisp"
              }
            ]
          }
        ]
      }
    ]
  }
  {
    "name": "Lisp",
    "type": "record",
    "fields": [
      {
        "name": "value",
        "type": [
          "null",
          "string",
          {
            "name": "Cons",
            "type": "record",
            "fields": [
              {
                "name": "car",
                "type": "Lisp"
              },
              {
                "name": "cdr",
                "type": "Lisp"
              }
            ]
          }
        ]
      }
    ]
  }
  #x06b3a0ed231ad968
  (closer-mop:ensure-class
   '|Lisp|
   :metaclass 'avro:record
   :direct-slots
   `((:name |value|
      :type ,(make-instance
              'avro:union
              :schemas (list
                        'avro:null
                        'avro:string
                        (make-instance
                         'avro:record
                         :name "Cons"
                         :direct-slots
                         `((:name |car| :type |Lisp|)
                           (:name |cdr| :type |Lisp|))))))))
  (defclass |Cons| ()
    ((|car| :type |Lisp|)
     (|cdr| :type |Lisp|))
    (:metaclass avro:record))
  (defclass union<null-string-cons> ()
    ()
    (:metaclass avro:union)
    (:schemas avro:null avro:string |Cons|))
  (defclass |Lisp| ()
    ((|value| :type union<null-string-cons>))
    (:metaclass avro:record)))

(define-schema-test python-handshake-request
  {
    "type": "record",
    "name": "HandshakeRequest",
    "namespace": "org.apache.avro.ipc",
    "fields": [
      {
        "name": "clientHash",
        "type": {
          "type": "fixed",
          "name": "MD5",
          "size": 16
        }
      },
      {
        "name": "clientProtocol",
        "type": [
          "null",
          "string"
        ]
      },
      {
        "name": "serverHash",
        "type": "MD5"
      },
      {
        "name": "meta",
        "type": [
          "null",
          {
            "type": "map",
            "values": "bytes"
          }
        ]
      }
    ]
  }
  {
    "name": "org.apache.avro.ipc.HandshakeRequest",
    "type": "record",
    "fields": [
      {
        "name": "clientHash",
        "type": {
          "name": "org.apache.avro.ipc.MD5",
          "type": "fixed",
          "size": 16
        }
      },
      {
        "name": "clientProtocol",
        "type": [
          "null",
          "string"
        ]
      },
      {
        "name": "serverHash",
        "type": "org.apache.avro.ipc.MD5"
      },
      {
        "name": "meta",
        "type": [
          "null",
          {
            "type": "map",
            "values": "bytes"
          }
        ]
      }
    ]
  }
  #x57577c5a9ed76ab9
  (make-instance
   'avro:record
   :name "HandshakeRequest"
   :namespace "org.apache.avro.ipc"
   :direct-slots
   `((:name |clientHash|
      :type ,(closer-mop:ensure-class 'MD5 :metaclass 'avro:fixed :size 16))
     (:name |clientProtocol|
      :type ,(make-instance 'avro:union :schemas '(avro:null avro:string)))
     (:name |serverHash|
      :type MD5)
     (:name |meta|
      :type ,(make-instance
              'avro:union
              :schemas (list 'avro:null (make-instance
                                         'avro:map :values 'avro:bytes))))))
  (defclass MD5 ()
    ()
    (:metaclass avro:fixed)
    (:size 16)
    (:enclosing-namespace "org.apache.avro.ipc")) ;; TODO should this be necessary?
  (defclass union<null-string> ()
    ()
    (:metaclass avro:union)
    (:schemas avro:null avro:string))
  (defclass map<bytes> ()
    ()
    (:metaclass avro:map)
    (:values avro:bytes))
  (defclass union<null-map<bytes>> ()
    ()
    (:metaclass avro:union)
    (:schemas avro:null map<bytes>))
  (defclass |HandshakeRequest| ()
    ((|clientHash| :type MD5)
     (|clientProtocol| :type union<null-string>)
     (|serverHash| :type MD5)
     (|meta| :type union<null-map<bytes>>))
    (:metaclass avro:record)
    (:namespace "org.apache.avro.ipc")))

(define-schema-test python-handshake-response
  {
    "type": "record",
    "name": "HandshakeResponse",
    "namespace": "org.apache.avro.ipc",
    "fields": [
      {
        "name": "match",
        "type": {
          "type": "enum",
          "name": "HandshakeMatch",
          "symbols": [
            "BOTH",
            "CLIENT",
            "NONE"
          ]
        }
      },
      {
        "name": "serverProtocol",
        "type": [
          "null",
          "string"
        ]
      },
      {
        "name": "serverHash",
        "type": [
          "null",
          {
            "name": "MD5",
            "size": 16,
            "type": "fixed"
          }
        ]
      },
      {
        "name": "meta",
        "type": [
          "null",
          {
            "type": "map",
            "values": "bytes"
          }
        ]
      }
    ]
  }
  {
    "name": "org.apache.avro.ipc.HandshakeResponse",
    "type": "record",
    "fields": [
      {
        "name": "match",
        "type": {
          "name": "org.apache.avro.ipc.HandshakeMatch",
          "type": "enum",
          "symbols": [
            "BOTH",
            "CLIENT",
            "NONE"
          ]
        }
      },
      {
        "name": "serverProtocol",
        "type": [
          "null",
          "string"
        ]
      },
      {
        "name": "serverHash",
        "type": [
          "null",
          {
            "name": "org.apache.avro.ipc.MD5",
            "type": "fixed",
            "size": 16
          }
        ]
      },
      {
        "name": "meta",
        "type": [
          "null",
          {
            "type": "map",
            "values": "bytes"
          }
        ]
      }
    ]
  }
  #x0ea54ede01eefe00
  (make-instance
   'avro:record
   :name '|HandshakeResponse|
   :namespace "org.apache.avro.ipc"
   :direct-slots
   `((:name |match|
      :type ,(make-instance
              'avro:enum
              :name "HandshakeMatch"
              :symbols '("BOTH" "CLIENT" "NONE")
              ;; TODO having to set enclosing-namespace kinda sucks
              ;; maybe canonicalize should handle this
              ;; or rather, make-instance should handle this
              ;; then the schema passed in won't necessarily be eq to schema
              ;; returned (not a big deal)
              :enclosing-namespace "org.apache.avro.ipc"))
     (:name |serverProtocol|
      :type ,(make-instance 'avro:union :schemas '(avro:null avro:string)))
     (:name |serverHash|
      :type ,(make-instance
              'avro:union
              :schemas (list 'avro:null
                             (make-instance
                              'avro:fixed
                              :name 'MD5
                              :size 16
                              :enclosing-namespace "org.apache.avro.ipc"))))
     (:name |meta|
      :type ,(make-instance
              'avro:union
              :schemas (list 'avro:null
                             (make-instance 'avro:map :values 'avro:bytes))))))
  (defclass |HandshakeMatch| ()
    ()
    (:metaclass avro:enum)
    (:symbols "BOTH" "CLIENT" "NONE")
    (:enclosing-namespace "org.apache.avro.ipc"))
  (defclass union<null-string> ()
    ()
    (:metaclass avro:union)
    (:schemas avro:null avro:string))
  (defclass MD5 ()
    ()
    (:metaclass avro:fixed)
    (:size 16)
    (:enclosing-namespace "org.apache.avro.ipc"))
  (defclass union<null-MD5> ()
    ()
    (:metaclass avro:union)
    (:schemas avro:null MD5))
  (defclass map<bytes> ()
    ()
    (:metaclass avro:map)
    (:values avro:bytes))
  (defclass union<null-map<bytes>> ()
    ()
    (:metaclass avro:union)
    (:schemas avro:null map<bytes>))
  (defclass |HandshakeResponse| ()
    ((|match| :type |HandshakeMatch|)
     (|serverProtocol| :type union<null-string>)
     (|serverHash| :type union<null-MD5>)
     (|meta| :type union<null-map<bytes>>))
    (:metaclass avro:record)
    (:namespace "org.apache.avro.ipc")))

(define-schema-test python-interop
  {
    "type": "record",
    "name": "Interop",
    "namespace": "org.apache.avro",
    "fields": [
      {
        "name": "intField",
        "type": "int"
      },
      {
        "name": "longField",
        "type": "long"
      },
      {
        "name": "stringField",
        "type": "string"
      },
      {
        "name": "boolField",
        "type": "boolean"
      },
      {
        "name": "floatField",
        "type": "float"
      },
      {
        "name": "doubleField",
        "type": "double"
      },
      {
        "name": "bytesField",
        "type": "bytes"
      },
      {
        "name": "nullField",
        "type": "null"
      },
      {
        "name": "arrayField",
        "type": {
          "type": "array",
          "items": "double"
        }
      },
      {
        "name": "mapField",
        "type": {
          "type": "map",
          "values": {
            "name": "Foo",
            "type": "record",
            "fields": [
              {
                "name": "label",
                "type": "string"
              }
            ]
          }
        }
      },
      {
        "name": "unionField",
        "type": [
          "boolean",
          "double",
          {
            "type": "array",
            "items": "bytes"
          }
        ]
      },
      {
        "name": "enumField",
        "type": {
          "type": "enum",
          "name": "Kind",
          "symbols": [
            "A",
            "B",
            "C"
          ]
        }
      },
      {
        "name": "fixedField",
        "type": {
          "type": "fixed",
          "name": "MD5",
          "size": 16
        }
      },
      {
        "name": "recordField",
        "type": {
          "type": "record",
          "name": "Node",
          "fields": [
            {
              "name": "label",
              "type": "string"
            },
            {
              "name": "children",
              "type": {
                "type": "array",
                "items": "Node"
              }
            }
          ]
        }
      }
    ]
  }
  {
    "name": "org.apache.avro.Interop",
    "type": "record",
    "fields": [
      {
        "name": "intField",
        "type": "int"
      },
      {
        "name": "longField",
        "type": "long"
      },
      {
        "name": "stringField",
        "type": "string"
      },
      {
        "name": "boolField",
        "type": "boolean"
      },
      {
        "name": "floatField",
        "type": "float"
      },
      {
        "name": "doubleField",
        "type": "double"
      },
      {
        "name": "bytesField",
        "type": "bytes"
      },
      {
        "name": "nullField",
        "type": "null"
      },
      {
        "name": "arrayField",
        "type": {
          "type": "array",
          "items": "double"
        }
      },
      {
        "name": "mapField",
        "type": {
          "type": "map",
          "values": {
            "name": "org.apache.avro.Foo",
            "type": "record",
            "fields": [
              {
                "name": "label",
                "type": "string"
              }
            ]
          }
        }
      },
      {
        "name": "unionField",
        "type": [
          "boolean",
          "double",
          {
            "type": "array",
            "items": "bytes"
          }
        ]
      },
      {
        "name": "enumField",
        "type": {
          "name": "org.apache.avro.Kind",
          "type": "enum",
          "symbols": [
            "A",
            "B",
            "C"
          ]
        }
      },
      {
        "name": "fixedField",
        "type": {
          "name": "org.apache.avro.MD5",
          "type": "fixed",
          "size": 16
        }
      },
      {
        "name": "recordField",
        "type": {
          "name": "org.apache.avro.Node",
          "type": "record",
          "fields": [
            {
              "name": "label",
              "type": "string"
            },
            {
              "name": "children",
              "type": {
                "type": "array",
                "items": "org.apache.avro.Node"
              }
            }
          ]
        }
      }
    ]
  }
  #xa4b5a0a6930a2ce8
  (make-instance
   'avro:record
   :name "Interop"
   :namespace "org.apache.avro"
   :direct-slots
   `((:name |intField| :type avro:int)
     (:name |longField| :type avro:long)
     (:name |stringField| :type avro:string)
     (:name |boolField| :type avro:boolean)
     (:name |floatField| :type avro:float)
     (:name |doubleField| :type avro:double)
     (:name |bytesField| :type avro:bytes)
     (:name |nullField| :type avro:null)
     (:name |arrayField|
      :type ,(closer-mop:ensure-class
              'array<double>
              :metaclass 'avro:array
              :items 'avro:double))
     (:name |mapField|
      :type ,(closer-mop:ensure-class
              'map<foo>
              :metaclass 'avro:map
              :values (closer-mop:ensure-class
                       'foo
                       :metaclass 'avro:record
                       :enclosing-namespace "org.apache.avro"
                       :direct-slots '((:name |label| :type avro:string)))))
     (:name |unionField|
      :type ,(closer-mop:ensure-class
              'union<boolean-double-array<bytes>>
              :metaclass 'avro:union
              :schemas `(avro:boolean
                         avro:double
                         ,(closer-mop:ensure-class
                           'array<bytes>
                           :metaclass 'avro:array
                           :items 'avro:bytes))))
     (:name |enumField|
      :type ,(closer-mop:ensure-class
              '|Kind|
              :metaclass 'avro:enum
              :enclosing-namespace "org.apache.avro"
              :symbols (list "A" "B" "C")))
     (:name |fixedField|
      :type ,(closer-mop:ensure-class
              'md5
              :metaclass 'avro:fixed
              :enclosing-namespace "org.apache.avro"
              :size 16))
     (:name |recordField|
      :type ,(closer-mop:ensure-class
              '|Node|
              :metaclass 'avro:record
              :direct-slots
              `((:name |label| :type avro:string)
                (:name |children|
                 :type ,(closer-mop:ensure-class
                         'array<node>
                         :metaclass 'avro:array
                         :items '|Node|)))
              :enclosing-namespace "org.apache.avro"))))
  (defclass array<double> ()
    ()
    (:metaclass avro:array)
    (:items avro:double))
  (defclass |Foo| ()
    ((|label| :type avro:string))
    (:metaclass avro:record)
    (:enclosing-namespace "org.apache.avro"))
  (defclass map<Foo> ()
    ()
    (:metaclass avro:map)
    (:values |Foo|))
  (defclass array<bytes> ()
    ()
    (:metaclass avro:array)
    (:items avro:bytes))
  (defclass union<boolean-double-array<bytes>> ()
    ()
    (:metaclass avro:union)
    (:schemas avro:boolean avro:double array<bytes>))
  (defclass |Kind| ()
    ()
    (:metaclass avro:enum)
    (:symbols "A" "B" "C")
    (:enclosing-namespace "org.apache.avro"))
  (defclass MD5 ()
    ()
    (:metaclass avro:fixed)
    (:size 16)
    (:enclosing-namespace "org.apache.avro"))
  (defclass array<Node> ()
    ()
    (:metaclass avro:array)
    (:items |Node|))
  (defclass |Node| ()
    ((|label| :type avro:string)
     (|children| :type array<Node>))
    (:metaclass avro:record)
    (:enclosing-namespace "org.apache.avro"))
  (defclass |Interop| ()
    ((|intField| :type avro:int)
     (|longField| :type avro:long)
     (|stringField| :type avro:string)
     (|boolField| :type avro:boolean)
     (|floatField| :type avro:float)
     (|doubleField| :type avro:double)
     (|bytesField| :type avro:bytes)
     (|nullField| :type avro:null)
     (|arrayField| :type array<double>)
     (|mapField| :type map<Foo>)
     (|unionField| :type union<boolean-double-array<bytes>>)
     (|enumField| :type |Kind|)
     (|fixedField| :type MD5)
     (|recordField| :type |Node|))
    (:metaclass avro:record)
    (:namespace "org.apache.avro")))

(define-schema-test python-ipaddr
  {
    "type": "record",
    "name": "ipAddr",
    "fields": [
      {
        "name": "addr",
        "type": [
          {
            "name": "IPv6",
            "type": "fixed",
            "size": 16
          },
          {
            "name": "IPv4",
            "type": "fixed",
            "size": 4
          }
        ]
      }
    ]
  }
  {
    "name": "ipAddr",
    "type": "record",
    "fields": [
      {
        "name": "addr",
        "type": [
          {
            "name": "IPv6",
            "type": "fixed",
            "size": 16
          },
          {
            "name": "IPv4",
            "type": "fixed",
            "size": 4
          }
        ]
      }
    ]
  }
  #x44188a294e1b968d
  (make-instance
   'avro:record
   :name "ipAddr"
   :direct-slots
   `((:name |addr|
      :type ,(make-instance
              'avro:union
              :schemas (list
                        (make-instance 'avro:fixed :name "IPv6" :size 16)
                        (make-instance 'avro:fixed :name "IPv4" :size 4))))))
  (defclass |IPv6| ()
    ()
    (:metaclass avro:fixed)
    (:size 16))
  (defclass |IPv4| ()
    ()
    (:metaclass avro:fixed)
    (:size 4))
  (defclass union<ipv6-ipv4> ()
    ()
    (:metaclass avro:union)
    (:schemas |IPv6| |IPv4|))
  (defclass |ipAddr| ()
    ((|addr| :type union<ipv6-ipv4>))
    (:metaclass avro:record)))

(define-schema-test python-testdoc
  {
    "type": "record",
    "name": "TestDoc",
    "doc": "Doc string",
    "fields": [
      {
        "name": "name",
        "type": "string",
        "doc": "Doc String"
      }
    ]
  }
  {
    "name": "TestDoc",
    "type": "record",
    "fields": [
      {
        "name": "name",
        "type": "string"
      }
    ]
  }
  #x09c1cd2bf060660e
  (make-instance
   'avro:record
   :name '|TestDoc|
   :documentation "Doc string"
   :direct-slots
   `((:name |name| :type avro:string :documentation "Doc String")))
  (defclass |TestDoc| ()
    ((|name| :type avro:string :documentation "Doc String"))
    (:metaclass avro:record)
    (:documentation "Doc string")))

(define-schema-test inner-outer
  {
    "type": "record",
    "name": "OuterRecord",
    "fields": [
      {
        "name": "name",
        "type": "string"
      },
      {
        "name": "nested",
        "default": {
          "value": 4
        },
        "type": {
          "type": "record",
          "name": "InnerRecord",
          "fields": [
            {
              "name": "value",
              "type": "int"
            }
          ]
        }
      }
    ]
  }
  {
    "name": "OuterRecord",
    "type": "record",
    "fields": [
      {
        "name": "name",
        "type": "string"
      },
      {
        "name": "nested",
        "type": {
          "name": "InnerRecord",
          "type": "record",
          "fields": [
            {
              "name": "value",
              "type": "int"
            }
          ]
        }
      }
    ]
  }
  #x66670f7ec6a70b6b
  (make-instance
   'avro:record
   :name "OuterRecord"
   :direct-slots
   `((:name |name|
      :type avro:string)
     (:name |nested|
      :default (|value| 4)
      :type ,(make-instance
              'avro:record
              :name "InnerRecord"
              :direct-slots
              `((:name |value| :type avro:int))))))
  (defclass |InnerRecord| ()
    ((|value| :type avro:int))
    (:metaclass avro:record))
  (defclass |OuterRecord| ()
    ((|name| :type avro:string)
     (|nested| :type |InnerRecord| :default (|value| 4)))
    (:metaclass avro:record)))

(define-io-test io
    ((array<double>
      (make-instance 'avro:array :items 'avro:double))
     (foo
      (make-instance
       'avro:record
       :name 'foo
       :direct-slots
       '((:name |label|
          :type avro:string
          :initargs (:label)
          :readers (label)
          :writers ((setf label))))))
     (map<foo>
      (make-instance 'avro:map :values foo))
     (array<bytes>
      (make-instance 'avro:array :items 'avro:bytes))
     (union<boolean-double-array<bytes>>
      (make-instance
       'avro:union
       :schemas `(avro:boolean avro:double ,array<bytes>)))
     (kind
      (make-instance 'avro:enum :name 'kind :symbols '("A" "B" "C")))
     (md5
      (make-instance 'avro:fixed :name 'md5 :size 16))
     (array<node>
      (make-instance 'avro:array :items 'node))
     (node
      (closer-mop:ensure-class
       'node
       :metaclass 'avro:record
       :direct-slots
       `((:name |label|
          :type avro:string
          :initargs (:label)
          :readers (label)
          :writers ((setf label)))
         (:name |children|
          :type ,array<node>
          :initargs (:children)
          :readers (children)
          :writers ((setf children)))))))
    (make-instance
     'avro:record
     :name 'interop
     :direct-slots
     `((:name |intField|
        :type avro:int
        :initargs (:int-field)
        :readers (int-field)
        :writers ((setf int-field)))
       (:name |longField|
        :type avro:long
        :initargs (:long-field)
        :readers (long-field)
        :writers ((setf long-field)))
       (:name |stringField|
        :type avro:string
        :initargs (:string-field)
        :readers (string-field)
        :writers ((setf string-field)))
       (:name |boolField|
        :type avro:boolean
        :initargs (:bool-field)
        :readers (bool-field)
        :writers ((setf bool-field)))
       (:name |floatField|
        :type avro:float
        :initargs (:float-field)
        :readers (float-field)
        :writers ((setf float-field)))
       (:name |doubleField|
        :type avro:double
        :initargs (:double-field)
        :readers (double-field)
        :writers ((setf double-field)))
       (:name |bytesField|
        :type avro:bytes
        :initargs (:bytes-field)
        :readers (bytes-field)
        :writers ((setf bytes-field)))
       (:name |nullField|
        :type avro:null
        :initargs (:null-field)
        :readers (null-field)
        :writers ((setf null-field)))
       (:name |arrayField|
        :type ,array<double>
        :initargs (:array-field)
        :readers (array-field)
        :writers ((setf array-field)))
       (:name |mapField|
        :type ,map<foo>
        :initargs (:map-field)
        :readers (map-field)
        :writers ((setf map-field)))
       (:name |unionField|
        :type ,union<boolean-double-array<bytes>>
        :initargs (:union-field)
        :readers (union-field)
        :writers ((setf union-field)))
       (:name |enumField|
        :type ,kind
        :initargs (:enum-field)
        :readers (enum-field)
        :writers ((setf enum-field)))
       (:name |fixedField|
        :type ,md5
        :initargs (:fixed-field)
        :readers (fixed-field)
        :writers ((setf fixed-field)))
       (:name |recordField|
        :type ,node
        :initargs (:record-field)
        :readers (record-field)
        :writers ((setf record-field)))))
    (make-instance
     schema
     :int-field 3
     :long-field 4
     :string-field "abc"
     :bool-field 'avro:true
     :float-field 12.34f0
     :double-field 56.78d0
     :bytes-field (make-array 3 :element-type '(unsigned-byte 8)
                                :initial-contents '(2 4 6))
     :null-field nil
     :array-field (make-instance
                   array<double>
                   :initial-contents '(23.7d0 123.456d0))
     :map-field (let ((map (make-instance map<foo>))
                      (abc (make-instance foo :label "ABC"))
                      (def (make-instance foo :label "DEF")))
                  (setf (avro:gethash "abc" map) abc
                        (avro:gethash "def" map) def)
                  map)
     :union-field (make-instance
                   union<boolean-double-array<bytes>>
                   :object 'avro:false)
     :enum-field (make-instance kind :enum "A")
     :fixed-field (make-instance
                   md5
                   :initial-contents (loop
                                       for i from 1 to 16
                                       collect (* i 2)))
     :record-field (let ((child-1
                           (make-instance
                            node
                            :label "child-1"
                            :children (make-instance array<node>)))
                         (child-2
                           (make-instance
                            node
                            :label "child-2"
                            :children (make-instance array<node>))))
                     (make-instance
                      node
                      :label "parent"
                      :children
                      (make-instance
                       array<node>
                       :initial-contents (list child-1 child-2)))))
    (6                                       ; 3
     8                                       ; 4
     6 #x61 #x62 #x63                        ; "abc"
     1                                       ; 'avro:true
     #xa4 #x70 #x45 #x41                     ; 12.34f0
     #xa4 #x70 #x3d #x0a #xd7 #x63 #x4c #x40 ; 56.78d0
     6 2 4 6                                 ; #(2 4 6)
     ;; nil is not serialized
     3                                       ; two elements in array:
     32                                      ; 16 bytes in array block
     #x33 #x33 #x33 #x33 #x33 #xb3 #x37 #x40 ; 23.7d0
     #x77 #xbe #x9f #x1a #x2f #xdd #x5e #x40 ; 123.456d0
     0                                       ; end array
     3                                  ; two key-value pairs in map
     32                                 ; 16 bytes in map block
     6 #x61 #x62 #x63                   ; "abc"
     6 #x41 #x42 #x43                   ; "ABC"
     6 #x64 #x65 #x66                   ; "def"
     6 #x44 #x45 #x46                   ; "DEF"
     0                                  ; end map
     0 0                                ; 'avro:false under union
     0                                  ; "A" under enum
     2 4 6 8 10 12 14 16 18 20 22 24 26 28 30 32 ; md5
     12 #x70 #x61 #x72 #x65 #x6e #x74            ; "parent"
     3                                     ; two elements in array
     36                                    ; 18 bytes in array block
     14 #x63 #x68 #x69 #x6c #x64 #x2d #x31 ; "child-1"
     0                                     ; empty array
     14 #x63 #x68 #x69 #x6c #x64 #x2d #x32 ; "child-2"
     0                                     ; empty array
     0                                     ; end 2 element array
     )
  (is (= 3 (int-field arg)))
  (is (= 4 (long-field arg)))
  (is (string= "abc" (string-field arg)))
  (is (eq 'avro:true (bool-field arg)))
  (is (= 12.34f0 (float-field arg)))
  (is (= 56.78d0 (double-field arg)))
  (is (equalp #(2 4 6) (bytes-field arg)))
  (is (null (null-field arg)))
  (is (equalp #(23.7d0 123.456d0) (avro:raw (array-field arg))))
  (let ((map (map-field arg)))
    (is (= 2 (avro:hash-table-count map)))
    (is (string= "ABC" (label (avro:gethash "abc" map))))
    (is (string= "DEF" (label (avro:gethash "def" map)))))
  (is (eq 'avro:false (avro:object (union-field arg))))
  (is (string= "A" (avro:which-one (enum-field arg))))
  (is (equalp
       #(2 4 6 8 10 12 14 16 18 20 22 24 26 28 30 32)
       (avro:raw (fixed-field arg))))
  (let* ((parent (record-field arg))
         (children (children parent))
         (child-1 (elt children 0))
         (child-2 (elt children 1)))
    (is (= 2 (length children)))
    (is (zerop (length (children child-1))))
    (is (zerop (length (children child-2))))
    (is (string= "parent" (label parent)))
    (is (string= "child-1" (label child-1)))
    (is (string= "child-2" (label child-2))))

  (setf (string-field arg) "foo")
  (is (string= "foo" (string-field arg)))
  (setf (string-field arg) "abc")

  (signals error
    (setf (string-field arg) 3)))

(test non-fixed-size
  (let ((schema (make-instance
                 'avro:record
                 :name '#:foo
                 :direct-slots '((:name #:foo :type avro:boolean)
                                 (:name #:bar :type avro:string)))))
    (is (null (internal:fixed-size schema)))))

(test fixed-size
  (let ((schema (make-instance
                 'avro:record
                 :name '#:foo
                 :direct-slots '((:name #:foo :type avro:boolean)
                                 (:name #:bar :type avro:boolean)))))
    (is (= 2 (internal:fixed-size schema)))))
