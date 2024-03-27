<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Public API

The following systems and packages are exported:

* `avro`, which defines the `org.apache.avro` package

* `avro/asdf`, which defines the `org.apache.avro/asdf` package

# Running Tests

Tests can be run through a lisp repl or through docker.

## Lisp REPL

```lisp
;; configure the asdf source-registry to avoid rebinding the special variable
(let ((asdf:*central-registry*
        (cons "path/to/avro/lang/common-lisp/" asdf:*central-registry*)))
  (ql:quickload 'avro/test))

(asdf:test-system 'avro)
```

## Docker

```shell
docker build . -f ./test/Dockerfile.test -t avro:v1

docker run -it --rm avro:v1
```
