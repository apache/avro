#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e						  # exit on error
set -x		

case "$1" in

    test)
	CONFIGURATION=Release TARGETFRAMEWORKVERSION=3.5 xbuild
	nunit-console Avro.nunit
	;;

    clean)
	rm -rf src/apache/{main,test,codegen}/obj
        rm -rf build
	;;

    *)
        echo "Usage: $0 {test|clean}"
        exit 1
esac

exit 0
