# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import avro.schema as schema
import avro.reflectio as reflectio
import testio

_PKGNAME = "org.apache.avro.test."

def dyvalidator(schm, object):
  return reflectio.validate(schm, _PKGNAME, object)

class DyRandomData(testio.RandomData):

  def nextdata(self, schm, d=0):
    if schm.gettype() == schema.RECORD:
      clazz = reflectio.gettype(schm, _PKGNAME)
      result = clazz()
      for field in schm.getfields().values():
        result.__setattr__(field.getname(), self.nextdata(field.getschema(),d))
      return result
    else:
      return testio.RandomData.nextdata(self, schm, d)

class ReflectDReader(reflectio.ReflectDatumReader):
  
  def __init__(self, schm=None):
    reflectio.ReflectDatumReader.__init__(self, _PKGNAME, schm)

class ReflectDWriter(reflectio.ReflectDatumWriter):
  
  def __init__(self, schm=None):
    reflectio.ReflectDatumWriter.__init__(self, _PKGNAME, schm)

class TestSchema(testio.TestSchema):

  def __init__(self, methodName):
    testio.TestSchema.__init__(self, methodName, dyvalidator, ReflectDWriter,
                               ReflectDReader, DyRandomData, False)

