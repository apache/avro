#Licensed to the Apache Software Foundation (ASF) under one
#or more contributor license agreements.  See the NOTICE file
#distributed with this work for additional information
#regarding copyright ownership.  The ASF licenses this file
#to you under the Apache License, Version 2.0 (the
#"License"); you may not use this file except in compliance
#with the License.  You may obtain a copy of the License at
#
#http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

""" Uses genericio to write and read data objects."""

import avro.schema as schema
import avro.genericio as genericio
import avro.ipc as ipc

class Requestor(ipc.RequestorBase):
  """Requestor implementation for generic python data."""

  def getdatumwriter(self, schm):
    return genericio.DatumWriter(schm)

  def getdatumreader(self, schm):
    return genericio.DatumReader(schm)

  def writerequest(self, schm, req, encoder):
    self.getdatumwriter(schm).write(req, encoder)

  def readresponse(self, schm, decoder):
    return self.getdatumreader(schm).read(decoder)

  def readerror(self, schm, decoder):
    return ipc.AvroRemoteException(self.getdatumreader(schm).read(decoder))

class Responder(ipc.ResponderBase):
  """Responder implementation for generic python data."""

  def getdatumwriter(self, schm):
    return genericio.DatumWriter(schm)

  def getdatumreader(self, schm):
    return genericio.DatumReader(schm)

  def readrequest(self, schm, decoder):
    return self.getdatumreader(schm).read(decoder)

  def writeresponse(self, schm, response, encoder):
    self.getdatumwriter(schm).write(response, encoder)

  def writeerror(self, schm, error, encoder):
    self.getdatumwriter(schm).write(error.getvalue(), encoder)
