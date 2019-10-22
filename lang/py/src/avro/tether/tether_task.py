#!/usr/bin/env python

##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import, division, print_function

import collections
import io as pyio
import logging
import os
import sys
import threading
import traceback
from StringIO import StringIO

from avro import io as avio
from avro import ipc, protocol, schema

__all__ = ["TetherTask", "TaskType", "inputProtocol", "outputProtocol", "HTTPRequestor"]

# create protocol objects for the input and output protocols
# The build process should copy InputProtocol.avpr and OutputProtocol.avpr
# into the same directory as this module
inputProtocol=None
outputProtocol=None

TaskType=None
if (inputProtocol is None):
  pfile=os.path.split(__file__)[0]+os.sep+"InputProtocol.avpr"

  if not(os.path.exists(pfile)):
    raise Exception("Could not locate the InputProtocol: {0} does not exist".format(pfile))

  with file(pfile,'r') as hf:
    prototxt=hf.read()

  inputProtocol=protocol.parse(prototxt)

  # use a named tuple to represent the tasktype enumeration
  taskschema=inputProtocol.types_dict["TaskType"]
  _ttype=collections.namedtuple("_tasktype",taskschema.symbols)
  TaskType=_ttype(*taskschema.symbols)

if (outputProtocol is None):
  pfile=os.path.split(__file__)[0]+os.sep+"OutputProtocol.avpr"

  if not(os.path.exists(pfile)):
    raise Exception("Could not locate the OutputProtocol: {0} does not exist".format(pfile))

  with file(pfile,'r') as hf:
    prototxt=hf.read()

  outputProtocol=protocol.parse(prototxt)

class Collector(object):
  """
  Collector for map and reduce output values
  """
  def __init__(self,scheme=None,outputClient=None):
    """

    Parameters
    ---------------------------------------------
    scheme - The scheme for the datums to output - can be a json string
           - or an instance of Schema
    outputClient - The output client used to send messages to the parent
    """

    if not(isinstance(scheme,schema.Schema)):
      scheme=schema.parse(scheme)

    if (outputClient is None):
      raise ValueError("output client can't be none.")

    self.scheme=scheme
    self.buff=StringIO()
    self.encoder=avio.BinaryEncoder(self.buff)

    self.datum_writer = avio.DatumWriter(writers_schema=self.scheme)
    self.outputClient=outputClient

  def collect(self,record,partition=None):
    """Collect a map or reduce output value

    Parameters
    ------------------------------------------------------
    record - The record to write
    partition - Indicates the partition for a pre-partitioned map output
              - currently not supported
    """

    self.buff.truncate(0)
    self.datum_writer.write(record, self.encoder);
    self.buff.flush();
    self.buff.seek(0)

    # delete all the data in the buffer
    if (partition is None):

      # TODO: Is there a more efficient way to read the data in self.buff?
      # we could use self.buff.read() but that returns the byte array as a string
      # will that work?  We can also use self.buff.readinto to read it into
      # a bytearray but the byte array must be pre-allocated
      # self.outputClient.output(self.buff.buffer.read())

      #its not a StringIO
      self.outputClient.request("output",{"datum":self.buff.read()})
    else:
      self.outputClient.request("outputPartitioned",{"datum":self.buff.read(),"partition":partition})



def keys_are_equal(rec1,rec2,fkeys):
  """Check if the "keys" in two records are equal. The key fields
  are all fields for which order isn't marked ignore.

  Parameters
  -------------------------------------------------------------------------
  rec1  - The first record
  rec2 - The second record
  fkeys - A list of the fields to compare
  """

  for f in fkeys:
    if not(rec1[f]==rec2[f]):
      return False

  return True


class HTTPRequestor(object):
  """
  This is a small requestor subclass I created for the HTTP protocol.
  Since the HTTP protocol isn't persistent, we need to instantiate
  a new transciever and new requestor for each request.
  But I wanted to use of the requestor to be identical to that for
  SocketTransciever so that we can seamlessly switch between the two.
  """

  def __init__(self, server,port,protocol):
    """
    Instantiate the class.

    Parameters
    ----------------------------------------------------------------------
    server - The server hostname
    port - Which port to use
    protocol - The protocol for the communication
    """

    self.server=server
    self.port=port
    self.protocol=protocol

  def request(self,*args,**param):
    transciever=ipc.HTTPTransceiver(self.server,self.port)
    requestor=ipc.Requestor(self.protocol, transciever)
    return requestor.request(*args,**param)


class TetherTask(object):
  """
  Base class for python tether mapreduce programs.

  ToDo: Currently the subclass has to implement both reduce and reduceFlush.
  This is not very pythonic. A pythonic way to implement the reducer
  would be to pass the reducer a generator (as dumbo does) so that the user
  could iterate over the records for the given key.
  How would we do this. I think we would need to have two threads, one thread would run
  the user's reduce function. This loop would be suspended when no reducer records were available.
  The other thread would read in the records for the reducer. This thread should
  only buffer so many records at a time (i.e if the buffer is full, self.input shouldn't return right
  away but wait for space to free up)
  """

  def __init__(self,inschema=None,midschema=None,outschema=None):
    """

    Parameters
    ---------------------------------------------------------
    inschema - The scheme for the input to the mapper
    midschema  - The scheme for the output of the mapper
    outschema - The scheme for the output of the reducer

    An example scheme for the prototypical word count example would be
    inscheme='{"type":"record", "name":"Pair","namespace":"org.apache.avro.mapred","fields":[
              {"name":"key","type":"string"},
              {"name":"value","type":"long","order":"ignore"}]
              }'

    Important: The records are split into (key,value) pairs as required by map reduce
    by using all fields with "order"=ignore for the key and the remaining fields for the value.

    The subclass provides these schemas in order to tell this class which schemas it expects.
    The configure request will also provide the schemas that the parent process is using.
    This allows us to check whether the schemas match and if not whether we can resolve
    the differences (see https://avro.apache.org/docs/current/spec.html#Schema+Resolution))

    """


    if (inschema is None):
      raise ValueError("inschema can't be None")

    if (midschema is None):
      raise ValueError("midschema can't be None")

    if (outschema is None):
      raise ValueError("outschema can't be None")

    # make sure we can parse the schemas
    # Should we call fail if we can't parse the schemas?
    self.inschema=schema.parse(inschema)
    self.midschema=schema.parse(midschema)
    self.outschema=schema.parse(outschema)


    # declare various variables
    self.clienTransciever=None

    # output client is used to communicate with the parent process
    # in particular to transmit the outputs of the mapper and reducer
    self.outputClient = None

    # collectors for the output of the mapper and reducer
    self.midCollector=None
    self.outCollector=None

    self._partitions=None

    # cache a list of the fields used by the reducer as the keys
    # we need the fields to decide when we have finished processing all values for
    # a given key. We cache the fields to be more efficient
    self._red_fkeys=None

    # We need to keep track of the previous record fed to the reducer
    # b\c we need to be able to determine when we start processing a new group
    # in the reducer
    self.midRecord=None

    # create an event object to signal when
    # http server is ready to be shutdown
    self.ready_for_shutdown=threading.Event()
    self.log=logging.getLogger("TetherTask")

  def open(self, inputport,clientPort=None):
    """Open the output client - i.e the connection to the parent process

    Parameters
    ---------------------------------------------------------------
    inputport - This is the port that the subprocess is listening on. i.e the
                subprocess starts a server listening on this port to accept requests from
                the parent process
    clientPort - The port on which the server in the parent process is listening
                - If this is None we look for the environment variable AVRO_TETHER_OUTPUT_PORT
                - This is mainly provided for debugging purposes. In practice
                we want to use the environment variable

    """


    # Open the connection to the parent process
    # The port the parent process is listening on is set in the environment
    # variable AVRO_TETHER_OUTPUT_PORT
    # open output client, connecting to parent

    if (clientPort is None):
      clientPortString = os.getenv("AVRO_TETHER_OUTPUT_PORT")
      if (clientPortString is None):
        raise Exception("AVRO_TETHER_OUTPUT_PORT env var is not set")

      clientPort = int(clientPortString)

    self.log.info("TetherTask.open: Opening connection to parent server on port={0}".format(clientPort))

    # We use the HTTP protocol although we hope to shortly have
    # support for SocketServer,
    usehttp=True

    if(usehttp):
      # self.outputClient =  ipc.Requestor(outputProtocol, self.clientTransceiver)
      # since HTTP is stateless, a new transciever
      # is created and closed for each request. We therefore set clientTransciever to None
      # We still declare clientTransciever because for other (state) protocols we will need
      # it and we want to check when we get the message fail whether the transciever
      # needs to be closed.
      # self.clientTranciever=None
      self.outputClient =  HTTPRequestor("127.0.0.1",clientPort,outputProtocol)

    else:
      raise NotImplementedError("Only http protocol is currently supported")

    try:
      self.outputClient.request('configure',{"port":inputport})
    except Exception as e:
      estr= traceback.format_exc()
      self.fail(estr)


  def configure(self,taskType,  inSchemaText,  outSchemaText):
    """

    Parameters
    -------------------------------------------------------------------
    taskType - What type of task (e.g map, reduce)
             - This is an enumeration which is specified in the input protocol
    inSchemaText -  string containing the input schema
                 - This is the actual schema with which the data was encoded
                   i.e it is the writer_schema (see https://avro.apache.org/docs/current/spec.html#Schema+Resolution)
                   This is the schema the parent process is using which might be different
                   from the one provided by the subclass of tether_task

    outSchemaText - string containing the output scheme
                  - This is the schema expected by the parent process for the output
    """
    self.taskType = taskType

    try:
      inSchema = schema.parse(inSchemaText)
      outSchema = schema.parse(outSchemaText)

      if (taskType==TaskType.MAP):
        self.inReader=avio.DatumReader(writers_schema=inSchema,readers_schema=self.inschema)
        self.midCollector=Collector(outSchemaText,self.outputClient)

      elif(taskType==TaskType.REDUCE):
        self.midReader=avio.DatumReader(writers_schema=inSchema,readers_schema=self.midschema)
        # this.outCollector = new Collector<OUT>(outSchema);
        self.outCollector=Collector(outSchemaText,self.outputClient)

        # determine which fields in the input record are they keys for the reducer
        self._red_fkeys=[f.name for f in self.midschema.fields if not(f.order=='ignore')]

    except Exception as e:

      estr= traceback.format_exc()
      self.fail(estr)

  def set_partitions(self,npartitions):

    try:
      self._partitions=npartitions
    except Exception as e:
      estr= traceback.format_exc()
      self.fail(estr)

  def get_partitions():
    """ Return the number of map output partitions of this job."""
    return self._partitions

  def input(self,data,count):
    """ Recieve input from the server

    Parameters
    ------------------------------------------------------
    data - Sould containg the bytes encoding the serialized data
          - I think this gets represented as a tring
    count - how many input records are provided in the binary stream
    """
    try:
      # to avio.BinaryDecoder
      bdata=StringIO(data)
      decoder = avio.BinaryDecoder(bdata)

      for i in range(count):
        if (self.taskType==TaskType.MAP):
          inRecord = self.inReader.read(decoder)

          # Do we need to pass midCollector if its declared as an instance variable
          self.map(inRecord, self.midCollector)

        elif (self.taskType==TaskType.REDUCE):

          # store the previous record
          prev = self.midRecord

          # read the new record
          self.midRecord = self.midReader.read(decoder);
          if (prev != None and not(keys_are_equal(self.midRecord,prev,self._red_fkeys))):
            # since the key has changed we need to finalize the processing
            # for this group of key,value pairs
            self.reduceFlush(prev, self.outCollector)
          self.reduce(self.midRecord, self.outCollector)

    except Exception as e:
      estr= traceback.format_exc()
      self.log.warning("failing: "+estr)
      self.fail(estr)

  def complete(self):
    """
    Process the complete request
    """
    if ((self.taskType == TaskType.REDUCE ) and not(self.midRecord is None)):
      try:
        self.reduceFlush(self.midRecord, self.outCollector);
      except Exception as e:
        estr=traceback.format_exc()
        self.log.warning("failing: "+estr);
        self.fail(estr)

    self.outputClient.request("complete",dict())

  def map(self,record,collector):
    """Called with input values to generate intermediat values (i.e mapper output).

    Parameters
    ----------------------------------------------------------------------------
    record - The input record
    collector - The collector to collect the output

    This is an abstract function which should be overloaded by the application specific
    subclass.
    """

    raise NotImplementedError("This is an abstract method which should be overloaded in the subclass")

  def reduce(self,record, collector):
    """ Called with input values to generate reducer output. Inputs are sorted by the mapper
    key.

    The reduce function is invoked once for each value belonging to a given key outputted
    by the mapper.

    Parameters
    ----------------------------------------------------------------------------
    record - The mapper output
    collector - The collector to collect the output

    This is an abstract function which should be overloaded by the application specific
    subclass.
    """

    raise NotImplementedError("This is an abstract method which should be overloaded in the subclass")

  def reduceFlush(self,record, collector):
    """
    Called with the last intermediate value in each equivalence run.
    In other words, reduceFlush is invoked once for each key produced in the reduce
    phase. It is called after reduce has been invoked on each value for the given key.

    Parameters
    ------------------------------------------------------------------
    record - the last record on which reduce was invoked.
    """
    raise NotImplementedError("This is an abstract method which should be overloaded in the subclass")

  def status(self,message):
    """
    Called to update task status
    """
    self.outputClient.request("status",{"message":message})

  def count(self,group, name, amount):
    """
    Called to increment a counter
    """
    self.outputClient.request("count",{"group":group, "name":name, "amount":amount})

  def fail(self,message):
    """
    Call to fail the task.
    """
    self.log.error("TetherTask.fail: failure occured message follows:\n{0}".format(message))
    try:
      self.outputClient.request("fail",{"message":message})
    except Exception as e:
      estr=traceback.format_exc()
      self.log.error("TetherTask.fail: an exception occured while trying to send the fail message to the output server:\n{0}".format(estr))

    self.close()

  def close(self):
    self.log.info("TetherTask.close: closing")
    if not(self.clienTransciever is None):
      try:
        self.clienTransciever.close()

      except Exception as e:
        # ignore exceptions
        pass

    # http server is ready to be shutdown
    self.ready_for_shutdown.set()
