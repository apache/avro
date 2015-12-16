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

import os
import subprocess
import sys
import time
import unittest

import set_avro_test_path


class TestTetherTaskRunner(unittest.TestCase):
  """ unit test for a tethered task runner.
  """

  def test1(self):
    from word_count_task import WordCountTask
    from avro.tether import TaskRunner, find_port,HTTPRequestor,inputProtocol, TaskType
    from avro import io as avio
    import mock_tether_parent
    import subprocess
    import StringIO
    import logging

    # set the logging level to debug so that debug messages are printed
    logging.basicConfig(level=logging.DEBUG)

    proc=None
    try:
      # launch the server in a separate process
      env=dict()
      env["PYTHONPATH"]=':'.join(sys.path)
      parent_port=find_port()

      pyfile=mock_tether_parent.__file__
      proc=subprocess.Popen(["python", pyfile,"start_server","{0}".format(parent_port)])
      input_port=find_port()

      print "Mock server started process pid={0}".format(proc.pid)
      # Possible race condition? open tries to connect to the subprocess before the subprocess is fully started
      # so we give the subprocess time to start up
      time.sleep(1)

      runner=TaskRunner(WordCountTask())

      runner.start(outputport=parent_port,join=False)

      # Wait for the server to start
      time.sleep(1)

      # Test sending various messages to the server and ensuring they are
      # processed correctly
      requestor=HTTPRequestor("localhost",runner.server.server_address[1],inputProtocol)

      # TODO: We should validate that open worked by grabbing the STDOUT of the subproces
      # and ensuring that it outputted the correct message.

      # Test the mapper
      requestor.request("configure",{"taskType":TaskType.MAP,"inSchema":str(runner.task.inschema),"outSchema":str(runner.task.midschema)})

      # Serialize some data so we can send it to the input function
      datum="This is a line of text"
      writer = StringIO.StringIO()
      encoder = avio.BinaryEncoder(writer)
      datum_writer = avio.DatumWriter(runner.task.inschema)
      datum_writer.write(datum, encoder)

      writer.seek(0)
      data=writer.read()


      # Call input to simulate calling map
      requestor.request("input",{"data":data,"count":1})

      #Test the reducer
      requestor.request("configure",{"taskType":TaskType.REDUCE,"inSchema":str(runner.task.midschema),"outSchema":str(runner.task.outschema)})

      #Serialize some data so we can send it to the input function
      datum={"key":"word","value":2}
      writer = StringIO.StringIO()
      encoder = avio.BinaryEncoder(writer)
      datum_writer = avio.DatumWriter(runner.task.midschema)
      datum_writer.write(datum, encoder)

      writer.seek(0)
      data=writer.read()


      #Call input to simulate calling reduce
      requestor.request("input",{"data":data,"count":1})

      requestor.request("complete",{})


      runner.task.ready_for_shutdown.wait()
      runner.server.shutdown()
      #time.sleep(2)
      #runner.server.shutdown()

      sthread=runner.sthread

      #Possible race condition?
      time.sleep(1)

      #make sure the other thread terminated
      self.assertFalse(sthread.isAlive())

      #shutdown the logging
      logging.shutdown()

    except Exception as e:
      raise
    finally:
      #close the process
      if not(proc is None):
        proc.kill()


  def test2(self):
    """
    In this test we want to make sure that when we run "tether_task_runner.py"
    as our main script everything works as expected. We do this by using subprocess to run it
    in a separate thread.
    """
    from word_count_task import WordCountTask
    from avro.tether import TaskRunner, find_port,HTTPRequestor,inputProtocol, TaskType
    from avro.tether import tether_task_runner
    from avro import io as avio
    import mock_tether_parent
    import subprocess
    import StringIO


    proc=None

    runnerproc=None
    try:
      #launch the server in a separate process
      env=dict()
      env["PYTHONPATH"]=':'.join(sys.path)
      parent_port=find_port()

      pyfile=mock_tether_parent.__file__
      proc=subprocess.Popen(["python", pyfile,"start_server","{0}".format(parent_port)])

      #Possible race condition? when we start tether_task_runner it will call
      # open tries to connect to the subprocess before the subprocess is fully started
      #so we give the subprocess time to start up
      time.sleep(1)


      #start the tether_task_runner in a separate process
      env={"AVRO_TETHER_OUTPUT_PORT":"{0}".format(parent_port)}
      env["PYTHONPATH"]=':'.join(sys.path)

      runnerproc=subprocess.Popen(["python",tether_task_runner.__file__,"word_count_task.WordCountTask"],env=env)

      #possible race condition wait for the process to start
      time.sleep(1)



      print "Mock server started process pid={0}".format(proc.pid)
      #Possible race condition? open tries to connect to the subprocess before the subprocess is fully started
      #so we give the subprocess time to start up
      time.sleep(1)


    except Exception as e:
      raise
    finally:
      #close the process
      if not(runnerproc is None):
        runnerproc.kill()

      if not(proc is None):
        proc.kill()

if __name__==("__main__"):
  unittest.main()
