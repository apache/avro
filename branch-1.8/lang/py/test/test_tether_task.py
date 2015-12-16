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

class TestTetherTask(unittest.TestCase):
  """
  TODO: We should validate the the server response by looking at stdout
  """
  def test1(self):
    """
    Test that the thether_task is working. We run the mock_tether_parent in a separate
    subprocess
    """
    from avro import tether
    from avro import io as avio
    from avro import schema
    from avro.tether import HTTPRequestor,inputProtocol, find_port

    import StringIO
    import mock_tether_parent
    from word_count_task import WordCountTask

    task=WordCountTask()

    proc=None
    try:
      # launch the server in a separate process
      # env["AVRO_TETHER_OUTPUT_PORT"]=output_port
      env=dict()
      env["PYTHONPATH"]=':'.join(sys.path)
      server_port=find_port()

      pyfile=mock_tether_parent.__file__
      proc=subprocess.Popen(["python", pyfile,"start_server","{0}".format(server_port)])
      input_port=find_port()

      print "Mock server started process pid={0}".format(proc.pid)
      # Possible race condition? open tries to connect to the subprocess before the subprocess is fully started
      # so we give the subprocess time to start up
      time.sleep(1)
      task.open(input_port,clientPort=server_port)

      # TODO: We should validate that open worked by grabbing the STDOUT of the subproces
      # and ensuring that it outputted the correct message.

      #***************************************************************
      # Test the mapper
      task.configure(tether.TaskType.MAP,str(task.inschema),str(task.midschema))

      # Serialize some data so we can send it to the input function
      datum="This is a line of text"
      writer = StringIO.StringIO()
      encoder = avio.BinaryEncoder(writer)
      datum_writer = avio.DatumWriter(task.inschema)
      datum_writer.write(datum, encoder)

      writer.seek(0)
      data=writer.read()

      # Call input to simulate calling map
      task.input(data,1)

      # Test the reducer
      task.configure(tether.TaskType.REDUCE,str(task.midschema),str(task.outschema))

      # Serialize some data so we can send it to the input function
      datum={"key":"word","value":2}
      writer = StringIO.StringIO()
      encoder = avio.BinaryEncoder(writer)
      datum_writer = avio.DatumWriter(task.midschema)
      datum_writer.write(datum, encoder)

      writer.seek(0)
      data=writer.read()

      # Call input to simulate calling reduce
      task.input(data,1)

      task.complete()

      # try a status
      task.status("Status message")

    except Exception as e:
      raise
    finally:
      # close the process
      if not(proc is None):
        proc.kill()

      pass

if __name__ == '__main__':
  unittest.main()