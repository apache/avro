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
"""
Command-line tool for manipulating Avro data files.

NOTE: The API for the command-line tool is experimental.
"""

import sys
from avro import datafile, io

def main(args=sys.argv):
  if len(args) == 1:
    print "Usage: %s (dump)" % args[0]
    return 1

  if args[1] == "dump":
    if len(args) != 3:
      print "Usage: %s dump input_file" % args[0]
      return 1
    for d in datafile.DataFileReader(file_or_stdin(args[2]), io.DatumReader()):
      print repr(d)
  return 0
  
def file_or_stdin(f):
  if f == "-":
    return sys.stdin
  else:
    return file(f)

if __name__ == "__main__":
  sys.exit(main(sys.argv))
