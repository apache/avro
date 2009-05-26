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

import unittest, random, cStringIO, time, sys, os, struct
import avro.schema as schema
import avro.io as io
import avro.generic as generic

_DIR = "build/test/"
_FILE = _DIR +"test.py.avro"

class RandomData(object):
  def __init__(self, schm, seed=None):
    self.__random = random.seed(seed)
    self.__schm = schm

  def next(self):
    return self.nextdata(self.__schm)
  
  def nextdata(self, schm, d=0):
    if schm.gettype() == schema.BOOLEAN:
      return random.sample([True, False], 1)[0]
    elif schm.gettype() == schema.STRING:
      sample = random.sample('abcdefghijklmnopqrstuvwxyz1234590-=', 
                   random.randint(0,20))
      string = cStringIO.StringIO()
      for s in sample:
        string.write(s)
      return unicode(string.getvalue())
    elif schm.gettype() == schema.INT:
      return random.randint(io._INT_MIN_VALUE, io._INT_MAX_VALUE)
    elif schm.gettype() == schema.LONG:
      return random.randint(io._LONG_MIN_VALUE, io._LONG_MAX_VALUE)
    elif schm.gettype() == schema.FLOAT:
      return round(random.uniform(-1024, 1024))
    elif schm.gettype() == schema.DOUBLE:
      return random.uniform(io._LONG_MIN_VALUE, io._LONG_MAX_VALUE)
    elif schm.gettype() == schema.BYTES:
      string = cStringIO.StringIO()
      len = random.randint(0, 20)
      for i in range(1, len):
        string.write(struct.pack('c',random.sample('12345abcd', 1)[0]))
      return string.getvalue()
    elif schm.gettype() == schema.NULL:
      return None
    elif schm.gettype() == schema.ARRAY:
      arr = list()
      len = random.randint(0, 4)+2-d
      if len < 0:
        len = 0
      for i in range(1, len):
        arr.append(self.nextdata(schm.getelementtype(), d+1))
      return arr
    elif schm.gettype() == schema.MAP:
      map = dict()
      len = random.randint(0, 4)+2-d
      if len < 0:
        len = 0
      for i in range(1, len):
        map[self.nextdata(schema._StringSchema())] = self.nextdata(
                                                    schm.getvaluetype(), d+1)
      return map
    elif schm.gettype() == schema.RECORD:
      m = dict()
      for field, fieldschm in schm.getfields():
        m[field] = self.nextdata(fieldschm, d+1)
      return m
    elif schm.gettype() == schema.UNION:
      datum = self.nextdata(random.choice(schm.getelementtypes()), d)
      return datum
    elif schm.gettype() == schema.ENUM:
      symbols = schm.getenumsymbols()
      len =  symbols.__len__()
      if len == 0:
        return None
      return symbols[random.randint(0,len)-1]
    elif schm.gettype() == schema.FIXED:
      string = cStringIO.StringIO()
      for i in range(0, schm.getsize()):
        string.write(struct.pack('c',random.sample('12345abcd', 1)[0]))
      return string.getvalue()

class TestSchema(unittest.TestCase):

  def __init__(self, methodName, validator=generic.validate,
                               dwriter=generic.DatumWriter, 
                               dreader=generic.DatumReader, random=RandomData,
                               assertdata=True):
    unittest.TestCase.__init__(self, methodName)
    self.__validator = validator
    self.__datumwriter = dwriter
    self.__datumreader = dreader
    self.__random = random
    self.__assertdata = assertdata

  def testNull(self):
    self.check("\"null\"")

  def testBoolean(self):
    self.check("\"boolean\"")

  def testString(self):
    self.check("\"string\"")

  def testBytes(self):
    self.check("\"bytes\"")

  def testInt(self):
    self.check("\"int\"")

  def testLong(self):
    self.check("\"long\"")

  def testFloat(self):
    self.check("\"float\"")

  def testDouble(self):
    self.check("\"double\"")

  def testArray(self):
    self.check("{\"type\":\"array\", \"items\": \"long\"}")

  def testMap(self):
    self.check("{\"type\":\"map\", \"values\": \"string\"}")

  def testRecord(self):
    self.check("{\"type\":\"record\", \"name\":\"Test\"," +
               "\"fields\":[{\"name\":\"f\", \"type\":" +
               "\"string\"}, {\"name\":\"fb\", \"type\":\"bytes\"}]}")

  def testEnum(self):
    self.check("{\"type\": \"enum\", \"name\":\"Test\","+
               "\"symbols\": [\"A\", \"B\"]}")

  def testRecursive(self):
    self.check("{\"type\": \"record\", \"name\": \"Node\", \"fields\": ["
          +"{\"name\":\"label\", \"type\":\"string\"},"
          +"{\"name\":\"children\", \"type\":"
          +"{\"type\": \"array\", \"items\": \"Node\" }}]}")

  def testLisp(self):
    self.check("{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": ["
          +"{\"name\":\"value\", \"type\":[\"null\", \"string\","
          +"{\"type\": \"record\", \"name\": \"Cons\", \"fields\": ["
          +"{\"name\":\"car\", \"type\":\"Lisp\"},"
          +"{\"name\":\"cdr\", \"type\":\"Lisp\"}]}]}]}")

  def testUnion(self):
    self.check("[\"string\", \"null\", \"long\", "
      +"{\"type\": \"record\", \"name\": \"Cons\", \"fields\": ["
      +"{\"name\":\"car\", \"type\":\"string\"}," 
      +"{\"name\":\"cdr\", \"type\":\"string\"}]}]")

  def testFixed(self):
    self.check("{\"type\": \"fixed\", \"name\":\"Test\", \"size\": 1}") 

  def check(self, string):
    schm = schema.parse(string)
    st = schema.stringval(schm)
    self.assertEquals(string.replace(" ",""), st.replace(" ",""))
    #test __eq__
    self.assertEquals(schm, schema.parse(string))
    #test hashcode doesn't generate infinite recursion
    schm.__hash__()
    randomdata = self.__random(schm)
    for i in range(1,10):
      self.checkser(schm, randomdata)
    self.checkdatafile(schm)

  def checkser(self, schm, randomdata):
    datum = randomdata.next()
    self.assertTrue(self.__validator(schm, datum))
    w = self.__datumwriter(schm)
    writer = cStringIO.StringIO()
    w.write(datum, io.ValueWriter(writer))
    r = self.__datumreader(schm)
    reader = cStringIO.StringIO(writer.getvalue())
    ob = r.read(io.ValueReader(reader))
    if self.__assertdata:
      self.assertEquals(datum, ob)

  def checkdatafile(self, schm):
    seed = time.time()
    randomData = self.__random(schm, seed)
    count = 10
    dw = io.DataFileWriter(schm, open(_FILE, 'wb'), self.__datumwriter())
    for i in range(0,count):
      dw.append(randomData.next())
    dw.close()
    randomData = self.__random(schm, seed)
    dr = io.DataFileReader(open(_FILE, "rb"), self.__datumreader())
    for i in range(0,count):
      data = randomData.next()
      if self.__assertdata:
        self.assertEquals(data, dr.next())

if __name__ == '__main__':
  if len(sys.argv) != 4:
    print "Usage: testio.py <schemafile> <outputfile> <count>"
    exit
  schm = schema.parse(open(sys.argv[1]).read())
  file = sys.argv[2]
  count = int(sys.argv[3])
  randomData = RandomData(schm)
  dw = io.DataFileWriter(schm, open(file, 'wb'), generic.DatumWriter())
  for i in range(0,count):
    dw.append(randomData.next())
  dw.close()

