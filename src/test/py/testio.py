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

import unittest, random, cStringIO, time, sys, os, struct
import avro.schema as schema
import avro.io as io
import avro.genericio as genericio
import avro.datafile as datafile

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
      for field in schm.getfields().values():
        m[field.getname()] = self.nextdata(field.getschema(), d+1)
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

  def __init__(self, methodName, validator=genericio.validate,
                               dwriter=genericio.DatumWriter, 
                               dreader=genericio.DatumReader, random=RandomData,
                               assertdata=True):
    unittest.TestCase.__init__(self, methodName)
    self.__validator = validator
    self.__datumwriter = dwriter
    self.__datumreader = dreader
    self.__random = random
    self.__assertdata = assertdata

  def testNull(self):
    self.checkdefault("\"null\"", "null", None)

  def testBoolean(self):
    self.checkdefault("\"boolean\"", "true", True)

  def testString(self):
    self.checkdefault("\"string\"", "\"foo\"", "foo")

  def testBytes(self):
    self.checkdefault("\"bytes\"", "\"foo\"", "foo")

  def testInt(self):
    self.checkdefault("\"int\"", "5", 5)

  def testLong(self):
    self.checkdefault("\"long\"", "9", 9)

  def testFloat(self):
    self.checkdefault("\"float\"", "1.2", float(1.2))

  def testDouble(self):
    self.checkdefault("\"double\"", "1.2", float(1.2))

  def testArray(self):
    self.checkdefault("{\"type\":\"array\", \"items\": \"long\"}",
                       "[1]", [1])

  def testMap(self):
    self.checkdefault("{\"type\":\"map\", \"values\": \"long\"}",
                      "{\"a\":1}", {unicode("a"):1})

  def testRecord(self):
    self.checkdefault("{\"type\":\"record\", \"name\":\"Test\"," +
               "\"fields\":[{\"name\":\"f\", \"type\":" +
               "\"long\"}]}", "{\"f\":11}", {"f" : 11})

  def testEnum(self):
    self.checkdefault("{\"type\": \"enum\", \"name\":\"Test\","+
               "\"symbols\": [\"A\", \"B\"]}", "\"B\"", "B")

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
    self.checkdefault("[\"double\", \"long\"]", "1.1", 1.1)

  def testFixed(self):
    self.checkdefault("{\"type\": \"fixed\", \"name\":\"Test\", \"size\": 1}", 
                      "\"a\"", "a") 

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

  def checkdefault(self, schemajson, defaultjson, defaultvalue):
    self.check(schemajson)
    actual = schema.parse("{\"type\":\"record\", \"name\":\"Foo\","
                          + "\"fields\":[]}")
    expected = schema.parse("{\"type\":\"record\", \"name\":\"Foo\"," 
                             +"\"fields\":[{\"name\":\"f\", "
                             +"\"type\":"+schemajson+", "
                             +"\"default\":"+defaultjson+"}]}")
    reader = genericio.DatumReader(actual, expected)
    record = reader.read(io.Decoder(cStringIO.StringIO()))
    self.assertEquals(defaultvalue, record.get("f"))
    #FIXME fix to string for default values
    #self.assertEquals(expected, schema.parse(schema.stringval(expected)))

  def checkser(self, schm, randomdata):
    datum = randomdata.next()
    self.assertTrue(self.__validator(schm, datum))
    w = self.__datumwriter(schm)
    writer = cStringIO.StringIO()
    w.write(datum, io.Encoder(writer))
    r = self.__datumreader(schm)
    reader = cStringIO.StringIO(writer.getvalue())
    ob = r.read(io.Decoder(reader))
    if self.__assertdata:
      self.assertEquals(datum, ob)

  def checkdatafile(self, schm):
    seed = time.time()
    randomData = self.__random(schm, seed)
    count = 10
    dw = datafile.DataFileWriter(schm, open(_FILE, 'wb'), self.__datumwriter())
    for i in range(0,count):
      dw.append(randomData.next())
    dw.close()
    randomData = self.__random(schm, seed)
    dr = datafile.DataFileReader(open(_FILE, "rb"), self.__datumreader())
    count_read = 0
    for data in dr:
      count_read = count_read + 1
      if self.__assertdata:
        self.assertEquals(randomData.next(), data)
    self.assertEquals(count, count_read)

if __name__ == '__main__':
  if len(sys.argv) != 4:
    print "Usage: testio.py <schemafile> <outputfile> <count>"
    sys.exit(1)
  schm = schema.parse(open(sys.argv[1]).read())
  file = sys.argv[2]
  count = int(sys.argv[3])
  randomData = RandomData(schm)
  dw = datafile.DataFileWriter(schm, open(file, 'wb'), genericio.DatumWriter())
  for i in range(0,count):
    dw.append(randomData.next())
  dw.close()

