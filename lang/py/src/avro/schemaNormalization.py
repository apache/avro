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
Contains the SchemaNormalization class"""


try:
  import json
except ImportError:
  import simplejson as json
  
import hashlib
from avro import schema

class NoSuchAlgorithmException(Exception):
    pass

class SchemaNormalization:
    def __init__(self):
        #Initialize EMPTY64 and FP_TABLE
        self.EMPTY64 = 0xc15d213aa4d7a795        
        self.FP_TABLE=[]
    
        for i  in xrange (0,256):
          fp = i;
          for j in xrange(0,8):
            mask = -(fp & 1L);
            fp = (fp >> 1) ^ (self.EMPTY64 & mask);
          self.FP_TABLE.append(fp)

        self.env={}


    def toParsingForm(self, schema):
        """
        Transform schema instance to its canonical json string .
          @arg schema: avro schema instance
          @return: the canonical form of the avro schema
        """
        self.env={}
        return self._build(schema.to_json());

    def fingerprint(self,fpName, data):
        """
        Process hash value of data using custom hash algorithm
          @arg fpName: hash algorithm : CRC-64-AVRO or MD5 or SHA-1 or SHA-224 or SHA-256 or SHA-384 or SHA-512
          @return: hash value in hexa
        """
        if (fpName.lower() == "crc-64-avro"):
          fp = self.fingerprint64(data);
          return fp
    
        m = None
        if(fpName.lower() == "md5") :        m = hashlib.md5()
        elif(fpName.lower() == "sha-1") :     m = hashlib.sha1()
        elif(fpName.lower() == "sha-224") :   m = hashlib.sha224()
        elif(fpName.lower() == "sha-256") :  m = hashlib.sha256()
        elif(fpName.lower() == "sha-384") :  m = hashlib.sha384()
        elif(fpName.lower() == "sha-512") :  m = hashlib.sha512()
        
        if m is not None:
            m.update(data)
            return m.hexdigest();
        else:
            raise NoSuchAlgorithmException("Unknown hash algorithm : '%s'"%(fpName))


    def parsingFingerprint(self,fpName, schema):
        """
        Compute hash value of a schema canonical string using custom hash algorithm
          @arg schema: avro schema instance
          @return: the hash value of the canonical schema string
        """
        return self.fingerprint(fpName, self.toParsingForm(schema))

    def parsingFingerprint64(self,schema):
        """
        Compute hash value of a schema canonical string using CRC-64-AVRO hash algorithm
          @arg schema: avro schema instance
          @return: the hash value of the canonical schema string
        """
        return self.fingerprint64(self.toParsingForm(schema))
    
    def fingerprint64(self,data):
        """
        Compute CRC-64-AVRO hash algorithm for a string
          @arg data: string
          @return: the hash value of the string
        """
        result = self.EMPTY64;
        for b in data:
          result = (result >> 8) ^ self.FP_TABLE[(result ^ ord(b)) & 0xff];
        return hex(result)

    def _build(self,jsonobj):
        """
        Create the canonical avro schema string (json) from a complete avro schema string
        @arg json: json string representing an avro shema
        @arg env: environment dict 
        @return: canonical avro schema string
        """
        

        #print jsonobj
        #print type(jsonobj)
             
        o=""
        firstTime = True;
        st=""

        if jsonobj is None:
          st='"null"'
        elif type(jsonobj)==unicode:
          st=jsonobj
        elif type(jsonobj)==dict:
          st = jsonobj[u'type']
          if type(st)==dict:
            return self._build(jsonobj[u'type'])
          elif type(st)==list:
            return self._build(jsonobj[u'type'])
        elif type(jsonobj)==list:
          st = u'union'
        

        if(st==u"union"):
          o+='['
          for b in jsonobj:
            if not firstTime:
              o+=','
            else :
              firstTime = False;
            o+=self._build(b)
          o+=']'
          return o

        elif st in [u"array",u"map"]:
          o+="{\"type\":\""+st+"\""
          if st == u"array":
            o+=",\"items\":"
            o+=self._build( jsonobj[u"items"])
          else:
            o+=",\"values\":"
            o+=self._build( jsonobj[u"values"])
          o+="}"
          return o
        elif st in [u"enum",u"fixed",u"record",u"error"]:
          name = jsonobj[u"name"]
          
          if u"namespace" in jsonobj and len(jsonobj[u"namespace"])>0:
            #schema has a namespace
            if (name.find('.') > 0):
              #name already have a qualified name with namespace
              pass
            else:
              name=jsonobj[u"namespace"]+"."+name# namespace + full name
                
                
            
          if name in self.env:
            o+=self.env[name]
            return o;
                     
          qname = "\""+name+"\"";
          self.env[name] = qname;
          o+="{\"name\":"+qname;
          o+=",\"type\":\""+st+"\"";
          if st == u"enum":
            o+=",\"symbols\":[";
            for enumSymbol in jsonobj[u"symbols"]:
              if not firstTime:
                o+=',';
              else :
                firstTime = False;
              o+='"'+enumSymbol+'"';
            o+="]";
          elif st == u"fixed":
            o+=",\"size\":"+str(jsonobj[u"size"]);
          else :# st == u"record":
            o+=",\"fields\":[";
            for  f in  jsonobj[u"fields"]:
              if not firstTime:
                  o+=',';
              else:
                  firstTime = False;
              o+="{\"name\":\""+f[u"name"]+"\"";
              o+=",\"type\":"
              o+=self._build(f)
              o+="}";
            o+="]";
        elif st in schema.PRIMITIVE_TYPES:
          # boolean, bytes, double, float, int, long, null, string
          o+='"%s"'%(st)
          return o
        elif st in self.env:
          o+='"%s"'%(st)
          return o
        o+="}"
        return o
