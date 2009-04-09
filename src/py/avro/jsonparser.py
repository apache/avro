"""Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from odict import OrderedDict
import simplejson
from simplejson import decoder

def OrderedJSONObject((s, end), encoding, strict, scan_once, object_hook, 
                     _w=decoder.WHITESPACE.match, _ws=decoder.WHITESPACE_STR):
  pairs = OrderedDict()
  # Use a slice to prevent IndexError from being raised, the following
  # check will raise a more specific ValueError if the string is empty
  nextchar = s[end:end + 1]
  # Normally we expect nextchar == '"'
  if nextchar != '"':
      if nextchar in _ws:
          end = _w(s, end).end()
          nextchar = s[end:end + 1]
      # Trivial empty object
      if nextchar == '}':
          return pairs, end + 1
      elif nextchar != '"':
          raise ValueError(decoder.errmsg("Expecting property name", s, end))
  end += 1
  while True:
      key, end = decoder.scanstring(s, end, encoding, strict)

      # To skip some function call overhead we optimize the fast paths where
      # the JSON key separator is ": " or just ":".
      if s[end:end + 1] != ':':
          end = _w(s, end).end()
          if s[end:end + 1] != ':':
              raise ValueError(decoder.errmsg("Expecting : delimiter", s, end))

      end += 1

      try:
          if s[end] in _ws:
              end += 1
              if s[end] in _ws:
                  end = _w(s, end + 1).end()
      except IndexError:
          pass

      try:
          value, end = scan_once(s, end)
      except StopIteration:
          raise ValueError(decoder.errmsg("Expecting object", s, end))
      pairs[key] = value

      try:
          nextchar = s[end]
          if nextchar in _ws:
              end = _w(s, end + 1).end()
              nextchar = s[end]
      except IndexError:
          nextchar = ''
      end += 1

      if nextchar == '}':
          break
      elif nextchar != ',':
          raise ValueError(decoder.errmsg("Expecting , delimiter", s, end - 1))

      try:
          nextchar = s[end]
          if nextchar in _ws:
              end += 1
              nextchar = s[end]
              if nextchar in _ws:
                  end = _w(s, end + 1).end()
                  nextchar = s[end]
      except IndexError:
          nextchar = ''

      end += 1
      if nextchar != '"':
          raise ValueError(decoder.errmsg("Expecting property name", s, end - 1))

  if object_hook is not None:
      pairs = object_hook(pairs)
  return pairs, end

class OrderedDecoder(decoder.JSONDecoder):

  def __init__(self, encoding=None):
    decoder.JSONDecoder.__init__(self, encoding)
    self.parse_object = OrderedJSONObject
    self.scan_once = decoder.make_scanner(self)

simplejson._default_decoder = OrderedDecoder()

def parse(s):
  return simplejson.loads(s)