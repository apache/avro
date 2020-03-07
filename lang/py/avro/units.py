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

"""Representation of unit analysis definitions.
"""

import copy
import json
import re

from avro import schema


class UnitException(schema.AvroException):
  pass

class UnitParseException(schema.AvroException):
  pass

class UnitAnalysisDB(object):
  """Encodes a database of unit definitions for resolving units in schemas.
  """

  def __init__(self, udefs):
    """Initializes a unit analysis db object.
    """
    self._unit_map = {}
    for ud in udefs:
      self._unit_map[ud.name] = ud

  def __getitem__(self, name):
    if name not in self._unit_map:
      raise UnitException('unit definition for "%s" does not exist' % (name))
    return self._unit_map[name]

  def __len__(self):
    return len(self._unit_map)

  def __str__(self):
    return str(self._unit_map)

  def __repr__(self):
    return str(self)

  def canonical(self, expr):
    """Compute the canonical form of a unit expression, as described in:
       http://erikerlandson.github.io/blog/2019/05/03/algorithmic-unit-analysis/
    """
    if isinstance(expr, Unitless):
      return CanonicalUnit(UnitCoef(1), {})
    elif isinstance(expr, UnitName):
      udef = self[expr.name]
      if isinstance(udef, BaseUnitDef):
        return CanonicalUnit(UnitCoef(1), { expr.name: 1 })
      elif isinstance(udef, DerivedUnitDef):
        cexpr = self.canonical(udef.expr)
        return CanonicalUnit(udef.coef.mul(cexpr.coef), cexpr.map)
    elif isinstance(expr, UnitMul):
      return (self.canonical(expr.lhs)).mul(self.canonical(expr.rhs))
    elif isinstance(expr, UnitDiv):
      return (self.canonical(expr.lhs)).div(self.canonical(expr.rhs))
    elif isinstance(expr, UnitPow):
      return (self.canonical(expr.lhs)).pow(expr.rhs)
    raise UnitException('Unrecognized unit expr "%s"' % (expr))

  def coefficient(self, uexpr1, uexpr2):
    """if uexpr1 and uexpr2 are compatible (aka convertable),
       return the coefficient of conversion, otherwise None
    """
    tcf = self.canonical(UnitDiv(uexpr1, uexpr2))
    if len(tcf.map.items()) == 0:
      # If the canonical forms cancel, then we know that:
      # 1) uexpr1 and uexpr2 represent convertable unit expressions
      # 2) the residual coefficient value is the conversion factor
      return tcf.coef
    else:
      return None

  def resolve_schemas(self, writer_schema, reader_schema):
    """Returns True if and only if unit annotations are compatible.
       If successful, adds conversion factors to schema.
       This function assumes both schema have already passed standard
       DatumReader.match_schema checking.
    """
    wsid = str(id(writer_schema))
    if hasattr(reader_schema, wsid):
      # if we've already resolved units against this write-schema, then
      # we don't have to do it again.
      return True
    if reader_schema.type == 'record':
      # For records we attempt to resolve units on any numeric fields.
      for (rfname, rf) in reader_schema.fields_dict.items():
        # If the reader field has no unit spec, we don't care about
        # trying to resolve it. Other policies might be supported,
        # for example an error or warning if corresponding fields do not
        # both have unit annotations.
        if "unit" not in rf.props: continue
        # Fields that were not in the write schema do not require resolving.
        if rfname not in writer_schema.fields_dict: continue
        wf = writer_schema.fields_dict[rfname]
        if "unit" not in wf.props: continue
        # Unit resolutions are only defined for numeric field types.
        if (rf.props['type']).type not in {'float', 'double', 'int', 'long'}:
          # possible policies: warning or error here
          continue
        # if units are compatible, then we'll get a coefficient of conversion:
        ruexpr = _parse_unit_expr(rf.props["unit"])
        wuexpr = _parse_unit_expr(wf.props["unit"])
        coef = self.coefficient(wuexpr, ruexpr)
        if coef is None:
          # throwing an exception here might make returning true/false irrelevant
          raise UnitException('Field %s failed unit resolution during schema matching' % (rfname))
        rf.props[wsid] = coef
      # tag this write schema so we don't need to resolve units in the future
      setattr(reader_schema, wsid, True)
      return True
    return False

class CanonicalUnit(object):
  def __init__(self, coef, cumap):
    if not isinstance(coef, UnitCoef):
      raise UnitException('coef was not a UnitCoef: "%s"' % (coef))
    if not isinstance(cumap, dict):
      raise UnitException('cumap was not a dict: "%s"' % (cumap))
    self._coef = coef
    self._map = cumap

  coef = property(lambda self: self._coef)
  map = property(lambda self: self._map)

  def mul(self, rhs):
    lmap = copy.deepcopy(self.map)
    rmap = rhs.map
    for (u, re) in rmap.items():
      e = (lmap.get(u) or 0) + re
      if e == 0:
        if u in lmap: del lmap[u]
      else:
        lmap[u] = e
    return CanonicalUnit(self.coef.mul(rhs.coef), lmap)

  def div(self, rhs):
    lmap = copy.deepcopy(self.map)
    rmap = rhs.map
    for (u, re) in rmap.items():
      e = (lmap.get(u) or 0) - re
      if e == 0:
        if u in lmap: del lmap[u]
      else:
        lmap[u] = e
    return CanonicalUnit(self.coef.div(rhs.coef), lmap)

  def pow(self, exp):
    if exp == 0:
      return CanonicalUnit(1.0, {})
    lmap = copy.deepcopy(self.map)
    if exp == 1:
      return CanonicalUnit(self.coef, lmap)
    for u in lmap.keys():
      lmap[u] = exp * lmap[u]
    return CanonicalUnit(self.coef.pow(exp), lmap)

  def __str__(self):
    return "CanonicalUnit(%s, %s)" % (self.coef, self.map)

  def __repr__(self):
    return str(self)

class UnitDef(object):
  def __init__(self, prop, name, abbv):
    self._prop = prop
    self._name = name
    self._abbv = abbv

  prop = property(lambda self: self._prop)
  name = property(lambda self: self._name)
  abbv = property(lambda self: self._abbv)

  def __repr__(self):
    return str(self)

class BaseUnitDef(UnitDef):
  def __init__(self, prop, name, abbv):
    super().__init__(prop, name, abbv)

  def __str__(self):
    return "BaseUnitDef(%s, %s)" % (self.name, self.abbv)

class DerivedUnitDef(UnitDef):
  def __init__(self, prop, name, abbv, coef, expr):
    super().__init__(prop, name, abbv)
    self._coef = coef
    self._expr = expr

  coef = property(lambda self: self._coef)
  expr = property(lambda self: self._expr)

  def __str__(self):
    return "DerivedUnitDef(%s, %s, %s, %s)" % \
      (self.name, self.abbv, self.coef, self.expr)

class UnitExpr(object):
  def __init__(self):
    pass

  def __repr__(self):
    return str(self)

class Unitless(UnitExpr):
  def __init__(self):
    super().__init__()

  def __str__(self):
    return "Unitless"

class UnitName(UnitExpr):
  def __init__(self, name):
    super().__init__()
    self._name = name

  name = property(lambda self: self._name)

  def __str__(self):
    return "UnitName(%s)" % (self.name)

class UnitMul(UnitExpr):
  def __init__(self, lhs, rhs):
    super().__init__()
    self._lhs = lhs
    self._rhs = rhs

  lhs = property(lambda self: self._lhs)
  rhs = property(lambda self: self._rhs)

  def __str__(self):
    return "UnitMul(%s, %s)" % (self.lhs, self.rhs)

class UnitDiv(UnitExpr):
  def __init__(self, lhs, rhs):
    super().__init__()
    self._lhs = lhs
    self._rhs = rhs

  lhs = property(lambda self: self._lhs)
  rhs = property(lambda self: self._rhs)

  def __str__(self):
    return "UnitDiv(%s, %s)" % (self.lhs, self.rhs)

class UnitPow(UnitExpr):
  def __init__(self, lhs, rhs):
    super().__init__()
    self._lhs = lhs
    self._rhs = rhs

  lhs = property(lambda self: self._lhs)
  rhs = property(lambda self: self._rhs)

  def __str__(self):
    return "UnitPow(%s, %s)" % (self.lhs, self.rhs)

class UnitCoef(object):
  def __init__(self, coef):
    try:
      self._coef = float(coef)
    except Exception as e:
      # I'd like to support float, Decimal and Fraction eventually.
      # I want to think through what that means for users specifying
      # unit conversion policies, how these look in json spec, etc.
      raise UnitException('Failed to store unit coef as float: "%s"' % (coef))

  as_float = property(lambda self: float(self._coef))

  def mul(self, rhs):
    if not isinstance(rhs, UnitCoef):
      raise UnitException('rhs was not a UnitCoef: "%s"' % (rhs))
    return UnitCoef(self.as_float * rhs.as_float)

  def div(self, rhs):
    if not isinstance(rhs, UnitCoef):
      raise UnitException('rhs was not a UnitCoef: "%s"' % (rhs))
    return UnitCoef(self.as_float / rhs.as_float)

  def pow(self, exp):
    if not isinstance(exp, int):
      raise UnitException('exp was not an integer: "%s"' % (exp))
    return UnitCoef(pow(self.as_float, exp))

  def __str__(self):
    return "UnitCoef(%s)" % (self._coef)

  def __repr__(self):
    return str(self)

def parse(udb_json):
  """Parse a json string (or list of json strings) into a unit analysis db
  """
  if isinstance(udb_json, list):
    udefs = []
    for udbj in udb_json:
      udefs.extend(_parse_unit_def_list(udbj))
  else:
    udefs = _parse_unit_def_list(udb_json)
  return UnitAnalysisDB(udefs)

def _parse_unit_def_list(obj):
  try:
    json_data = json.loads(obj)
  except Exception as e:
    raise UnitParseException('Unit definition parsing expects a valid JSON string: "%s"' % (obj))
  if not isinstance(json_data, list):
    raise UnitParseException('Avro unit definitions must be a list')
  return [_parse_unit_def(e) for e in json_data]

def _parse_unit_def(obj):
  if not isinstance(obj, dict):
    raise UnitParseException('Expecting a dict, got: %r' % (obj))
  utype = obj.get("unit")
  if utype == "base":
    if "name" not in obj:
      raise UnitParseException('Unit definitions must include unit name')
    uname = _parse_unit_name(obj["name"])
    uabbv = _parse_unit_name(obj.get("abbv") or uname.name)
    return BaseUnitDef(obj, uname.name, uabbv.name)
  elif utype == "derived":
    if "name" not in obj:
      raise UnitParseException('Unit definitions must include unit name')
    uname = _parse_unit_name(obj["name"])
    uabbv = _parse_unit_name(obj.get("abbv") or uname.name)
    if "coef" not in obj:
      raise UnitParseException('Derived unit must include unit coef')
    ucoef = _parse_unit_coef(obj["coef"])
    if "expr" not in obj:
      raise UnitParseException('Derived unit must include unit expr')
    uexpr = _parse_unit_expr(obj["expr"])
    return DerivedUnitDef(obj, uname.name, uabbv.name, ucoef, uexpr)
  raise UnitParseException('Unrecognized unit type: "%s"' % (utype))

UNIT_NAME_REGEX = re.compile("^([a-z]|[A-Z])([a-z]|[A-Z]|[0-9])*$")

UNITLESS_KEYWORD = "unitless"

def _parse_unit_expr(obj):
  if obj == UNITLESS_KEYWORD:
    return Unitless()
  elif isinstance(obj, str):
    return _parse_unit_name(obj)
  elif isinstance(obj, dict) and "op" in obj:
    for k in ["lhs", "rhs"]:
      if k not in obj:
        raise UnitParseException('Missing "%s" from unit expression' % (k))
    op = obj["op"]
    if op == "*":
      return UnitMul(_parse_unit_expr(obj["lhs"]), _parse_unit_expr(obj["rhs"]))
    elif op == "/":
      return UnitDiv(_parse_unit_expr(obj["lhs"]), _parse_unit_expr(obj["rhs"]))
    elif op == "^":
      try:
        exp = int(obj["rhs"])
      except Exception as e:
        raise UnitParseException('exponent was not an integer: "%s"' % (obj["rhs"]))
      return UnitPow(_parse_unit_expr(obj["lhs"]), exp)
  raise UnitParseException('Bad unit expression object: "%s"' % (obj))

def _parse_unit_coef(obj):
  if isinstance(obj, dict):
    for k in ["coef", "num", "den"]:
      if k not in obj:
        raise UnitParseException('Unit coefficient missing key "%s"' % (k))
    if obj["coef"] != "rational":
      raise UnitParseException('expecting "rational" for "coef"')
    numval = obj["num"]
    if not isinstance(numval, int) or numval <= 0:
      raise UnitParseException('Unit coefficient numerator must be a positive integer')
    denval = obj["den"]
    if not isinstance(denval, int) or denval <= 0:
      raise UnitParseException('Unit coefficient denominator must be a positive integer')
    try:
      # Currently not supporting lossless rational coefficients
      # Instead just turn it into a floating point value for now
      coef = float(numval) / float(denval)
    except Exception as e:
      raise UnitParseException('Numeric error parsing rational unit coefficient')
    return UnitCoef(coef)
  else:
    # For now I'll take anything that can be cast to a floating point value
    try:
      coef = float(obj)
    except Exception as e:
      raise UnitParseException('Unrecognized unit coefficient "%r"' % (obj))
    if coef <= 0.0:
      raise UnitParseException('Unit coefficient was not positive: "%r"' % (obj))
    return UnitCoef(coef)

def _parse_unit_name(obj):
  if not isinstance(obj, str):
    raise UnitParseException('Unit names must be a string')
  if obj == UNITLESS_KEYWORD:
    raise UnitParseException('Unit name may not be "%s"' % (UNITLESS_KEYWORD))
  if UNIT_NAME_REGEX.fullmatch(obj):
    return UnitName(obj)
  raise UnitParseException('Bad unit name format: "%s"' % (obj))
