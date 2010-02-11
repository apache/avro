import sys
from avro import schema
from avro import io
from avro import datafile

DATUM = {
  'intField': 12,
  'longField': 15234324L,
  'stringField': unicode('hey'),
  'boolField': True,
  'floatField': 1234.0,
  'doubleField': -1234.0,
  'bytesField': '12312adf',
  'nullField': None,
  'arrayField': [5.0, 0.0, 12.0],
  'mapField': {'a': {'label': 'a'}, 'bee': {'label': 'cee'}},
  'unionField': 12.0,
  'enumField': 'C',
  'fixedField': '1019181716151413',
  'recordField': {'label': 'blah', 'children': [{'label': 'inner', 'children': []}]},
}

if __name__ == "__main__":
  interop_schema = schema.parse(open(sys.argv[1], 'r').read())
  writer = open(sys.argv[2], 'wb')
  datum_writer = io.DatumWriter()
  # NB: not using compression
  dfw = datafile.DataFileWriter(writer, datum_writer, interop_schema)
  dfw.append(DATUM)
  dfw.close()
