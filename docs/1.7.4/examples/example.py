import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schema = avro.schema.parse(open("user.avsc").read())

writer = DataFileWriter(open("/tmp/users.avro", "w"), DatumWriter(), schema)
writer.append({"name": "Alyssa", "favorite_number": 256, "WTF": 2})
writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
writer.close()

reader = DataFileReader(open("/tmp/users.avro", "r"), DatumReader())
for user in reader:
    print user
reader.close()
