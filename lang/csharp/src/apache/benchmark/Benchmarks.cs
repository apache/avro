/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System.Collections.Generic;
using System.IO;
using BenchmarkDotNet.Attributes;
using Avro.Generic;
using Avro.IO;
using Avro.Specific;

namespace Avro.Benchmark
{
    public class Benchmarks
    {
        private const int _numberOfRecordsInAvro = 100;

        private readonly string _schemaStrSmall;
        private readonly string _schemaStrBig;

        private readonly RecordSchema _schemaSmall;
        private readonly RecordSchema _schemaBig;
        private readonly RecordSchema _schemaAddress;

        private readonly byte[] _avroGenericSmall;
        private readonly byte[] _avroGenericBig;
        private readonly byte[] _avroSpecificSmall;
        private readonly byte[] _avroSpecificBig;

        public Benchmarks()
        {
            _schemaStrSmall = System.IO.File.ReadAllText("schema/small.avsc");
            _schemaStrBig = System.IO.File.ReadAllText("schema/big.avsc");

            _schemaSmall = (RecordSchema)Schema.Parse(_schemaStrSmall);
            _schemaBig = (RecordSchema)Schema.Parse(_schemaStrBig);
            _schemaAddress = (RecordSchema)_schemaBig["address"].Schema;

            // Create avro for reading benchmarking
            _avroGenericSmall = GenericRecordsToAvro(CreateGenericRecordSmall());
            _avroGenericBig = GenericRecordsToAvro(CreateGenericRecordBig());

            _avroSpecificSmall = SpecificRecordsToAvro(CreateSpecificRecordSmall());
            _avroSpecificBig = SpecificRecordsToAvro(CreateSpecificRecordBig());
        }

        private byte[] GenericRecordsToAvro(GenericRecord record)
        {
            using (MemoryStream outputStream = new MemoryStream())
            {
                GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.Schema);
                BinaryEncoder encoder = new BinaryEncoder(outputStream);

                for (int i = 0; i < _numberOfRecordsInAvro; i++)
                {
                    writer.Write(record, encoder);
                }

                encoder.Flush();

                return outputStream.ToArray();
            }
        }

        private IList<GenericRecord> AvroToGenericRecordsToAvro(byte[] avro, RecordSchema schema)
        {
            using (MemoryStream inputStream = new MemoryStream(avro))
            {
                GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema, schema);
                BinaryDecoder decoder = new BinaryDecoder(inputStream);
                List<GenericRecord> records = new List<GenericRecord>();

                for (int i = 0; i < _numberOfRecordsInAvro; i++)
                {
                    GenericRecord record = reader.Read(null, decoder);
                    if (record == null)
                        break;
                    records.Add(record);
                }

                return records;
            }
        }

        private byte[] SpecificRecordsToAvro<T>(T record) where T : ISpecificRecord
        {
            using (MemoryStream outputStream = new MemoryStream())
            {
                SpecificDatumWriter<T> writer = new SpecificDatumWriter<T>(record.Schema);
                BinaryEncoder encoder = new BinaryEncoder(outputStream);

                for (int i = 0; i < _numberOfRecordsInAvro; i++)
                {
                    writer.Write(record, encoder);
                }

                encoder.Flush();

                return outputStream.ToArray();
            }
        }

        private IList<T> AvroToSpecificRecords<T>(byte[] avro, RecordSchema schema) where T : ISpecificRecord
        {
            using (MemoryStream inputStream = new MemoryStream(avro))
            {
                SpecificDatumReader<T> reader = new SpecificDatumReader<T>(schema, schema);
                BinaryDecoder decoder = new BinaryDecoder(inputStream);
                List<T> records = new List<T>();

                for (int i = 0; i < _numberOfRecordsInAvro; i++)
                {
                    T record = reader.Read(default, decoder); ;
                    if (record == null)
                        break;
                    records.Add(record);
                }

                return records;
            }
        }

        [Benchmark]
        public void ParseSchemaSmall()
        {
            Schema.Parse(_schemaStrSmall);
        }

        [Benchmark]
        public void ParseSchemaBig()
        {
            Schema.Parse(_schemaStrBig);
        }

        [Benchmark]
        public GenericRecord CreateGenericRecordSmall()
        {
            GenericRecord record = new GenericRecord(_schemaSmall);
            record.Add("field", "foo");

            return record;
        }

        [Benchmark]
        public GenericRecord CreateGenericRecordBig()
        {
            GenericRecord address = new GenericRecord(_schemaAddress);
            address.Add("street", "street");
            address.Add("city", "city");
            address.Add("state_prov", "state_prov");
            address.Add("country", "country");
            address.Add("zip", "zip");

            GenericRecord record = new GenericRecord(_schemaBig);
            record.Add("username", "username");
            record.Add("age", 10);
            record.Add("phone", "000000000");
            record.Add("housenum", "0000");
            record.Add("address", address);

            return record;
        }

        [Benchmark]
        public ISpecificRecord CreateSpecificRecordSmall()
        {
            return new com.benchmark.small.test()
            {
                field = "foo"
            };
        }

        [Benchmark]
        public ISpecificRecord CreateSpecificRecordBig()
        {
            return new com.benchmark.big.userInfo()
            {
                username = "username",
                age = 10,
                phone = "000000000",
                housenum = "0000",
                address = new com.benchmark.big.mailing_address()
                {
                    street = "street",
                    city = "city",
                    state_prov = "state_prov",
                    country = "country",
                    zip = "zip"
                }
            };
        }

        [Benchmark]
        public void GenericRecordsToAvroSmall()
        {
            GenericRecordsToAvro(CreateGenericRecordSmall());
        }

        [Benchmark]
        public void GenericRecordsToAvroBig()
        {
            GenericRecordsToAvro(CreateGenericRecordBig());
        }

        [Benchmark]
        public void AvroToGenericRecordsSmall()
        {
            AvroToGenericRecordsToAvro(_avroGenericSmall, _schemaSmall);
        }

        [Benchmark]
        public void AvroToGenericRecordsBig()
        {
            AvroToGenericRecordsToAvro(_avroGenericBig, _schemaBig);
        }

        [Benchmark]
        public void SpecificRecordsToAvroSmall()
        {
            SpecificRecordsToAvro(CreateSpecificRecordSmall());
        }

        [Benchmark]
        public void SpecificRecordsToAvroBig()
        {
            SpecificRecordsToAvro(CreateSpecificRecordBig());
        }

        [Benchmark]
        public void AvroToSpecificRecordsSmall()
        {
            AvroToSpecificRecords<com.benchmark.small.test>(_avroSpecificSmall, _schemaSmall);
        }

        [Benchmark]
        public void AvroToSpecificRecordsBig()
        {
            AvroToSpecificRecords<com.benchmark.big.userInfo>(_avroSpecificBig, _schemaBig);
        }
    }
}
