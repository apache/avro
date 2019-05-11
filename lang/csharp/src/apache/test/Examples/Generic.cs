using Avro.File;
using Avro.Generic;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;

namespace Avro.test.Examples
{
    /// <summary>
    /// This class contains example usages of the Avro.Generic namespace.
    /// </summary>
    [TestFixture]
    [Parallelizable(ParallelScope.All)]
    public class Generic
    {
        /// <summary>
        /// Create some <see cref="GenericRecord"/>s containing weather reports and write them to
        /// an Avro data file using the <see cref="DataFileWriter{T}"/>.
        ///
        /// In this example we are creating an Avro data file whose content matches that of the
        /// `weather.json` sample file.
        /// </summary>
        [Test]
        public void Generic_DataFileWriter()
        {
            var schemaPath = GetExampleContentPath("weather.avsc");

            // Load the schema used for weather report records.
            var schema = (RecordSchema)Schema.Parse(System.IO.File.ReadAllText(schemaPath));

            // Create each record and populate its fields.
            var record0 = new GenericRecord(schema);
            var record1 = new GenericRecord(schema);
            var record2 = new GenericRecord(schema);
            var record3 = new GenericRecord(schema);
            var record4 = new GenericRecord(schema);

            record0.Add("station", "011990-99999");
            record0.Add("time", -619524000000);
            record0.Add("temp", 0);
            record1.Add("station", "011990-99999");
            record1.Add("time", -619506000000);
            record1.Add("temp", 22);
            record2.Add("station","011990-99999");
            record2.Add("time", -619484400000);
            record2.Add("temp", -11);
            record3.Add("station","012650-99999");
            record3.Add("time", -655531200000);
            record3.Add("temp", 111);
            record4.Add("station","012650-99999");
            record4.Add("time", -655509600000);
            record4.Add("temp", 78);

            var dataFilePath = GetExampleContentPath(Path.GetRandomFileName() + ".avro");

            try
            {
                // This writer serializes the records according to the schema.
                var datumWriter = new GenericDatumWriter<GenericRecord>(schema);

                // Create a DataFileWriter that uses the GenericDatumWriter to serialize
                // records to an Avro data file.
                using (var writer = DataFileWriter<GenericRecord>.OpenWriter(
                    datumWriter, dataFilePath))
                {
                    writer.Append(record0);
                    writer.Append(record1);
                    writer.Append(record2);
                    writer.Append(record3);
                    writer.Append(record4);
                }

                // TODO: This Avro data file *should* be identical to the weather.avro data file.
                // If they were identical, we would compare the two files here to confirm
                // everything worked as expected. However, due to a bug in the C# implementation,
                // the schema written to the Avro data file does not contain the documentation
                // for the record. This results in two files that are not identical.
            }
            finally
            {
                // Clean up our test file.
                if (System.IO.File.Exists(dataFilePath))
                {
                    System.IO.File.Delete(dataFilePath);
                }
            }
        }

        /// <summary>
        /// Read an Avro data file, `weather.avro`, using a <see cref="DataFileReader{T}"/>.
        /// Store the result in a <see cref="GenericRecord"/>. See `weather.json` for the
        /// JSON representation of this data file.
        /// </summary>
        [Test]
        public void Generic_DataFileReader()
        {
            var dataFilePath = GetExampleContentPath("weather.avro");
            var records = new List<GenericRecord>();

            // weather.avro is an Avro data file. To get a better idea of what it contains,
            // see weather.json, which is the JSON representation of that data file.
            // DataFileReader uses the schema embedded in the weather.avro data file to
            // parse the records in the file.
            using (var reader = DataFileReader<GenericRecord>.OpenReader(dataFilePath))
            {
                while (reader.HasNext())
                {
                    records.Add(reader.Next());
                }
            }

            // This file contains 5 records.
            Assert.AreEqual(5, records.Count);

            var firstRecord = records[0];

            // Use dictionary-like syntax to access fields in the records.
            Assert.AreEqual("011990-99999", firstRecord["station"]);
            Assert.AreEqual(-619524000000, firstRecord["time"]);
            Assert.AreEqual(0, firstRecord["temp"]);
        }

        private static string GetExampleContentPath(string fileName)
        {
            return Path.Combine(TestContext.CurrentContext.TestDirectory,
                "Examples", "Content", fileName);
        }
    }
}
