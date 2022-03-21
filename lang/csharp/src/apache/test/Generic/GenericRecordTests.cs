using System;
using System.Collections.Generic;
using Avro.Generic;
using NUnit.Framework;

namespace Avro.test.Generic
{
    [TestFixture]
    public class GenericRecordTests
    {
        private const string baseSchema = "{\"type\":\"record\",\"name\":\"r\",\"fields\":" +
            "[{\"name\":\"f2\",\"type\":\"int\"},{\"name\":\"f1\",\"type\":\"boolean\"}]}";

        [Test]
        public void TestAddByFieldNameThrows()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();

            // Field does not exist
            Assert.Throws<AvroException>(() => { genericRecord.Add("badField", "test"); });
        }

        [Test]
        public void TestAddByPosition()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();

            genericRecord.Add(0, 2);

            object value = genericRecord.GetValue(0);

            Assert.IsNotNull(value);
            Assert.IsTrue(value is int);
            Assert.AreEqual(2, (int)value);
        }

        [Test]
        public void TestAddByPositionThrows()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();

            Assert.Throws<IndexOutOfRangeException>(() => { genericRecord.Add(2, 2); });
        }

        [Test]
        public void TestEquals()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();
            GenericRecord genericRecord2 = GetBaseGenericRecord();

            Assert.IsTrue(genericRecord.Equals(genericRecord2));
        }

        [Test]
        public void TestEqualsNotEqual()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();
            GenericRecord genericRecord2 = GetBaseGenericRecord();
            genericRecord2.Add(0, 2);

            Assert.IsFalse(genericRecord.Equals(genericRecord2));
        }

        [Test]
        public void TestEqualsObject()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();
            object genericRecord2 = genericRecord;

            Assert.IsTrue(genericRecord.Equals(genericRecord2));
        }

        [Test]
        public void TestEqualsObjectNotEqual()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();
            GenericRecord genericRecord2 = GetBaseGenericRecord();
            genericRecord2.Add(0, 2);

            Assert.IsFalse(genericRecord.Equals((object)genericRecord2));
        }

        [Test]
        public void TestEqualsObjectNullObject()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();

            Assert.IsFalse(genericRecord.Equals((object)null));
        }

        [Test]
        public void TestGetHashCode()
        {
            int hashCode = GetBaseGenericRecord().GetHashCode();
            Assert.IsTrue(hashCode > 0);
        }

        [Test]
        public void TestGetValue()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();

            object value = genericRecord.GetValue(0);

            Assert.IsNotNull(value);
            Assert.IsTrue(value is int);
            Assert.AreEqual(1, (int)value);
        }

        [Test]
        public void TestKeyValueLookup()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();

            // Key Exists
            object existingKey = genericRecord["f2"];
            Assert.IsNotNull(existingKey);
            Assert.IsTrue(existingKey is int);
        }

        [Test]
        public void TestKeyValueLookupThrows()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();

            // Key does not exist
            Assert.Throws<KeyNotFoundException>(() => { object missingKey = genericRecord["badField"]; });
        }

        [Test]
        public void TestToString()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();
            string str = genericRecord.ToString();
            string expectedValue = "Schema: {\"type\":\"record\",\"name\":\"r\",\"fields\":" +
                            "[{\"name\":\"f2\",\"type\":\"int\"},{\"name\":\"f1\",\"type\":" +
                            "\"boolean\"}]}, contents: { f2: 1, f1: True, }";

            Assert.AreEqual(expectedValue, str);
        }

        [Test]
        public void TestTryGetValue()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();

            // Value exists
            bool returnResult = genericRecord.TryGetValue("f2", out object result);

            Assert.IsTrue(returnResult);
            Assert.IsNotNull(result);
            Assert.IsTrue(result is int);
            Assert.AreEqual(1, (int)result);
        }

        [Test]
        public void TestTryGetValueByPosition()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();

            bool returnResult = genericRecord.TryGetValue(0, out object value);

            Assert.IsTrue(returnResult);
            Assert.IsNotNull(value);
            Assert.IsTrue(value is int);
            Assert.AreEqual(1, (int)value);
        }

        [Test]
        public void TestTryGetValueByPositionNotFound()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();

            bool returnResult = genericRecord.TryGetValue(3, out object value);

            Assert.IsFalse(returnResult);
            Assert.IsNull(value);
        }

        [Test]
        public void TestTryGetValueNotFound()
        {
            GenericRecord genericRecord = GetBaseGenericRecord();

            // Value exists
            bool returnResult = genericRecord.TryGetValue("badField", out object result);

            Assert.IsFalse(returnResult);
            Assert.IsNull(result);
        }

        private GenericRecord GetBaseGenericRecord()
        {
            RecordSchema testSchema = Schema.Parse(baseSchema) as RecordSchema;
            GenericRecord genericRecord = new GenericRecord(testSchema);
            genericRecord.Add("f2", 1);
            genericRecord.Add("f1", true);

            return genericRecord;
        }
    }
}
