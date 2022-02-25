using NUnit.Framework;

namespace Avro.test
{
    [TestFixture]
    public class ArraySchemaTests
    {
        [Test]
        public void EqualsNullCheck()
        {
            string schemaString = "{\"type\": \"array\", \"items\": \"long\"}";
            ArraySchema nullSchema = null;

            Schema schema = Schema.Parse(schemaString);

            if (schema is ArraySchema arraySchema)
            {
                Assert.False(arraySchema.Equals(nullSchema));
            }
            else
            {
                Assert.Fail("Schema was not an Array Schema");
            }
        }

        [Test]
        public void EqualsNotArraySchema()
        {
            string schemaString = "[\"string\", \"null\", \"long\"]";
            string arraySchemaString = "{\"type\": \"array\", \"items\": \"long\"}";
            ArraySchema arraySchema = Schema.Parse(arraySchemaString) as ArraySchema;
            Schema schema = Schema.Parse(schemaString);

            Assert.False(arraySchema.Equals(schema));
        }
    }
}
