using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Avro.Test
{
    [TestFixture]
    public class AliasesTests
    {
        [TestCase]
        public void TestNoNamespace()
        {
            CollectionAssert.AreEqual(new[] { new SchemaName("alias", null, null, null) }, Aliases.GetSchemaNames(new[] { "alias" }, "name", null));
        }

        [TestCase]
        public void TestTypeWithNamespace()
        {
            CollectionAssert.AreEqual(new[] { new SchemaName("space.alias", null, null, null) }, Aliases.GetSchemaNames(new[] { "alias" }, "name", "space"));
        }

        [TestCase]
        public void TestTypeWithNamespaceInName()
        {
            CollectionAssert.AreEqual(new[] { new SchemaName("space.alias", null, null, null) }, Aliases.GetSchemaNames(new[] { "alias" }, "space.name", null));
        }

        [TestCase]
        public void TestAliasWithNamespace()
        {
            CollectionAssert.AreEqual(new[] { new SchemaName("name.alias", null, null, null) }, Aliases.GetSchemaNames(new[] { "name.alias" }, "space.name", null));
        }
    }
}
