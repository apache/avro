using System;

namespace Avro.POCO
{
    public interface IAvroFieldConverter
    {
        object ToAvroType(object o);

        object FromAvroType(object o);

        Type GetPropertyType();
    }
}
