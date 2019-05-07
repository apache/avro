using System;

namespace Avro.POCO
{
    public class DateTimeOffsetConverter : IAvroFieldConverter
    {
        public object ToAvroType(object o)
        {
            var dt = (DateTimeOffset)o;
            return dt.ToUnixTimeMilliseconds();
        }

        public object FromAvroType(object o)
        {
            var dt = CompatibilityExtensions.FromUnixTimeMilliseconds((long)o);
            return dt;
        }

        public Type GetPropertyType()
        {
            return typeof(long);
        }
    }
}
