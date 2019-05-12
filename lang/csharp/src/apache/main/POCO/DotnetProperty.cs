using System;
using System.Reflection;

namespace Avro.POCO
{
    public class DotnetProperty
    {
        private PropertyInfo _property;
        public IAvroFieldConverter Converter;

        public DotnetProperty(PropertyInfo property, IAvroFieldConverter converter)
        {
            _property = property;
            Converter = converter;
        }

        public DotnetProperty(PropertyInfo property) : this(property, null)
        {
        }

        virtual public Type GetPropertyType()
        {
            if (Converter != null)
            {
                return Converter.GetPropertyType();
            }

            return _property.PropertyType;
        }

        virtual public object GetValue(object o)
        {
            if (Converter != null)
            {
                return Converter.ToAvroType(_property.GetValue(o));
            }

            return _property.GetValue(o);
        }

        virtual public void SetValue(object o, object v)
        {
            if (Converter != null)
            {
                _property.SetValue(o, Converter.FromAvroType(v));
            }
            else
            {
                _property.SetValue(o, v);
            }
        }
    }
}
