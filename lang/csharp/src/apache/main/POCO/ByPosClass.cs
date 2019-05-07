using System;
using System.Reflection;
using System.Collections.Concurrent;
using Avro;

namespace Avro.POCO
{
    public class ByPosClass : IDotnetClass
    {
        private ConcurrentDictionary<int, DotnetProperty> _propertyMap;

        private Type _type { get; set; }

        public Type GetClassType()
        {
            return _type;
        }
        public ByPosClass(Type t, RecordSchema r)
        {
            _type = t;
            foreach (var f in r.Fields)
            {
                _propertyMap.TryAdd(f.Pos, null);
            }
            foreach (var prop in _type.GetProperties())
            {
                foreach (var attr in prop.GetCustomAttributes(true))
                {
                    var avroAttr = attr as AvroFieldAttribute;
                    if (avroAttr != null)
                    {
                        if (avroAttr.FieldPos < 0)
                        {
                            throw new AvroException($"By field position mapping requires position in attribute. Type {_type.Name}, property {prop.Name}");
                        }
                        DotnetProperty p;
                        if (!_propertyMap.TryGetValue(avroAttr.FieldPos, out p))
                        {
                            throw new AvroException($"Avro record {r.Fullname} does not have field position {avroAttr.FieldPos}. Type {_type.Name}");
                        }
                        _propertyMap.TryAdd(avroAttr.FieldPos, new DotnetProperty(prop, avroAttr.Converter));
                    }
                }
            }
            foreach (var pos in _propertyMap.Keys)
            {
                DotnetProperty p;
                if (!_propertyMap.TryGetValue(pos, out p))
                {
                    throw new AvroException($"Avro record {r.Fullname} does not have mapping for field position {pos}. Type {_type.Name}");
                }
            }
        }
        public object GetValue(object o, Field f)
        {
            DotnetProperty p;
            if (!_propertyMap.TryGetValue(f.Pos, out p))
            {
                throw new AvroException($"ByPosClass doesnt contain property at position{f.Pos}");
            }
            return p.GetValue(o);
        }

        public void SetValue(object o, Field f, object v)
        {
            DotnetProperty p;
            if (!_propertyMap.TryGetValue(f.Pos, out p))
            {
                throw new AvroException($"ByPosClass doesnt contain property at position{f.Pos}");
            }
            p.SetValue(o, v);
        }

        public Type GetFieldType(Field f)
        {
            DotnetProperty p;
            if (!_propertyMap.TryGetValue(f.Pos, out p))
            {
                throw new AvroException($"ByPosClass doesnt contain property at position{f.Pos}");
            }
            return p.GetPropertyType();
        }

    }
}
