using System;
using Avro;

namespace Avro.POCO
{
    public interface IDotnetClass
    {
        Type GetClassType();
        Type GetFieldType( Field f );
        object GetValue(object o, Field f);
        void SetValue(object o, Field f, object v);
    }
}
