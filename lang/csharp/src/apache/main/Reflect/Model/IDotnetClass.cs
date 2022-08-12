using System;
using System.Collections.Generic;
using System.Text;

namespace Avro.Reflect.Model
{
    /// <summary>
    /// Interface that represent dotnet class
    /// </summary>
    public interface IDotnetClass
    {
        /// <summary>
        /// Return the type of the Class
        /// </summary>
        /// <returns>The </returns>
        Type GetClassType();

        /// <summary>
        /// Return the type of a property referenced by a field
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        Type GetPropertyType(Field field);

        /// <summary>
        /// Return the value of a property from an object referenced by a field
        /// </summary>
        /// <param name="o">the object</param>
        /// <param name="field">FieldSchema used to look up the property</param>
        /// <returns></returns>
        object GetValue(object o, Field field);

        /// <summary>
        /// Set the value of a property in a C# object
        /// </summary>
        /// <param name="o">the object</param>
        /// <param name="field">field schema</param>
        /// <param name="v">value for the property referenced by the field schema</param>
        void SetValue(object o, Field field, object v);
    }
}
