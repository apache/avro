using System;
using System.Collections.Generic;
using System.Text;

namespace Avro.Reflect.Model
{
    /// <summary>
    /// Interface that represent Dotnet property
    /// </summary>
    public interface IDotnetProperty
    {
        /// <summary>
        /// Get .Net property type
        /// </summary>
        /// <returns></returns>
        Type GetPropertyType();

        /// <summary>
        /// Get value
        /// </summary>
        /// <param name="o"></param>
        /// <param name="s"></param>
        /// <returns></returns>
        object GetValue(object o, Schema s);

        /// <summary>
        /// Set value
        /// </summary>
        /// <param name="o"></param>
        /// <param name="v"></param>
        /// <param name="s"></param>
        void SetValue(object o, object v, Schema s);
    }
}
