using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Avro.Reflect.Array;

namespace Avro.Reflect.Service
{
    /// <summary>
    /// Additional functionality to serialize and deserialize arrays.
    /// Works with array helpert
    /// </summary>
    public class ArrayService : IArrayService
    {
        private readonly IReflectCache _reflectCache;

        /// <summary>
        /// Public constructor
        /// </summary>
        /// <param name="reflectCache"></param>
        public ArrayService(IReflectCache reflectCache)
        {
            _reflectCache = reflectCache;
        }

        /// <summary>
        /// Find an array helper for an array schema node.
        /// </summary>
        /// <param name="schema">Schema</param>
        /// <param name="enumerable">The array object. If it is null then Add(), Count() and Clear methods will throw exceptions.</param>
        /// <returns></returns>
        public IArrayHelper GetArrayHelper(ArraySchema schema, IEnumerable enumerable)
        {
            string s = GetHelperName(schema);

            if (s != null && _reflectCache.TryGetArrayHelperType(s, out Type arrayHelperType))
            {
                return (IArrayHelper)Activator.CreateInstance(arrayHelperType, enumerable);
            }

            return (IArrayHelper)Activator.CreateInstance(typeof(ArrayHelper), enumerable);
        }

        internal string GetHelperName(ArraySchema ars)
        {
            // ArraySchema is unnamed schema and doesn't have a FulllName, use "helper" metadata.
            // Metadata is json string, strip quotes

            string s = null;
            s = ars.GetProperty("helper");
            if (s != null && s.Length > 2)
            {
                s = s.Substring(1, s.Length - 2);
            }
            else
            {
                s = null;
            }

            return s;
        }
    }
}
