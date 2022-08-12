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
    public interface IArrayService
    {
        /// <summary>
        /// Find an array helper for an array schema node.
        /// </summary>
        /// <param name="schema">Schema</param>
        /// <param name="enumerable">The array object. If it is null then Add(), Count() and Clear methods will throw exceptions.</param>
        /// <returns></returns>
        IArrayHelper GetArrayHelper(ArraySchema schema, IEnumerable enumerable);
    }
}
