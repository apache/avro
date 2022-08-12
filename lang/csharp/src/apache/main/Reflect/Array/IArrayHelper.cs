using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Avro.Reflect.Array
{
    /// <summary>
    /// Interface to help serialize and deserialize arrays. Arrays need the following methods Count(), Add(), Clear().true
    /// This class allows these methods to be specified externally to the collection.
    /// </summary>
    public interface IArrayHelper
    {
        /// <summary>
        /// Type of the array to create when deserializing
        /// </summary>
        Type ArrayType { get; }

        /// <summary>
        /// Return the number of elements in the array.
        /// </summary>
        /// <value></value>
        int Count();

        /// <summary>
        /// Add an element to the array.
        /// </summary>
        /// <param name="o">Element to add to the array.</param>
        void Add(object o);

        /// <summary>
        /// Clear the array.
        /// </summary>
        /// <value></value>
        void Clear();
    }
}
