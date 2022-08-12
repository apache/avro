using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Avro.Reflect.Array;
using Avro.Reflect.Model;

namespace Avro.Reflect.Service
{
    /// <summary>
    /// Caches
    /// </summary>
    public interface IReflectCache
    {
        /// <summary>
        /// Find a class that matches the schema full name.
        /// </summary>
        /// <param name="schemaFullName"></param>
        /// <returns></returns>
        DotnetClass GetClass(string schemaFullName);

        /// <summary>
        /// Add a class that for schema full name.
        /// </summary>
        /// <param name="schemaFullName"></param>
        /// <param name="dotnetClass"></param>
        void AddClass(string schemaFullName, DotnetClass dotnetClass);

        /// <summary>
        /// Find a enum type that matches the schema full name.
        /// </summary>
        /// <param name="schemaFullName"></param>
        /// <returns></returns>
        Type GetEnum(string schemaFullName);

        /// <summary>
        /// Add a class that for schema full name.
        /// </summary>
        /// <param name="schemaFullName"></param>
        /// <param name="enumType"></param>
        void AddEnum(string schemaFullName, Type enumType);

        /// <summary>
        /// Add an array helper type. Array helpers are used for collections that are not generic lists.
        /// </summary>
        /// <param name="arrayHelperName">Name of the helper. Corresponds to metadata "helper" field in the schema.</param>
        void AddArrayHelperType<T>(string arrayHelperName) where T : IArrayHelper;

        /// <summary>
        /// Find an array helper type for an array schema node.
        /// </summary>
        /// <param name="arrayHelperName">Schema</param>
        /// <param name="arrayHelperType">Schema</param>
        /// <returns></returns>
        bool TryGetArrayHelperType(string arrayHelperName, out Type arrayHelperType);
    }
}
