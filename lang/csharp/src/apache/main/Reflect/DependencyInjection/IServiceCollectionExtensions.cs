using System;
using System.Collections.Generic;
using System.Text;
using Avro.Reflect.Interface;
using Avro.Reflect.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace Avro.Reflect.DependencyInjection
{
    /// <summary>
    /// IServiceCollection extensions
    /// </summary>
    public static class IServiceCollectionExtensions
    {
        /// <summary>
        /// Register Apache.Avro.Reflect
        /// </summary>
        /// <param name="serviceCollection"></param>
        public static void AddAvroReflect(this IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IReflectCache, ReflectCache>();
        }
    }
}
