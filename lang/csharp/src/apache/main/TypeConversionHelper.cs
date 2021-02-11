using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Avro.Reflect;

namespace Avro
{
    /// <summary>
    /// Methods to help with type conversion
    /// </summary>
    internal static class TypeConversionHelper
    {
        private static ConcurrentDictionary<Type, Tuple<bool, Func<object[], object>>> _arrayToListConverters
            = new ConcurrentDictionary<Type, Tuple<bool, Func<object[], object>>>();
        
        public static T TryCast<T>(this object result)
        {
            try
            {
                return (T) result;
            }
            catch (InvalidCastException)
            {
                if (!TryConvertArrayToList(result, out T converted))
                {
                    throw;
                }

                return converted;
            }
        }

        public static bool TryConvertArrayToList<T>(object mightBeArray, out T list)
        {
            object[] arr = mightBeArray as object[];
            if (arr == null)
            {
                list = default(T);
                return false;
            }

            var tuple = _arrayToListConverters.GetOrAdd(typeof(T), GenerateArrayToListConverter);

            if (!tuple.Item1)
            {
                list = default(T);
                return false;
            }

            list = (T)tuple.Item2(arr);
            return true;
        }

        private static Tuple<bool, Func<object[], object>> GenerateArrayToListConverter(Type t1)
        {
            // look for the IList<T> interface to see what sort of type of instances are in the array
            var iList =
                t1.IsGenericType && t1.GetGenericTypeDefinition() == typeof(IList<>)
                    ? t1
                    : t1
                        .GetInterfaces()
                        .FirstOrDefault(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IList<>));

            if (iList == null)
            {
                return Tuple.Create<bool, Func<object[], object>>(false, null);
            }

            var itemType = iList.GetGenericArguments()[0];
            var method = typeof(TypeConversionHelper)
                .GetMethod(nameof(ConvertObjectArrayToList), BindingFlags.NonPublic | BindingFlags.Static)
                .MakeGenericMethod(itemType);

            return Tuple.Create<bool, Func<object[], object>>(true, a => method.Invoke(null, new[] { a }));
        }

        private static object ConvertObjectArrayToList<T>(object[] arr)
        {
            var list = new List<T>(arr.Length);
            list.AddRange(arr.Select(a => (T)a));
            return list;
        }
    }
}
