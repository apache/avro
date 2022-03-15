/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System.Collections.Generic;
using System.Linq;

namespace Avro
{
    internal static class Aliases
    {
        internal static IList<SchemaName> GetSchemaNames(IEnumerable<string> aliases, string enclosingTypeName, string enclosingTypeNamespace)
        {
            if (aliases == null)
                return null;

            var enclosingSchemaName = new SchemaName(enclosingTypeName, enclosingTypeNamespace, null, null);
            return aliases.Select(alias => new SchemaName(alias, enclosingSchemaName.Namespace, null, null)).ToList();
        }
    }
}
