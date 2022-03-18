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

namespace Avro
{
    public class SchemaArguments : Arguments
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaArguments"/> class.
        /// </summary>
        public SchemaArguments() : base()
        {
            CreateNamespaceDirectories = true;
        }

        /// <summary>
        /// Gets or sets a value indicating whether [create namespace directories].
        /// </summary>
        /// <value>
        ///   <c>true</c> if [create namespace directories]; otherwise, <c>false</c>.
        /// </value>
        public bool CreateNamespaceDirectories { get; set; }

        /// <summary>
        /// Gets or sets the schema file.
        /// </summary>
        /// <value>
        /// The schema file.
        /// </value>
        public string SchemaFile { get; set; }

        /// <summary>
        /// Gets a value indicating whether this instance has schema file.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance has schema file; otherwise, <c>false</c>.
        /// </value>
        internal bool HasSchemaFile => !string.IsNullOrEmpty(SchemaFile);
    }
}
