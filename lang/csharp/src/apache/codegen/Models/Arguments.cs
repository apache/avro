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

namespace Avro
{
    public abstract class Arguments
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="Arguments"/> class.
        /// </summary>
        public Arguments()
        {
            NamespaceMapping = new Dictionary<string, string>();
        }

        /// <summary>
        /// Gets or sets the namespace mapping.
        /// </summary>
        /// <value>
        /// The namespace mapping.
        /// </value>
        public Dictionary<string, string> NamespaceMapping { get; set; }

        /// <summary>
        /// Gets or sets the output directory.
        /// </summary>
        /// <value>
        /// The output directory.
        /// </value>
        public string OutputDirectory { get; set; }

        /// <summary>
        /// Gets a value indicating whether this instance has namespace mapping.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance has namespace mapping; otherwise, <c>false</c>.
        /// </value>
        internal bool HasNamespaceMapping => NamespaceMapping.Count > 0;

        /// <summary>
        /// Gets a value indicating whether this instance has output directory.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance has output directory; otherwise, <c>false</c>.
        /// </value>
        internal bool HasOutputDirectory => !string.IsNullOrEmpty(OutputDirectory);
    }
}
