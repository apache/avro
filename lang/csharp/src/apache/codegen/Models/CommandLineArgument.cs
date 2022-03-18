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
    internal class CommandLineArgument
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CommandLineArgument"/> class.
        /// </summary>
        public CommandLineArgument()
        {
            Aliases = new List<string>();
        }

        /// <summary>
        /// Gets or sets the aliases, which are variations allowed instead of the parameter.
        /// </summary>
        /// <value>
        /// The aliases.
        /// </value>
        public List<string> Aliases { get; set; }

        /// <summary>
        /// Gets or sets the Friendly Name.
        /// </summary>
        /// <value>
        /// The Friendly Name.
        /// </value>
        public string FriendlyName { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this parameter is optional.
        /// </summary>
        /// <value>
        ///   <c>true</c> if this parameter is optional; otherwise, <c>false</c>.
        /// </value>
        public bool IsOptional { get; set; }

        /// <summary>
        /// Gets or sets the parameter.
        /// </summary>
        /// <value>
        /// The parameter.
        /// </value>
        public string Parameter { get; set; }

        /// <summary>
        /// Gets or sets the usage text.
        /// </summary>
        /// <value>
        /// The usage text.
        /// </value>
        public string Usage { get; set; }
    }
}
