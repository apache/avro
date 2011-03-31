/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Avro
{
    public class Protocol
    {
        /// <summary>
        /// Name of the protocol
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Namespace of the protocol
        /// </summary>
        public string Namespace { get; set; }

        /// <summary>
        /// Documentation for the protocol
        /// </summary>
        public string Doc { get; set; }

        /// <summary>
        /// List of schemas objects representing the different schemas defined under the 'types' attribute
        /// </summary>
        public IList<Schema> Types { get; set; }

        /// <summary>
        /// List of message objects representing the different schemas defined under the 'messages' attribute
        /// </summary>
        public IDictionary<string,Message> Messages { get; set; }

        /// <summary>
        /// Constructor for Protocol class
        /// </summary>
        /// <param name="name">required name of protocol</param>
        /// <param name="space">optional namespace</param>
        /// <param name="doc">optional documentation</param>
        /// <param name="types">required list of types</param>
        /// <param name="messages">required list of messages</param>
        public Protocol(string name, string space,
                        string doc, IEnumerable<Schema> types,
                        IDictionary<string,Message> messages)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException("name", "name cannot be null.");
            if (null == types) throw new ArgumentNullException("types", "types cannot be null.");
            if (null == messages) throw new ArgumentNullException("messages", "messages cannot be null.");

            this.Name = name;
            this.Namespace = space;
            this.Doc = doc;
            this.Types = new List<Schema>(types);
            this.Messages = new Dictionary<string, Message>(messages);
        }

        /// <summary>
        /// Parses the given JSON string to create a Protocol object
        /// </summary>
        /// <param name="jstring">JSON string</param>
        /// <returns>Protocol object</returns>
        public static Protocol Parse(string jstring)
        {
            if (string.IsNullOrEmpty(jstring)) throw new ArgumentNullException("json", "json cannot be null.");

            JToken jtok = null;
            try
            {
                jtok = JObject.Parse(jstring);
            }
            catch (Exception ex)
            {
                throw new ProtocolParseException("Invalid JSON format: " + jstring, ex);
            }
            return Parse(jtok);
        }

        /// <summary>
        /// Parses the given JSON object to create a Protocol object
        /// </summary>
        /// <param name="jtok">JSON object</param>
        /// <returns>Protocol object</returns>
        private static Protocol Parse(JToken jtok)
        {
            string name = JsonHelper.GetRequiredString(jtok, "protocol");
            string space = JsonHelper.GetOptionalString(jtok, "namespace");
            string doc = JsonHelper.GetOptionalString(jtok, "doc");

            var names = new SchemaNames();

            JToken jtypes = jtok["types"];
            var types = new List<Schema>();
            if (jtypes is JArray)
            {
                foreach (JToken jtype in jtypes)
                {
                    var schema = Schema.ParseJson(jtype, names, space);
                    types.Add(schema);
                }
            }

            var messages = new Dictionary<string,Message>();
            JToken jmessages = jtok["messages"];
            if (null != jmessages)
            {
                foreach (JProperty jmessage in jmessages)
                {
                    var message = Message.Parse(jmessage, names, space);
                    messages.Add(message.Name, message);
                }
            }

            return new Protocol(name, space, doc, types, messages);
        }

        /// <summary>
        /// Writes Protocol in JSON format
        /// </summary>
        /// <returns>JSON string</returns>
        public override string ToString()
        {
            using (System.IO.StringWriter sw = new System.IO.StringWriter())
            {
                using (Newtonsoft.Json.JsonTextWriter writer = new Newtonsoft.Json.JsonTextWriter(sw))
                {
                    #if(DEBUG)
                    writer.Formatting = Newtonsoft.Json.Formatting.Indented;
                    #endif

                    WriteJson(writer, new SchemaNames());
                    writer.Flush();
                    return sw.ToString();
                }
            }
        }

        /// <summary>
        /// Writes Protocol in JSON format
        /// </summary>
        /// <param name="writer">JSON writer</param>
        /// <param name="names">list of named schemas already written</param>
        internal void WriteJson(Newtonsoft.Json.JsonTextWriter writer, SchemaNames names)
        {
            writer.WriteStartObject();

            JsonHelper.writeIfNotNullOrEmpty(writer, "protocol", this.Name);
            JsonHelper.writeIfNotNullOrEmpty(writer, "namespace", this.Namespace);
            JsonHelper.writeIfNotNullOrEmpty(writer, "doc", this.Doc);

            writer.WritePropertyName("types");
            writer.WriteStartArray();

            foreach (Schema type in this.Types)
                type.WriteJson(writer, names, this.Namespace);

            writer.WriteEndArray();

            writer.WritePropertyName("messages");
            writer.WriteStartObject();

            foreach (KeyValuePair<string,Message> message in this.Messages)
            {
                writer.WritePropertyName(message.Key);
                message.Value.writeJson(writer, names, this.Namespace);
            }

            writer.WriteEndObject();
            writer.WriteEndObject();
        }
    }
}
