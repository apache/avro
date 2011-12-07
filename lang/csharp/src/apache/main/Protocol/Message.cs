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
using System.Text;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace Avro
{
    public class Message
    {
        /// <summary>
        /// Name of the message
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Documentation for the message
        /// </summary>
        public string Doc { get; set; }

        /// <summary>
        /// Anonymous record for the list of parameters for the request fields
        /// </summary>
        public RecordSchema Request { get; set; }

        /// <summary>
        /// Schema object for the 'response' attribute
        /// </summary>
        public Schema Response { get; set; }

        /// <summary>
        /// Union schema object for the 'error' attribute
        /// </summary>
        public UnionSchema Error { get; set; }

        /// <summary>
        /// Optional one-way attribute
        /// </summary>
        public bool? Oneway { get; set; }

        /// <summary>
        /// Constructor for Message class
        /// </summary>
        /// <param name="name">name property</param>
        /// <param name="doc">doc property</param>
        /// <param name="request">list of parameters</param>
        /// <param name="response">response property</param>
        /// <param name="error">error union schema</param>
        public Message(string name, string doc, RecordSchema request, Schema response, UnionSchema error, bool? oneway)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException("name", "name cannot be null.");
            this.Request = request;
            this.Response = response;
            this.Error = error;
            this.Name = name;
            this.Doc = doc;
            this.Oneway = oneway;
        }

        /// <summary>
        /// Parses the messages section of a protocol definition
        /// </summary>
        /// <param name="jmessage">messages JSON object</param>
        /// <param name="names">list of parsed names</param>
        /// <param name="encspace">enclosing namespace</param>
        /// <returns></returns>
        internal static Message Parse(JProperty jmessage, SchemaNames names, string encspace)
        {
            string name = jmessage.Name;
            string doc = JsonHelper.GetOptionalString(jmessage.Value, "doc");
            bool? oneway = JsonHelper.GetOptionalBoolean(jmessage.Value, "one-way");

            PropertyMap props = Schema.GetProperties(jmessage.Value);
            RecordSchema schema = RecordSchema.NewInstance(Schema.Type.Record, jmessage.Value as JObject, props, names, encspace);

            JToken jresponse = jmessage.Value["response"];
            var response = Schema.ParseJson(jresponse, names, encspace);

            JToken jerrors = jmessage.Value["errors"];
            UnionSchema uerrorSchema = null;
            if (null != jerrors)
            {
                Schema errorSchema = Schema.ParseJson(jerrors, names, encspace);
                if (!(errorSchema is UnionSchema))
                    throw new AvroException("");

                uerrorSchema = errorSchema as UnionSchema;
            }

            return new Message(name, doc, schema, response, uerrorSchema, oneway);
        }

        /// <summary>
        /// Writes the messages section of a protocol definition
        /// </summary>
        /// <param name="writer">writer</param>
        /// <param name="names">list of names written</param>
        /// <param name="encspace">enclosing namespace</param>
        internal void writeJson(Newtonsoft.Json.JsonTextWriter writer, SchemaNames names, string encspace)
        {
            writer.WriteStartObject();
            JsonHelper.writeIfNotNullOrEmpty(writer, "doc", this.Doc);

            if (null != this.Request)
                this.Request.WriteJsonFields(writer, names, null);

            if (null != this.Response)
            {
                writer.WritePropertyName("response");
                Response.WriteJson(writer, names, encspace);
            }

            if (null != this.Error)
            {
                writer.WritePropertyName("errors");
                this.Error.WriteJson(writer, names, encspace);
            }

            if (null != Oneway)
            {
                writer.WritePropertyName("one-way");
                writer.WriteValue(Oneway);
            }

            writer.WriteEndObject();
        }

        /// <summary>
        /// Tests equality of this Message object with the passed object
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(Object obj) 
        {
          if (obj == this) return true;
          if (!(obj is Message)) return false;

          Message that = obj as Message;
          return this.Name.Equals(that.Name) && 
                 this.Request.Equals(that.Request) &&
                 areEqual(this.Response, that.Response) && 
                 areEqual(this.Error, that.Error);
        }

        /// <summary>
        /// Returns the hash code of this Message object
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode() 
        {
            return Name.GetHashCode() +
                   Request.GetHashCode() +
                  (Response == null ? 0 : Response.GetHashCode()) +
                  (Error == null ? 0 : Error.GetHashCode());
        }

        /// <summary>
        /// Tests equality of two objects taking null values into account 
        /// </summary>
        /// <param name="o1"></param>
        /// <param name="o2"></param>
        /// <returns></returns>
        protected static bool areEqual(object o1, object o2)
        {
            return o1 == null ? o2 == null : o1.Equals(o2);
        }
    }
}
