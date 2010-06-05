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


#include "CompilerNode.hh"
#include "NodeImpl.hh"

namespace avro {

NodePtr
nodeFromCompilerNode(CompilerNode &node)
{
    NodePtr ptr;

    switch(node.type()) {

      case AVRO_ARRAY:
        ptr.reset ( new NodeArray(node.itemsAttribute_));
        break;
    
      case AVRO_ENUM:
        ptr.reset ( new NodeEnum(node.nameAttribute_, node.symbolsAttribute_));
        break;

      case AVRO_FIXED:
        ptr.reset ( new NodeFixed(node.nameAttribute_, node.sizeAttribute_));
        break;
    
      case AVRO_MAP:
        ptr.reset ( new NodeMap(node.valuesAttribute_));
        break;

      case AVRO_RECORD:
        ptr.reset ( new NodeRecord(node.nameAttribute_, node.fieldsAttribute_, node.fieldsNamesAttribute_));
        break;
    
      case AVRO_UNION:
        ptr.reset ( new NodeUnion(node.typesAttribute_));
        break;
    
      case AVRO_SYMBOLIC:
        ptr.reset ( new NodeSymbolic(node.nameAttribute_));
        break;

      default:
        if(isPrimitive(node.type())) {
            ptr.reset ( new NodePrimitive(node.type()));        
        }
        else {
            throw Exception("Unknown type in nodeFromCompilerNode");
        }
        break;
    }

    return ptr;
}

} // namespace avro
