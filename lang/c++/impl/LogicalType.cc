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

#include "LogicalType.hh"

namespace avro {

LogicalTypes logicalTypes;

// Wrap an indentation in a struct for ostream operator<<
struct indent {
    indent(int depth) :
        d(depth)
    { }
    int d;
};

void LogicalTypeImpl::getRequiredFields(std::vector<std::string>& fields) const
{
    for (FieldMap::const_iterator it = requiredFields_.begin();
            it != requiredFields_.end(); ++it)
    {
        fields.push_back(it->first);
    }
}

void LogicalTypeImpl::getOptionalFields(std::vector<std::string>& fields) const
{
    for (FieldMap::const_iterator it = optionalFields_.begin();
            it != optionalFields_.end(); ++it)
    {
        fields.push_back(it->first);
    }
}

const std::string& LogicalTypeImpl::getFieldValue(const std::string& field)
{
    for (FieldMap::iterator it = requiredFields_.begin();
            it != requiredFields_.end(); ++it)
    {
        if (field.compare(it->first) == 0)
        {
            return requiredFields_[field];
        }
    }
    for (FieldMap::iterator it = optionalFields_.begin();
            it != optionalFields_.end(); ++it)
    {
        if (field.compare(it->first) == 0)
        {
            return optionalFields_[field];
        }
    }
    throw Exception("No such field " + field);
}

void LogicalTypeImpl::setFieldValue(const std::string& field, const std::string& value)
{
    FieldMap::iterator it = requiredFields_.find(field);
    if (it != requiredFields_.end())
    {
        it->second = value;
    }
    else
    {
        it = optionalFields_.find(field);
        if (it != optionalFields_.end())
        {
            it->second = value;
        }
    }
}

void LogicalTypeImpl::validate(Type type) const
{
    bool compatible = false;
    for (std::vector<Type>::const_iterator it = compatibleTypes_.begin();
            it != compatibleTypes_.end(); ++it) {
        if (type == *it) {
            compatible = true;
        }
    }
    if (!compatible)
    {
        throw Exception("LogicalType " + name_ +
                    " doesn't support type " + toString(type));
    }
}

LogicalTypes::LogicalTypes()
{
    std::string name = "decimal";
    LogicalTypeFactoryPtr factory(new DecimalFactory());
    registerNewLogicalType(name, factory);
}

LogicalTypePtr LogicalTypes::createLogicalType(const std::string& name)
{

    for (FactoryMap::iterator it = registeredLogicalTypes_.begin();
            it != registeredLogicalTypes_.end(); ++it)
    {
        if (name.compare(it->first) == 0)
        {
            return LogicalTypePtr(it->second->create());
        }
    }
    throw Exception("No logicalType called " + name);
}

void LogicalTypes::registerNewLogicalType(std::string& name, LogicalTypeFactoryPtr& factory)
{
    registeredLogicalTypes_.insert(std::make_pair<std::string,
            LogicalTypeFactoryPtr>(name, factory));
}

} // namespace avro
