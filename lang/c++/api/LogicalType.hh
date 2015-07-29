/*
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

#ifndef avro_LogicalType_hh__
#define avro_LogicalType_hh__

#include "Exception.hh"
#include "Types.hh"
#include <vector>
#include <map>
#include <boost/lexical_cast.hpp>

namespace avro {

///
/// logical type interface
///
class LogicalType
{
public:
    /// return the logical type name
    virtual const std::string& getName() const = 0;

    /// get the required fields, e.g. "precision" of "decimal" logical type
    virtual void getRequiredFields(std::vector<std::string>& fields) const = 0;

    /// get the optional fields, e.g. "scale" of "decimal" logical type
    virtual void getOptionalFields(std::vector<std::string>& fields) const = 0;

    /// get value for a required or optional field
    virtual const std::string& getFieldValue(const std::string& field) = 0;

    /// set value for a required or optional field
    virtual void setFieldValue(const std::string& field, const std::string& value) = 0;

    /// validate if a logical type is compatible with given type
    virtual void validate(Type type) const = 0;
};
typedef boost::shared_ptr<LogicalType> LogicalTypePtr;

///
/// logical type factory interface,
/// user could implement this interface for custom logical type
///
class LogicalTypeFactory
{
public:
    /// create a new logical type
    virtual LogicalTypePtr create() = 0;
};
typedef boost::shared_ptr<LogicalTypeFactory> LogicalTypeFactoryPtr;

///
/// base class of logical type, which implements logical type interfaces
/// all logical types should derived from this class
///
class LogicalTypeImpl : public LogicalType
{
public:
    LogicalTypeImpl() { }
    LogicalTypeImpl(std::string name) :
        name_(name) { }
    virtual ~LogicalTypeImpl() { }

    virtual const std::string& getName() const
    {
        return name_;
    }
    virtual void getRequiredFields(std::vector<std::string>& fields) const;
    virtual void getOptionalFields(std::vector<std::string>& fields) const;

    virtual const std::string& getFieldValue(const std::string& field);
    virtual void setFieldValue(const std::string& field, const std::string& value);

    virtual void validate(Type type) const;

    typedef std::map<std::string /*fieldName*/, std::string /*fieldVavlue*/> FieldMap;

protected:
    std::string name_;
    std::vector<Type> compatibleTypes_;
    FieldMap requiredFields_;
    FieldMap optionalFields_;
};

class Decimal : public LogicalTypeImpl
{
public:
    Decimal() : LogicalTypeImpl("decimal")
    {
        LogicalTypeImpl::compatibleTypes_.push_back(AVRO_BYTES);
        LogicalTypeImpl::compatibleTypes_.push_back(AVRO_FIXED);
        // initialize the required and optional fields as empty strings
        LogicalTypeImpl::requiredFields_.insert(
                std::pair<std::string, std::string>("precision", ""));
        LogicalTypeImpl::optionalFields_.insert(
                std::pair<std::string, std::string>("scale", ""));
    }

    virtual ~Decimal() { }

    void validate(Type type) const
    {
        LogicalTypeImpl::validate(type);
        int precision = boost::lexical_cast<int>(requiredFields_.find("precision")->second);
        if (precision < 0)
        {
            throw Exception("precision can not be negative");
        }

        if (!(optionalFields_.find("scale")->second).empty())
        {
            int scale = boost::lexical_cast<int>(optionalFields_.find("scale")->second);
            if (scale < 0)
            {
                throw Exception("scale can not be negative");
            }
        }
    }
};

class DecimalFactory : public LogicalTypeFactory
{
public:
    DecimalFactory() { }
    virtual ~DecimalFactory() { }

    virtual LogicalTypePtr create()
    {
        return LogicalTypePtr(new Decimal());
    }
};

class LogicalTypes : boost::noncopyable
{
public:
    LogicalTypes();
    virtual ~LogicalTypes() {}

    typedef std::map<std::string /*LogicalTypeName*/, LogicalTypeFactoryPtr> FactoryMap;

    LogicalTypePtr createLogicalType(const std::string& name);
    void registerNewLogicalType(std::string& name, LogicalTypeFactoryPtr& factory);

private:
    FactoryMap registeredLogicalTypes_;
};

extern LogicalTypes logicalTypes;


} // namespace avro

#endif //avro_LogicalType_hh__
