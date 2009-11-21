
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

#include "Instruction.hh"
#include "NodeImpl.hh"
#include "ValidSchema.hh"
#include "ValidatingReader.hh"
#include "Boost.hh"

namespace avro {

class DynamicBuilder;
typedef boost::shared_ptr<Instruction> InstructionPtr;
typedef boost::ptr_vector<Instruction> InstructionPtrVector;

// #define DEBUG_VERBOSE

#ifdef DEBUG_VERBOSE
#define DEBUG_OUT(str) std::cout << str << '\n'
#else
class NoOp {};
template<typename T> NoOp& operator<<(NoOp &noOp, const T&) {
    return noOp;
}
NoOp noop;
#define DEBUG_OUT(str) noop << str 
#endif

template<typename T>
class PrimitiveSkipper : public Instruction
{
  public:

    PrimitiveSkipper() : 
        Instruction()
    {}

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        T val;
        reader.readValue(val);
        DEBUG_OUT("Skipping " << val);
    }
};

template<typename T>
class PrimitiveParser : public Instruction
{
  public:

    PrimitiveParser(const Offset &offset) : 
        Instruction(),
        offset_(offset.offset())
    {}

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        T* location = reinterpret_cast<T *> (address + offset_);
        reader.readValue(*location);
        DEBUG_OUT("Reading " << *location);
    }

  private:

    size_t offset_;
};

template<typename WT, typename RT>
class PrimitivePromoter : public Instruction
{
  public:

    PrimitivePromoter(const Offset &offset) : 
        Instruction(),
        offset_(offset.offset())
    {}

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        parseIt<WT>(reader, address);
    }

  private:

    void parseIt(ValidatingReader &reader, uint8_t *address, const boost::true_type &) const
    {
        WT val;
        reader.readValue(val);
        RT *location = reinterpret_cast<RT *> (address + offset_);
        *location = static_cast<RT>(val);
        DEBUG_OUT("Promoting " << val);
    }

    void parseIt(ValidatingReader &reader, uint8_t *address, const boost::false_type &) const
    { }

    template<typename T>
    void parseIt(ValidatingReader &reader, uint8_t *address) const
    {
        parseIt(reader, address, is_promotable<T>());
    }

    size_t offset_;
};

template <>
class PrimitiveSkipper<std::vector<uint8_t> > : public Instruction
{
  public:

    PrimitiveSkipper() : 
        Instruction()
    {}

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        std::vector<uint8_t> val;
        reader.readBytes(val);
        DEBUG_OUT("Skipping bytes");
    }
};

template <>
class PrimitiveParser<std::vector<uint8_t> > : public Instruction
{
  public:

    PrimitiveParser(const Offset &offset) : 
        Instruction(),
        offset_(offset.offset()) 
    {}

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        std::vector<uint8_t> *location = reinterpret_cast<std::vector<uint8_t> *> (address + offset_);
        reader.readBytes(*location);
        DEBUG_OUT("Reading bytes");
    }

  private:

    size_t offset_;
};

class RecordSkipper : public Instruction
{
  public:

    RecordSkipper(DynamicBuilder &builder, const NodePtr &writer);

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        DEBUG_OUT("Skipping record");

        reader.readRecord();
        size_t steps = instructions_.size();
        for(size_t i = 0; i < steps; ++i) {
            instructions_[i].parse(reader, address);
        }
    }

  protected:
    
    InstructionPtrVector instructions_;

};

class RecordParser : public Instruction
{
  public:

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        DEBUG_OUT("Reading record");

        reader.readRecord();
        size_t steps = instructions_.size();
        for(size_t i = 0; i < steps; ++i) {
            instructions_[i].parse(reader, address);
        }
    }

    RecordParser(DynamicBuilder &builder, const NodePtr &writer, const NodePtr &reader, const CompoundOffset &offsets);

  protected:
    
    InstructionPtrVector instructions_;

};


class MapSkipper : public Instruction
{
  public:

    MapSkipper(DynamicBuilder &builder, const NodePtr &writer);

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        DEBUG_OUT("Skipping map");

        std::string key;
        int64_t size = 0;
        do {
            size = reader.readMapBlockSize();
            for(int64_t i = 0; i < size; ++i) {
                reader.readValue(key);
                instruction_->parse(reader, address);
            }
        } while (size != 0);
    }

  protected:

    InstructionPtr instruction_;
};


class MapParser : public Instruction
{
  public:

    typedef uint8_t *(*GenericMapSetter)(uint8_t *map, const std::string &key);

    MapParser(DynamicBuilder &builder, const NodePtr &writer, const NodePtr &reader, const CompoundOffset &offsets);

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        DEBUG_OUT("Reading map");

        uint8_t *mapAddress = address + offset_;

        std::string key;
        GenericMapSetter* setter = reinterpret_cast<GenericMapSetter *> (address + setFuncOffset_);

        int64_t size = 0;
        do {
            size = reader.readMapBlockSize();
            for(int64_t i = 0; i < size; ++i) {
                reader.readValue(key);

                // create a new map entry and get the address
                uint8_t *location = (*setter)(mapAddress, key);
                instruction_->parse(reader, location);
            }
        } while (size != 0);
    }

  protected:
    
    InstructionPtr  instruction_;
    size_t          offset_;
    size_t          setFuncOffset_;
};

class ArraySkipper : public Instruction
{
  public:

    ArraySkipper(DynamicBuilder &builder, const NodePtr &writer);

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        DEBUG_OUT("Skipping array");

        int64_t size = 0;
        do {
            size = reader.readArrayBlockSize();
            for(int64_t i = 0; i < size; ++i) {
                instruction_->parse(reader, address);
            }
        } while (size != 0);
    }

  protected:
   
    InstructionPtr instruction_;
};

typedef uint8_t *(*GenericArraySetter)(uint8_t *array);

class ArrayParser : public Instruction
{
  public:

    ArrayParser(DynamicBuilder &builder, const NodePtr &writer, const NodePtr &reader, const CompoundOffset &offsets);

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        DEBUG_OUT("Reading array");

        uint8_t *arrayAddress = address + offset_;

        GenericArraySetter* setter = reinterpret_cast<GenericArraySetter *> (address + setFuncOffset_);

        int64_t size = 0;
        do {
            size = reader.readArrayBlockSize();
            for(int64_t i = 0; i < size; ++i) {
                // create a new map entry and get the address
                uint8_t *location = (*setter)(arrayAddress);
                instruction_->parse(reader, location);
            }
        } while (size != 0);
    }

  protected:
    
    ArrayParser() :
        Instruction()
    {}
    
    InstructionPtr instruction_;
    size_t         offset_;
    size_t         setFuncOffset_;
};

class EnumSkipper : public Instruction
{
  public:

    EnumSkipper(DynamicBuilder &builder, const NodePtr &writer) :
        Instruction()
    { }

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        int64_t val = reader.readEnum();
        DEBUG_OUT("Skipping enum" << val);
    }
};

class EnumParser : public Instruction
{
  public:

    enum EnumRepresentation {
        VAL
    };

    EnumParser(DynamicBuilder &builder, const NodePtr &writer, const NodePtr &reader, const CompoundOffset &offsets) :
        Instruction(),
        offset_(offsets.at(0).offset()),
        readerSize_(reader->names())
    { 
        const size_t writerSize = writer->names();

        mapping_.reserve(writerSize);

        for(size_t i = 0; i < writerSize; ++i) {
            const std::string &name = writer->nameAt(i);
            size_t readerIndex = readerSize_;
            reader->nameIndex(name, readerIndex);
            mapping_.push_back(readerIndex);
        }
    }

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        int64_t val = reader.readEnum();
        assert(static_cast<size_t>(val) < mapping_.size());

        if(mapping_[val] < readerSize_) {
            EnumRepresentation* location = reinterpret_cast<EnumRepresentation *> (address + offset_);
            *location = static_cast<EnumRepresentation>(mapping_[val]);
            DEBUG_OUT("Setting enum" << *location);
        }
    }

protected:

    size_t offset_;
    size_t readerSize_;
    std::vector<size_t> mapping_;
    
};

class UnionSkipper : public Instruction
{
  public:

    UnionSkipper(DynamicBuilder &builder, const NodePtr &writer);

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        DEBUG_OUT("Skipping union");
        int64_t choice = reader.readUnion();
        instructions_[choice].parse(reader, address);
    }

  protected:
    
    InstructionPtrVector instructions_;
};


class UnionParser : public Instruction
{
  public:

    typedef uint8_t *(*GenericUnionSetter)(uint8_t *, int64_t);

    UnionParser(DynamicBuilder &builder, const NodePtr &writer, const NodePtr &reader, const CompoundOffset &offsets);

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        DEBUG_OUT("Reading union");
        int64_t writerChoice = reader.readUnion();
        int64_t *readerChoice = reinterpret_cast<int64_t *>(address + choiceOffset_);

        *readerChoice = choiceMapping_[writerChoice];
        GenericUnionSetter* setter = reinterpret_cast<GenericUnionSetter *> (address + setFuncOffset_);
        uint8_t *value = reinterpret_cast<uint8_t *> (address + offset_);
        uint8_t *location = (*setter)(value, *readerChoice);

        instructions_[writerChoice].parse(reader, location);
    }

  protected:
    
    InstructionPtrVector instructions_;
    std::vector<int64_t> choiceMapping_;
    size_t offset_;
    size_t choiceOffset_;
    size_t setFuncOffset_;
};

class UnionToNonUnionParser : public Instruction
{
  public:

    typedef uint8_t *(*GenericUnionSetter)(uint8_t *, int64_t);

    UnionToNonUnionParser(DynamicBuilder &builder, const NodePtr &writer, const NodePtr &reader, const Offset &offsets);

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        DEBUG_OUT("Reading union to non-union");
        int64_t choice = reader.readUnion();
        instructions_[choice].parse(reader, address);
    }

  protected:
    
    InstructionPtrVector instructions_;
};

class NonUnionToUnionParser : public Instruction
{
  public:

    typedef uint8_t *(*GenericUnionSetter)(uint8_t *, int64_t);

    NonUnionToUnionParser(DynamicBuilder &builder, const NodePtr &writer, const NodePtr &reader, const CompoundOffset &offsets);

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        DEBUG_OUT("Reading non-union to union");

        int64_t *choice = reinterpret_cast<int64_t *>(address + choiceOffset_);
        *choice = choice_;
        GenericUnionSetter* setter = reinterpret_cast<GenericUnionSetter *> (address + setFuncOffset_);
        uint8_t *value = reinterpret_cast<uint8_t *> (address + offset_);
        uint8_t *location = (*setter)(value, choice_);

        instruction_->parse(reader, location);
    }

  protected:
    
    InstructionPtr instruction_;
    size_t choice_;
    size_t offset_;
    size_t choiceOffset_;
    size_t setFuncOffset_;
};

class FixedSkipper : public Instruction
{
  public:

    FixedSkipper(DynamicBuilder &builder, const NodePtr &writer) :
        Instruction() 
    {
        size_ = writer->fixedSize();
    }

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        DEBUG_OUT("Skipping fixed");
        uint8_t val[size_];
        reader.readFixed(val, size_);
    }

  protected:

    int size_;
    
};

class FixedParser : public Instruction
{
  public:

    FixedParser(DynamicBuilder &builder, const NodePtr &writer, const NodePtr &reader, const CompoundOffset &offsets) :
        Instruction() 
    {
        size_ = writer->fixedSize();
        offset_ = offsets.at(0).offset();
    }

    virtual void parse(ValidatingReader &reader, uint8_t *address) const
    {
        DEBUG_OUT("Reading fixed");
        uint8_t *location = reinterpret_cast<uint8_t *> (address + offset_);
        reader.readFixed(location, size_);
    }

  protected:

    int size_;
    size_t offset_;
    
};


class DynamicBuilder : public boost::noncopyable {

    template<typename T>
    Instruction*
    buildPrimitiveSkipper(const NodePtr &writer) 
    {
        return new PrimitiveSkipper<T>();
    }

    template<typename T>
    Instruction*
    buildPrimitive(const NodePtr &writer, const NodePtr &reader, const Offset &offset)
    {
        Instruction *instruction;

        SchemaResolution match = writer->resolve(*reader);

        if (match == RESOLVE_NO_MATCH) {
            instruction = new PrimitiveSkipper<T>();
        } 
        else if (reader->type() == AVRO_UNION) {
            const CompoundOffset &compoundOffset = static_cast<const CompoundOffset &>(offset);
            instruction = new NonUnionToUnionParser(*this, writer, reader, compoundOffset);
        }
        else if (match == RESOLVE_MATCH) {
            instruction = new PrimitiveParser<T>(offset);
        }
        else if(match == RESOLVE_PROMOTABLE_TO_LONG) {
            instruction = new PrimitivePromoter<T, int64_t>(offset);
        }
        else if(match == RESOLVE_PROMOTABLE_TO_FLOAT) {
            instruction = new PrimitivePromoter<T, float>(offset);
        }
        else if(match == RESOLVE_PROMOTABLE_TO_DOUBLE) {
            instruction = new PrimitivePromoter<T, double>(offset);
        }
        else {
            assert(0);
        }
        return instruction;
    }

    template<typename Skipper>
    Instruction*
    buildCompoundSkipper(const NodePtr &writer) 
    {
        return new Skipper(*this, writer);
    }


    template<typename Parser, typename Skipper>
    Instruction*
    buildCompound(const NodePtr &writer, const NodePtr &reader, const Offset &offset)
    {
        Instruction *instruction;

        SchemaResolution match = RESOLVE_NO_MATCH;

        match = writer->resolve(*reader);

        if (match == RESOLVE_NO_MATCH) {
            instruction = new Skipper(*this, writer);
        }
        else if(writer->type() != AVRO_UNION && reader->type() == AVRO_UNION) {
            const CompoundOffset &compoundOffset = dynamic_cast<const CompoundOffset &>(offset);
            instruction = new NonUnionToUnionParser(*this, writer, reader, compoundOffset);
        }
        else if(writer->type() == AVRO_UNION && reader->type() != AVRO_UNION) {
            instruction = new UnionToNonUnionParser(*this, writer, reader, offset);
        }
        else {
            const CompoundOffset &compoundOffset = dynamic_cast<const CompoundOffset &>(offset);
            instruction = new Parser(*this, writer, reader, compoundOffset);
        } 

        return instruction;
    }

  public:

    Instruction *
    build(const NodePtr &writer, const NodePtr &reader, const Offset &offset)
    {

        typedef Instruction* (DynamicBuilder::*BuilderFunc)(const NodePtr &writer, const NodePtr &reader, const Offset &offset);

        NodePtr currentWriter = (writer->type() == AVRO_SYMBOLIC) ?
            resolveSymbol(writer) : writer;

        NodePtr currentReader = (reader->type() == AVRO_SYMBOLIC) ?
            resolveSymbol(reader) : reader;

        static const BuilderFunc funcs[] = {
            &DynamicBuilder::buildPrimitive<std::string>, 
            &DynamicBuilder::buildPrimitive<std::vector<uint8_t> >,
            &DynamicBuilder::buildPrimitive<int32_t>,
            &DynamicBuilder::buildPrimitive<int64_t>,
            &DynamicBuilder::buildPrimitive<float>,
            &DynamicBuilder::buildPrimitive<double>,
            &DynamicBuilder::buildPrimitive<bool>,
            &DynamicBuilder::buildPrimitive<Null>,
            &DynamicBuilder::buildCompound<RecordParser, RecordSkipper>,
            &DynamicBuilder::buildCompound<EnumParser, EnumSkipper>,
            &DynamicBuilder::buildCompound<ArrayParser, ArraySkipper>,
            &DynamicBuilder::buildCompound<MapParser, MapSkipper>,
            &DynamicBuilder::buildCompound<UnionParser, UnionSkipper>,
            &DynamicBuilder::buildCompound<FixedParser, FixedSkipper>
        };

        BOOST_STATIC_ASSERT( (sizeof(funcs)/sizeof(BuilderFunc)) == (AVRO_NUM_TYPES) );

        BuilderFunc func = funcs[currentWriter->type()];
        assert(func);

        return  ((this)->*(func))(currentWriter, currentReader, offset);
    }

    Instruction *
    skipper(const NodePtr &writer) 
    {

        typedef Instruction* (DynamicBuilder::*BuilderFunc)(const NodePtr &writer);

        NodePtr currentWriter = (writer->type() == AVRO_SYMBOLIC) ?
            writer->leafAt(0) : writer;

        static const BuilderFunc funcs[] = {
            &DynamicBuilder::buildPrimitiveSkipper<std::string>, 
            &DynamicBuilder::buildPrimitiveSkipper<std::vector<uint8_t> >,
            &DynamicBuilder::buildPrimitiveSkipper<int32_t>,
            &DynamicBuilder::buildPrimitiveSkipper<int64_t>,
            &DynamicBuilder::buildPrimitiveSkipper<float>,
            &DynamicBuilder::buildPrimitiveSkipper<double>,
            &DynamicBuilder::buildPrimitiveSkipper<bool>,
            &DynamicBuilder::buildPrimitiveSkipper<Null>,
            &DynamicBuilder::buildCompoundSkipper<RecordSkipper>,
            &DynamicBuilder::buildCompoundSkipper<EnumSkipper>,
            &DynamicBuilder::buildCompoundSkipper<ArraySkipper>,
            &DynamicBuilder::buildCompoundSkipper<MapSkipper>,
            &DynamicBuilder::buildCompoundSkipper<UnionSkipper>,
            &DynamicBuilder::buildCompoundSkipper<FixedSkipper>
        };

        BOOST_STATIC_ASSERT( (sizeof(funcs)/sizeof(BuilderFunc)) == (AVRO_NUM_TYPES) );

        BuilderFunc func = funcs[currentWriter->type()];
        assert(func);

        return  ((this)->*(func))(currentWriter);
    }

    DynamicBuilder(const ValidSchema &writer, const ValidSchema &reader, const OffsetPtr &offset) :
        dparser_(build(writer.root(), reader.root(), *offset))
    { }

    DynamicParser 
    build()
    {
        return dparser_;
    }

  private:

    DynamicParser dparser_;

};


DynamicParser buildDynamicParser(const ValidSchema &writer, const ValidSchema &reader, const OffsetPtr &offset)
{
    DynamicBuilder b(writer, reader, offset);
    return b.build();
}

RecordSkipper::RecordSkipper(DynamicBuilder &builder, const NodePtr &writer) :
    Instruction() 
{
    size_t leaves = writer->leaves();
    for(size_t i = 0; i < leaves; ++i) {
        const NodePtr &w = writer->leafAt(i);
        instructions_.push_back(builder.skipper(w));
    }
}

RecordParser::RecordParser(DynamicBuilder &builder, const NodePtr &writer, const NodePtr &reader, const CompoundOffset &offsets) :
    Instruction()
{
    size_t leaves = writer->leaves();
    for(size_t i = 0; i < leaves; ++i) {
    
        const NodePtr &w = writer->leafAt(i);

        const std::string &name = writer->nameAt(i);

        size_t readerIndex = 0;
        bool found = reader->nameIndex(name, readerIndex);

        if(found) {
            const NodePtr &r = reader->leafAt(readerIndex);
            instructions_.push_back(builder.build(w, r, offsets.at(readerIndex)));
        }
        else {
            instructions_.push_back(builder.skipper(w));
        }
    }
}

MapSkipper::MapSkipper(DynamicBuilder &builder, const NodePtr &writer) :
    Instruction(),
    instruction_(builder.skipper(writer->leafAt(1)))
{ }

MapParser::MapParser(DynamicBuilder &builder, const NodePtr &writer, const NodePtr &reader, const CompoundOffset &offsets) :
    Instruction(),
    instruction_(builder.build(writer->leafAt(1), reader->leafAt(1), offsets.at(1))),
    offset_(offsets.offset()),
    setFuncOffset_( offsets.at(0).offset())
{ }

ArraySkipper::ArraySkipper(DynamicBuilder &builder, const NodePtr &writer) :
    Instruction(),
    instruction_(builder.skipper(writer->leafAt(0)))
{ }

ArrayParser::ArrayParser(DynamicBuilder &builder, const NodePtr &writer, const NodePtr &reader, const CompoundOffset &offsets) :
    Instruction(),
    instruction_(builder.build(writer->leafAt(0), reader->leafAt(0), offsets.at(1))),
    offset_(offsets.offset()),
    setFuncOffset_(offsets.at(0).offset())
{ }

UnionSkipper::UnionSkipper(DynamicBuilder &builder, const NodePtr &writer) :
    Instruction() 
{
    size_t leaves = writer->leaves();
    for(size_t i = 0; i < leaves; ++i) {
    const NodePtr &w = writer->leafAt(i);
        instructions_.push_back(builder.skipper(w));
    }
}

namespace {

// asumes the writer is NOT a union, and the reader IS a union

SchemaResolution    
checkUnionMatch(const NodePtr &writer, const NodePtr &reader, size_t &index)
{
    SchemaResolution bestMatch = RESOLVE_NO_MATCH;
 
    index = 0;
    size_t leaves = reader->leaves();

    for(size_t i=0; i < leaves; ++i) {

        const NodePtr &leaf = reader->leafAt(i);
        SchemaResolution newMatch = writer->resolve(*leaf);

        if(newMatch == RESOLVE_MATCH) {
            bestMatch = newMatch;
            index = i;
            break;
        }
        if(bestMatch == RESOLVE_NO_MATCH) {
            bestMatch = newMatch;
            index = i;
        }
    }

    return bestMatch;
}

};

UnionParser::UnionParser(DynamicBuilder &builder, const NodePtr &writer, const NodePtr &reader, const CompoundOffset &offsets) :
    Instruction(),
    offset_(offsets.offset()),
    choiceOffset_(offsets.at(0).offset()),
    setFuncOffset_(offsets.at(1).offset())
{

    size_t leaves = writer->leaves();
    for(size_t i = 0; i < leaves; ++i) {

        // for each writer, we need a schema match for the reader
        const NodePtr &w = writer->leafAt(i);
        size_t index = 0;

        SchemaResolution match = checkUnionMatch(w, reader, index);

        if(match == RESOLVE_NO_MATCH) {
            instructions_.push_back(builder.skipper(w));
            // push back a non-sensical number
            choiceMapping_.push_back(reader->leaves());
        }
        else {
            const NodePtr &r = reader->leafAt(index);
            instructions_.push_back(builder.build(w, r, offsets.at(index+2)));
            choiceMapping_.push_back(index);
        }
    }
}

NonUnionToUnionParser::NonUnionToUnionParser(DynamicBuilder &builder, const NodePtr &writer, const NodePtr &reader, const CompoundOffset &offsets) :
    Instruction(),
    offset_(offsets.offset()),
    choiceOffset_(offsets.at(0).offset()),
    setFuncOffset_(offsets.at(1).offset())
{

    SchemaResolution bestMatch = checkUnionMatch(writer, reader, choice_);
    assert(bestMatch != RESOLVE_NO_MATCH);
    instruction_.reset(builder.build(writer, reader->leafAt(choice_), offsets.at(choice_+2)));
}

UnionToNonUnionParser::UnionToNonUnionParser(DynamicBuilder &builder, const NodePtr &writer, const NodePtr &reader, const Offset &offsets) :
    Instruction()
{
    size_t leaves = writer->leaves();
    for(size_t i = 0; i < leaves; ++i) {
        const NodePtr &w = writer->leafAt(i);
        instructions_.push_back(builder.build(w, reader, offsets));
    }
}

} // namespace avro
