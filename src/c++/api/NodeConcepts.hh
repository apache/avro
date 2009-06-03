#ifndef avro_NodeConcepts_hh__
#define avro_NodeConcepts_hh__

#include <vector>

namespace avro {


/// 
/// The concept classes are used to simplify NodeImpl.  Since different types
/// of avro types carry different attributes, such as names, or field names for
/// record members.  Using the concept class of NoAttribute vs Attribute, the
/// NodeImpl object can enable/disable the attribute, but the code is the same
/// in either case.
///
/// Furthermore, attributes may have different types, for example, most
/// attributes are strings, but fixed types have a size attribute, which is
/// integer.
///
/// Since compound types are composed of other types, the leaf attribute
/// concepts extend a NodeImpl to include leaf nodes, and attributes for leaf
/// nodes, which are used to build parse trees.
///
///

namespace concepts {

template <typename Attribute>
struct NoAttribute
{
    static const bool hasAttribute = false;

    const Attribute &get() const {
        static Attribute empty;
        throw Exception("This type does not have attribute");
        return empty;
    }

    void set(const Attribute &value) {
        throw Exception("This type does not have attribute");
    }
};

template <typename Attribute>
struct HasAttribute
{
    static const bool hasAttribute = true;

    const Attribute &get() const {
        return val_;
    }

    void set(const Attribute &val) {
        val_ = val;
    }

  private:
    Attribute val_;
};


template<typename LeafType>
struct NoLeafAttributes
{
    static const bool hasAttribute = false;

    NoLeafAttributes(size_t min, size_t max) 
    {}

    size_t size() const {
        return 0;
    }

    void add( const LeafType &newLeaf) {
        throw Exception("This type does not have leaf types");
    }

    const LeafType &at(size_t index) const {
        static LeafType null;
        throw Exception("This type does not have leaf types");
        return null;
    }

    bool inRange() const {
        return true;
    }
};

template<typename LeafType>
struct HasLeafAttributes
{
    static const bool hasAttribute = true;

    HasLeafAttributes(size_t min, size_t max) :
        minSize_(min), maxSize_(max)
    {
        attrs_.reserve(minSize_);
    }

    size_t size() const {
        return attrs_.size();
    }

    void add(const LeafType &attr) {
        if(attrs_.size() == maxSize_) {
            throw Exception("Too many attributes");
        }
        attrs_.push_back(attr); 
    }

    const LeafType &at(size_t index) const {
        return attrs_.at(index);
    }

    bool inRange() const {
        size_t size = attrs_.size();
        return size >= minSize_ && size <= maxSize_;
    }

  private:

    std::vector<LeafType> attrs_;
    const size_t minSize_;
    const size_t maxSize_;
};


} // namespace concepts
} // namespace avro

#endif
