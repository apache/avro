#ifndef avro_NodeImpl_hh__
#define avro_NodeImpl_hh__

#include <limits>

#include "Node.hh"
#include "NodeConcepts.hh"

namespace avro {

/// Implementation details for Node.  NodeImpl represents all the avro types,
/// whose properties are enabled are disabled by selecting concept classes.

template 
< 
    class NameConcept,
    class LeavesConcept,
    class LeafNamesConcept,
    class SizeConcept
>
class NodeImpl : public Node
{

  protected:

    NodeImpl(Type type, size_t minLeaves = 0, size_t maxLeaves = 0) :
        Node(type),
        leafAttributes_(minLeaves, maxLeaves),
        leafNamesAttributes_(minLeaves, maxLeaves)
    { }

    bool hasName() const {
        return nameAttribute_.hasAttribute;
    }

    void doSetName(const std::string &name) {
        nameAttribute_.set(name);
    }
    
    const std::string &name() const {
        return nameAttribute_.get();
    }

    void doAddLeaf(const NodePtr &newLeaf) { 
        leafAttributes_.add(newLeaf);
    }

    size_t leaves() const {
        return leafAttributes_.size();
    }

    const NodePtr &leafAt(int index) const { 
        return leafAttributes_.at(index);
    }

    void doAddName(const std::string &name) { 
        leafNamesAttributes_.add(name);
    }

    size_t names() const {
        return leafNamesAttributes_.size();
    }

    const std::string &nameAt(int index) const { 
        return leafNamesAttributes_.at(index);
    }

    void doSetFixedSize(int size) {
        sizeAttribute_.set(size);
    }

    int fixedSize() const {
        return sizeAttribute_.get();
    }

    bool isValid() const {
        return leafAttributes_.inRange() && leafAttributes_.inRange();
    }

    void printBasicInfo(std::ostream &os) const;

    void setLeafToSymbolic(int index);
   
    NameConcept nameAttribute_;
    LeavesConcept leafAttributes_;
    LeafNamesConcept leafNamesAttributes_;
    SizeConcept sizeAttribute_;
};

typedef concepts::NoAttribute<std::string>  NoName;
typedef concepts::HasAttribute<std::string> HasName;

typedef concepts::NoLeafAttributes<NodePtr>  NoLeaves;
typedef concepts::HasLeafAttributes<NodePtr> HasLeaves;

typedef concepts::NoLeafAttributes<std::string>  NoLeafNames;
typedef concepts::HasLeafAttributes<std::string> HasLeafNames;

typedef concepts::NoAttribute<int>  NoSize;
typedef concepts::HasAttribute<int> HasSize;

typedef NodeImpl< NoName,  NoLeaves,  NoLeafNames,  NoSize > NodeImplPrimitive;
typedef NodeImpl< HasName, NoLeaves,  NoLeafNames,  NoSize > NodeImplSymbolic;

typedef NodeImpl< HasName, HasLeaves, HasLeafNames, NoSize > NodeImplRecord;
typedef NodeImpl< HasName, NoLeaves,  HasLeafNames, NoSize > NodeImplEnum;
typedef NodeImpl< NoName,  HasLeaves, NoLeafNames,  NoSize > NodeImplArray;
typedef NodeImpl< NoName,  HasLeaves, NoLeafNames,  NoSize > NodeImplMap;
typedef NodeImpl< NoName,  HasLeaves, NoLeafNames,  NoSize > NodeImplUnion;
typedef NodeImpl< HasName, NoLeaves,  NoLeafNames,  HasSize > NodeImplFixed;

class NodePrimitive : public NodeImplPrimitive
{
  public:

    NodePrimitive(Type type) :
        NodeImplPrimitive(type)
    { }
    void printJson(std::ostream &os, int depth) const;
};

class NodeSymbolic : public NodeImplSymbolic
{
  public:

    NodeSymbolic() :
        NodeImplSymbolic(AVRO_SYMBOLIC)
    { }
    void printJson(std::ostream &os, int depth) const;

};

class NodeRecord : public NodeImplRecord
{
  public:

    NodeRecord() :
        NodeImplRecord(AVRO_RECORD, 1, std::numeric_limits<size_t>::max()) 
    { }
    void printJson(std::ostream &os, int depth) const;
};

class NodeEnum : public NodeImplEnum
{
  public:

    NodeEnum() :
        NodeImplEnum(AVRO_ENUM, 1, std::numeric_limits<size_t>::max()) 
    { }
    void printJson(std::ostream &os, int depth) const;
};

class NodeArray : public NodeImplArray
{
  public:

    NodeArray() :
        NodeImplArray(AVRO_ARRAY, 1, 1)
    { }
    void printJson(std::ostream &os, int depth) const;
};

class NodeMap : public NodeImplMap
{
  public:

    NodeMap() :
        NodeImplMap(AVRO_MAP, 2, 2)
    { 
         NodePtr key(new NodePrimitive(AVRO_STRING));
         doAddLeaf(key);
    }
    void printJson(std::ostream &os, int depth) const;
};

class NodeUnion : public NodeImplUnion
{
  public:

    NodeUnion() :
        NodeImplUnion(AVRO_UNION, 2, std::numeric_limits<size_t>::max())
    { }
    void printJson(std::ostream &os, int depth) const;
};

class NodeFixed : public NodeImplFixed
{
  public:

    NodeFixed() :
        NodeImplFixed(AVRO_FIXED)
    { }
    void printJson(std::ostream &os, int depth) const;
};

template < class A, class B, class C, class D >
inline void 
NodeImpl<A,B,C,D>::setLeafToSymbolic(int index)
{
    if(!leafAttributes_.hasAttribute) {
        throw Exception("Cannot change leaf node for nonexistent leaf");
    } 
    NodePtr symbol(new NodeSymbolic);

    NodePtr &node = const_cast<NodePtr &>(leafAttributes_.at(index));
    symbol->setName(node->name());
    node = symbol;
}

template < class A, class B, class C, class D >
inline void 
NodeImpl<A,B,C,D>::printBasicInfo(std::ostream &os) const
{
    os << type();
    if(hasName()) {
        os << " " << nameAttribute_.get();
    }
    if(sizeAttribute_.hasAttribute) {
        os << " " << sizeAttribute_.get();
    }
    os << '\n';
    int count = leaves();
    count = count ? count : names();
    for(int i= 0; i < count; ++i) {
        if( leafNamesAttributes_.hasAttribute ) {
            os << "name " << nameAt(i) << '\n';
        }
        if( leafAttributes_.hasAttribute) {
            leafAt(i)->printBasicInfo(os);
        }
    }
    if(isCompound(type())) {
        os << "end " << type() << '\n';
    }
}

} // namespace avro

#endif
