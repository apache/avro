#ifndef avro_Node_hh__
#define avro_Node_hh__

#include <cassert>
#include <boost/noncopyable.hpp>
#include <boost/intrusive_ptr.hpp>

#include "Exception.hh"
#include "Types.hh"

namespace avro {

class Node;

typedef boost::intrusive_ptr<Node> NodePtr;


/// Node is the building block for parse trees.  Each node represents an avro
/// type.  Compound types have leaf nodes that represent the types they are
/// composed of.
///
/// The user does not use the Node object directly, they interface with Schema
/// objects.
///
/// The Node object has an embedded reference count for use in the
/// boost::intrusive_ptr smart pointer.  This is so that schemas may be reused
/// in other other schemas, without needing to worry about memory deallocation
/// for nodes that are added to multiple schema parse trees.
///
/// Node has minimal implementation, serving as an abstract base class for
/// different node types.
///

class Node : private boost::noncopyable
{
  public:

    Node(Type type) :
        type_(type),
        refCount_(0),
        locked_(false)
    {}

    virtual ~Node();

    Type type() const {
        return type_;
    }

    void lock() {
        locked_ = true;
    }

    bool locked() const {
        return locked_;
    }

    virtual bool hasName() const = 0;

    void setName(const std::string &name) {
        checkLock();
        checkName(name);
        doSetName(name);
    }
    virtual const std::string &name() const = 0;

    void addLeaf(const NodePtr &newLeaf) {
        checkLock();
        doAddLeaf(newLeaf);
    }
    virtual size_t leaves() const = 0;
    virtual const NodePtr& leafAt(int index) const = 0;

    void addName(const std::string &name) {
        checkLock();
        checkName(name);
        doAddName(name);
    }
    virtual size_t names() const = 0;
    virtual const std::string &nameAt(int index) const = 0;

    void setFixedSize(int size) {
        checkLock();
        doSetFixedSize(size);
    }
    virtual int fixedSize() const = 0;

    virtual bool isValid() const = 0;

    virtual void printJson(std::ostream &os, int depth) const = 0;

    virtual void printBasicInfo(std::ostream &os) const = 0;

  protected:

    friend class ValidSchema;

    virtual void setLeafToSymbolic(int index) = 0;

    void checkLock() const {
        if(locked()) {
            throw Exception("Cannot modify locked schema");
        }
    }

    void checkName(const std::string &name) const;

    virtual void doSetName(const std::string &name) = 0;
    virtual void doAddLeaf(const NodePtr &newLeaf) = 0;
    virtual void doAddName(const std::string &name) = 0;
    virtual void doSetFixedSize(int size) = 0;

  private:

    friend void intrusive_ptr_add_ref(Node *node);
    friend void intrusive_ptr_release(Node *node);

    const Type type_;
    int refCount_;
    bool locked_;
};

inline void 
intrusive_ptr_add_ref(Node *node) {
    ++(node->refCount_);
}

inline void 
intrusive_ptr_release(Node *node) {
    --(node->refCount_);
    if(node->refCount_ == 0) { 
        delete node;
    }
}

} // namespace avro

#endif
