#ifndef avro_ValidSchema_hh__ 
#define avro_ValidSchema_hh__ 

#include <boost/noncopyable.hpp>

#include "Node.hh"
#include "SymbolMap.hh"

namespace avro {

class Schema;

/// A ValidSchema is basically a non-mutable Schema that has passed some
/// minumum of sanity checks.  Once valididated, any Schema that is part of
/// this ValidSchema is considered locked, and cannot be modified (an attempt
/// to modify a locked Schema will throw).  Also, as it is validated, any
/// recursive duplications of schemas are replaced with symbolic links to the
/// original.
///
/// Once a Schema is converted to a valid schema it can be used in validating
/// parsers/serializers, converted to a json schema, etc.
///

class ValidSchema : private boost::noncopyable
{
  public:

    explicit ValidSchema(const Schema &schema);
    ValidSchema();

    void setSchema(const Schema &schema);

    const NodePtr &root() const {
        return node_;
    }

    void toJson(std::ostream &os);

    void toFlatList(std::ostream &os);

    NodePtr followSymbol(const std::string &name) const {
        return symbolMap_.locateSymbol(name);
    }

  protected:

    bool validate(const NodePtr &node);

    SymbolMap symbolMap_;
    NodePtr node_;
};

} // namespace avro

#endif
