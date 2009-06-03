#include "ValidSchema.hh"
#include "Schema.hh"
#include "Node.hh"

namespace avro {

    ValidSchema::ValidSchema(const Schema &schema) :
    node_(schema.root())
{
    validate(node_);
}

ValidSchema::ValidSchema() :
   node_(NullSchema().root()) 
{ }

void
ValidSchema::setSchema(const Schema &schema)
{
    const NodePtr &node(schema.root());
    validate(schema.root());
    node_ = node;
}

bool
ValidSchema::validate(const NodePtr &node) 
{
    if(!node) {
        node_ = new NodePrimitive(AVRO_NULL);
    }

    if(!node->isValid()) {
        throw Exception("Schema is invalid");
    }
    if(node->hasName()) {
        if(node->type() == AVRO_SYMBOLIC) {
            if(!symbolMap_.hasSymbol(node->name())) {
                throw Exception("Symbolic name not found");
            }
            return true;
        }
        bool registered = symbolMap_.registerSymbol(node);
        if(!registered) {
            return false;
        }
    }
    node->lock();
    size_t leaves = node->leaves();
    for(size_t i = 0; i < leaves; ++i) {
        const NodePtr &leaf(node->leafAt(i));

        if(! validate(leaf)) {
            node->setLeafToSymbolic(i);
        }
    }

    return true;
}

void 
ValidSchema::toJson(std::ostream &os)
{ 
    node_->printJson(os, 0);
    os << '\n';
}

void 
ValidSchema::toFlatList(std::ostream &os)
{ 
    node_->printBasicInfo(os);
}

} // namespace avro

