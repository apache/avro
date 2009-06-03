#include <boost/regex.hpp>
#include "Node.hh"

namespace avro {

Node::~Node()
{ }

void 
Node::checkName(const std::string &name) const
{
    static const boost::regex exp("[A-Za-z_][A-Za-z0-9_]*");
    if(!name.empty() && !boost::regex_match(name, exp)) {
        throw Exception("Names must match [A-Za-z_][A-Za-z0-9_]*");
    }
}

} // namespace avro
