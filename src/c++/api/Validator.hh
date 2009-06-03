#ifndef avro_Validating_hh__
#define avro_Validating_hh__

#include <boost/noncopyable.hpp>
#include <vector>

#include "Types.hh"
#include "Node.hh"

namespace avro {

class ValidSchema;
class OutputStreamer;

/// This class is used by both the ValidatingSerializer and ValidationParser
/// objects.  It advances the parse tree (containing logic how to advance
/// through the various compound types, for example a record must advance
/// through all leaf nodes but a union only skips to one), and reports which
/// type is next.

class Validator : private boost::noncopyable
{
    typedef uint64_t flag_t;

  public:

    Validator(const ValidSchema &schema);

    void advance();
    void advanceWithCount(int64_t val);

    bool typeIsExpected(Type type) const {
        return (expectedTypesFlag_ & typeToFlag(type));
    }

    Type nextTypeExpected() const {
        return nextType_;
    }

    int nextSizeExpected() const;

    bool getCurrentRecordName(std::string &name) const;
    bool getNextFieldName(std::string &name) const;

  private:

    flag_t typeToFlag(Type type) const {
        flag_t flag = (1L << type);
        return flag;
    }

    void setupOperation(const NodePtr &node);

    void setWaitingForCount();

    void recordAdvance();
    void enumAdvance();
    void countingAdvance();
    void unionAdvance();
    void fixedAdvance();

    void setupFlag(Type type);

    const ValidSchema &schema_;
    NodePtr parseTree_;

    Type nextType_; 
    flag_t expectedTypesFlag_;
    bool compoundStarted_;
    bool waitingForCount_;
    int64_t count_;

    struct CompoundType {
        explicit CompoundType(const NodePtr &n) :
            node(n), pos(0)
        {}
        NodePtr node;  ///< save the node
        size_t  pos; ///< track the leaf position to visit
    };

    std::vector<CompoundType> compoundStack_;
    std::vector<size_t> counters_;

};

} // namespace avro

#endif
