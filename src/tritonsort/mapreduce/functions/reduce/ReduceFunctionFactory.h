#ifndef MAPRED_REDUCE_FUNCTION_FACTORY_H
#define MAPRED_REDUCE_FUNCTION_FACTORY_H

#include "core/constants.h"

class Params;
class ReduceFunction;

/**
   Constructs one of a family of reduce functions based on the value of the
   REDUCE_FUNCTION parameter
 */
class ReduceFunctionFactory {
public:
  /// Construct a new reduce function
  /**
     \param params a pointer to a Params object whose REDUCE_FUNCTION parameter
     will be used to determine which reduce function to construct

     \return a new reduce function instance
   */
  static ReduceFunction* getNewReduceFunctionInstance(
    const std::string& reduceName, const Params& params);

private:
  ReduceFunctionFactory();
};

#endif // MAPRED_REDUCE_FUNCTION_FACTORY_H
