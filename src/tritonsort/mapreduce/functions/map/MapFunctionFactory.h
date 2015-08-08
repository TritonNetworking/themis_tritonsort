#ifndef MAPRED_MAP_FUNCTION_FACTORY_H
#define MAPRED_MAP_FUNCTION_FACTORY_H

#include "core/constants.h"

class MapFunction;
class Params;

class MapFunctionFactory {
public:
  static MapFunction* getNewMapFunctionInstance(
    const std::string& mapName, const Params& params);

private:
  DISALLOW_COPY_AND_ASSIGN(MapFunctionFactory);
  MapFunctionFactory();
};

#endif // MAPRED_MAP_FUNCTION_FACTORY_H
