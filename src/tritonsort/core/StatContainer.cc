#include "core/StatContainer.h"

StatContainer::StatContainer(
  uint64_t _parentLoggerID, uint64_t _containerID, const std::string& _statName)
  : parentLoggerID(_parentLoggerID),
    containerID(_containerID),
    statName(_statName) {
}

StatContainer::~StatContainer() {

}

uint64_t StatContainer::getParentLoggerID() const {
  return parentLoggerID;
}

uint64_t StatContainer::getContainerID() const {
  return containerID;
}

const std::string& StatContainer::getStatName() const {
  return statName;
}
