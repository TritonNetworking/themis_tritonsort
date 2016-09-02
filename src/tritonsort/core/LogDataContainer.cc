#include "LogDataContainer.h"
#include "core/LoggableDatum.h"
#include "core/LoggableStringDatum.h"
#include "core/LoggableTimeDurationDatum.h"
#include "core/LoggableUInt64Datum.h"
#include "core/Timer.h"

LogDataContainer::LogDataContainer(const LogLineDescriptor& parentDescriptor)
  : baseLogLineDescriptor(parentDescriptor) {

  baseLogLineDescriptor.setLogLineTypeName("DATM");
}

LogDataContainer::~LogDataContainer() {
  for (LoggableDatumMapIter iter = data.begin(); iter != data.end(); iter++) {
    for (LoggableDatumList::iterator listIter = (iter->second).begin();
         listIter != (iter->second).end(); listIter++) {
      delete *listIter;
    }
    iter->second.clear();
  }
  data.clear();

  for (LogLineDescriptorMapIter iter = descriptors.begin();
       iter != descriptors.end(); iter++) {
    delete iter->second;
  }
  descriptors.clear();
}

void LogDataContainer::add(const std::string& statName,
                         const std::string& datum) {
  add(statName, new LoggableStringDatum(datum));
}

void LogDataContainer::add(const std::string& statName, uint64_t datum) {
  add(statName, new LoggableUInt64Datum(datum));
}

void LogDataContainer::add(const std::string& statName, const Timer& timer) {
  add(statName, new LoggableTimeDurationDatum(timer));
}

void LogDataContainer::add(const std::string& statName,
                           LoggableDatum* datumObj) {
  data[statName].push_back(datumObj);

  LogLineDescriptorMapIter iter = descriptors.find(statName);

  if (iter == descriptors.end()) {
    descriptors[statName] = datumObj->createLogLineDescriptor(
      baseLogLineDescriptor, statName);
  }
}

void LogDataContainer::write(int fileDescriptor, const std::string& phaseName,
                             uint64_t epoch) {
  for (LoggableDatumMapIter iter = data.begin(); iter != data.end(); iter++) {
    const std::string& statName = iter->first;

    LogLineDescriptor* descriptor = descriptors[statName];
    TRITONSORT_ASSERT(descriptor != NULL, "Descriptor not created during stat insertion; "
           "something has gone horribly wrong.");

    LoggableDatumList& list = iter->second;

    for (LoggableDatumList::iterator listIter = list.begin();
         listIter != list.end(); listIter++) {
      LoggableDatum* datum = *listIter;
      datum->write(fileDescriptor, phaseName, epoch, *descriptor);
      delete datum;
    }

    list.clear();
  }
}

void LogDataContainer::addLogLineDescriptions(
  std::set<Json::Value>& descriptionSet) {

  for (LogLineDescriptorMapIter iter = descriptors.begin();
       iter != descriptors.end(); iter++) {
    descriptionSet.insert(iter->second->getDescriptionJson());
  }
}

bool LogDataContainer::empty() const {
  return data.empty();
}
