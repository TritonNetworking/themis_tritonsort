#ifndef THEMIS_MAPRED_SENDER_IMPLS_H
#define THEMIS_MAPRED_SENDER_IMPLS_H

#include "core/ImplementationList.h"
#include "mapreduce/workers/sender/SelectSender.h"
#include "mapreduce/workers/sender/Sender.h"

/// Implementations of the Sender stage
class SenderImpls : public ImplementationList {
public:
  /// Declare implementations of the Sender stage
  /**
     \sa Sender
     \sa SelectSender
   */
  SenderImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(SelectSender, "SelectSender");
    ADD_IMPLEMENTATION(Sender, "Sender");
  }
};

#endif // THEMIS_MAPRED_SENDER_IMPLS_H
