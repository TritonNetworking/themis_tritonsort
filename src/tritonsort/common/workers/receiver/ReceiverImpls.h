#ifndef THEMIS_RECEIVER_IMPLS_H
#define THEMIS_RECEIVER_IMPLS_H

#include "common/workers/receiver/Receiver.h"
#include "common/workers/receiver/SelectReceiver.h"
#include "common/workers/receiver/SinkReceiver.h"
#include "core/ImplementationList.h"

/**
   Implementations for the Receiver stage
 */
template <typename OutFactory> class ReceiverImpls : public ImplementationList {
public:
  /// List Receiver stage implementations
  /**
     \sa Receiver<OutFactory>
     \sa SelectReceiver<OutFactory>
     \sa SinkReceiver
   */
  ReceiverImpls() : ImplementationList() {
    ADD_IMPLEMENTATION(Receiver<OutFactory>, "Receiver");
    ADD_IMPLEMENTATION(SelectReceiver<OutFactory>, "SelectReceiver");
    ADD_IMPLEMENTATION(SinkReceiver, "SinkReceiver");
  }
};

#endif // THEMIS_RECEIVER_IMPLS_H
