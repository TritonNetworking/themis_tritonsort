#include <boost/lexical_cast.hpp>
#include <errno.h>
#include <netinet/tcp.h>
#include <sys/ioctl.h>
#include <sys/types.h>

#include "mapreduce/workers/sender/Connection.h"

Connection::Connection(
  Socket& _socket, StatLogger& _senderLogger, bool _enhancedNetworkLogging)
  : enhancedNetworkLogging(_enhancedNetworkLogging),
    socket(_socket),
    buffer(NULL),
    metadata(NULL),
    metadataBytesSent(0),
    bufferBytesSent(0),
    totalBytesSent(0),
    broken(false),
    loggingSuffix(
      "_" + boost::lexical_cast<std::string>(socket.getFlowID()) + "_"+
      boost::lexical_cast<std::string>(socket.getPeerID())),
    senderLogger(_senderLogger) {
  connectionTimer.start();

  // Log some information about the connection.
  struct tcp_info info;
  socklen_t infoSize = sizeof(info);
  int status = getsockopt(socket.getFD(), SOL_TCP, TCP_INFO, &info, &infoSize);
  ABORT_IF(status != 0,
           "getsockopt() failed with errno %d: %s", errno, strerror(errno));

  senderLogger.logDatum("mss" + loggingSuffix, info.tcpi_advmss);
  senderLogger.logDatum("snd_wscale" + loggingSuffix, info.tcpi_snd_wscale);
}

Connection::~Connection() {
  // Sanity check: make sure the socket is actually closed.
  ASSERT(socket.closed(), "On destruction, flow %llu to peer %llu still open.",
         socket.getFlowID(), socket.getPeerID());

  // Sanity check: we shouldn't have any more data to send to this peer.
  ASSERT(buffer == NULL, "Found non-NULL buffer for peer %llu, flow %llu "
         "during teardown", socket.getPeerID(), socket.getFlowID());
  ASSERT(metadata == NULL, "Found non-NULL metadata for peer %llu, flow %llu "
         "during teardown", socket.getPeerID(), socket.getFlowID());
}

void Connection::close() {
  // Close the socket.
  socket.close();

  // Log the time the connection was open.
  connectionTimer.stop();
  senderLogger.logDatum("connection_time" + loggingSuffix, connectionTimer);
  senderLogger.logDatum("total_bytes_sent" + loggingSuffix, totalBytesSent);
}


void Connection::registerWithIntervalLogger(StatLogger& intervalLogger) {
  if (enhancedNetworkLogging) {
    bytesSentStatID = intervalLogger.registerStat("bytes_sent" + loggingSuffix);

    congestionWindowStatID = intervalLogger.registerStat(
      "cwnd" + loggingSuffix);
    retransmitsStatID = intervalLogger.registerStat(
      "retransmits" + loggingSuffix);
    rttStatID = intervalLogger.registerStat("rtt" + loggingSuffix);
    rttVarianceStatID = intervalLogger.registerStat(
      "rtt_variance" + loggingSuffix);
    senderSlowStartThresholdStatID = intervalLogger.registerStat(
      "snd_ssthresh" + loggingSuffix);
    lastDataSentStatID = intervalLogger.registerStat(
      "last_data_sent" + loggingSuffix);
    sendQueueSizeStatID = intervalLogger.registerStat(
      "send_queue_size" + loggingSuffix);
  }
}

void Connection::logIntervalStats(StatLogger& intervalLogger) {
  if (!socket.closed() && enhancedNetworkLogging) {
    intervalLogger.add(bytesSentStatID, totalBytesSent);

    // Get TCP info.
    struct tcp_info info;
    socklen_t infoSize = sizeof(info);
    int status = getsockopt(
      socket.getFD(), SOL_TCP, TCP_INFO, &info, &infoSize);
    ABORT_IF(status != 0,
             "getsockopt() interval stats failed with errno %d: %s", errno,
             strerror(errno));

    // Log the congestion window and the number of retransmits
    intervalLogger.add(congestionWindowStatID, info.tcpi_snd_cwnd);
    intervalLogger.add(retransmitsStatID, info.tcpi_total_retrans);
    intervalLogger.add(rttStatID, info.tcpi_rtt);
    intervalLogger.add(rttVarianceStatID, info.tcpi_rttvar);
    intervalLogger.add(senderSlowStartThresholdStatID, info.tcpi_snd_ssthresh);
    intervalLogger.add(lastDataSentStatID, info.tcpi_last_data_sent);

    // Get the number of unsent bytes in the send queue.
    int unsentBytes = 0;
    status = ioctl(socket.getFD(), TIOCOUTQ, &unsentBytes);
    ABORT_IF(status != 0, "ioctl(TIOCOUTQ) failed with errno %d: %s",
             errno, strerror(errno));
    intervalLogger.add(sendQueueSizeStatID, unsentBytes);
  }
}
