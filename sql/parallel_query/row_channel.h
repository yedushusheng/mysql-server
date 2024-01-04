#ifndef PARALLEL_QUERY_ROW_CHANNEL_H
#define PARALLEL_QUERY_ROW_CHANNEL_H
#include "sql/parallel_query/comm_types.h"
class MEM_ROOT;

namespace pq {
namespace comm {
class RowChannel {
 public:
  enum class Type {MEM, TCP, BRPC_STREAM};

  virtual ~RowChannel() {}
  using Result = RowTxResult;

  virtual bool Init(THD *thd, Event *event, bool receiver) = 0;

  virtual Result Send(std::size_t nbytes, const void *data, bool nowait) = 0;
  virtual Result Receive(std::size_t *nbytesp, void **datap, bool nowait,
                         MessageBuffer *buf) = 0;
  virtual Result SendEOF() = 0;

  // Call this function to send EOF after all rows are sent out.
  virtual void Close() = 0;
  virtual bool IsClosed() const = 0;
};

RowChannel *CreateMemRowChannel(MEM_ROOT *mem_root, RowChannel *peer);
void SetPeerEventForMemChannel(RowChannel *channel, Event *peer_event);

RowChannel *CreateTcpRowChannel(MEM_ROOT *mem_root, int *sock);

bool CreateBrpcStreamRowChannelPair(MEM_ROOT *mem_root, RowChannel **receiver,
                                    RowChannel **sender);

}  // namespace comm
}  // namespace pq
#endif
