// Copyright (c) 2013, Cloudera, inc.
#include <boost/asio.hpp>
#include <boost/function.hpp>
#include <map>

namespace google {
  namespace protobuf {
    class Message;
  }
}

class RpcResponseHeaderProto;

namespace hrpc {
using ::boost::shared_ptr;
using ::boost::asio::ip::tcp;
using ::boost::shared_ptr;
using ::std::string;
using ::boost::system::error_code;

namespace pb = ::google::protobuf;

typedef uint32_t CallId;

typedef boost::function<void (void)> Callback;

namespace internal {
  class CallInProgress {
  public:
    CallInProgress(CallId call_id, shared_ptr<pb::Message> result,
        Callback cb) :
      call_id_(call_id),
      result_(result),
      callback_(cb) {
    }

    CallId call_id_;
    shared_ptr<pb::Message> result_;
    Callback callback_;
  };
}


class RpcClient {
public:

  RpcClient(shared_ptr<boost::asio::io_service> ios,
    const string &host, uint32_t port);

  void Connect(Callback callback);

  void Call(const string &declaring_class,
            uint64_t protocol_version,
            const string &method_name,
            const pb::Message *request,
            shared_ptr<pb::Message> result,
            Callback cb);

  error_code err() {
    return err_;
  }

private:

  //////////////////////////////////////////////////////////////////////
  // Connection process
  //////////////////////////////////////////////////////////////////////

  
  void ResolveCallback(
      const error_code &err,
      tcp::resolver::iterator endpoint_iterator,
      Callback callback);
  void ConnectCallback(
      const error_code &err,
      tcp::resolver::iterator endpoint_iterator,
      Callback callback);

  void PrepareConnectionHeader(std::string &str);

  void ConnHeaderWrittenCallback(
      const error_code &err,
      Callback callback);


  // Callback for after the call has been written to the wire.
  // In the case of success, this doesn't need to do anything, since
  // the actual user callback will be invoked when the response arrived.
  // But, if the call is never written to the wire, this is responsible
  // for setting the error state and invoking the error callback.
  void CallWrittenCallback(
      const error_code &err,
      shared_ptr<internal::CallInProgress> call);


  //////////////////////////////////////////////////////////////////////

  // Reader loop methods:

  // Starts the reader process. If there is any left-over buffered
  // data in the input buffer, this is responsible for shifting it
  // back to the start of the buffer.
  void StartReader();

  // As soon as there are any bytes, try to parse the buffer to determine
  // the length of the response header. This will loop back to itself
  // until there are enough bytes to decode a varint, at which point
  // it will read that number of bytes, calling back to
  // ResponseHeaderReadCallback
  void ReadResponseHeaderLengthCallback(
    const error_code &err);

  // Once the Response Header is available in the buffer, parse it
  // and then read the Response Length, forwarding to
  // ResponseLengthReadCallback
  void ResponseHeaderReadCallback(
    const error_code &err,
    size_t header_len);

  // The length of the response body has been read. This decodes it and
  // issues the read for the response body itself.
  void ResponseLengthReadCallback(
    const error_code &err,
    shared_ptr<RpcResponseHeaderProto> hdr);

  // The entire response body is available in the buffer. This decodes it,
  // invokes the user's callback, and then restarts the reader loop
  // by calling StartReader()
  void ResponseReceivedCallback(
    const error_code &err,
    shared_ptr<RpcResponseHeaderProto> hdr,
    size_t length);


  //////////////////////////////////////////////////////////////////////

  // Reader utility methods:
  // The reader operates on a buffer, rbuf_, which contains read-but-not-parsed
  // data.
  // The buffer is organized as follows:
  // <CCCCCAAAAAA----------------->
  //       |    |
  //       |    \- last available byte: index _rbuf_valid
  //       \- first available byte: _rbuf_consumed
  // This structure allows the reader to "read ahead" from the socket
  // and each function only needs to consume the required data.


  typedef boost::function<void (const error_code &err)> EnsureLengthCallback;

  // Issue a read to rbuf_ such that, when the given callback is invoked,
  // either rbuf_available() will return at least 'length', or an error
  // will be flagged.
  void EnsureAvailableLength(size_t length, EnsureLengthCallback callback);

  // Callback for above process.
  void EnsureAvailableLengthCallback(
    const error_code &err,
    size_t length_read,
    EnsureLengthCallback handler);

  // Reorganize the rbuf_ buffer such that any available data is moved to
  // the front of the buffer.
  void CopyBufferedDataToStart();

  // Return the number of bytes which are valid in the buffer but not yet
  // consumed.
  size_t rbuf_available() {
    assert(rbuf_valid_ >= rbuf_consumed_);
    return rbuf_valid_ - rbuf_consumed_;
  }

  //////////////////////////////////////////////////////////////////////

  shared_ptr<boost::asio::io_service> ios_;
  tcp::resolver resolver_;
  shared_ptr<tcp::socket> socket_;

  // Map of in-progress calls.
  // These calls have been sent out, but their responses not yet read.
  // This allows out-of-order responses by the server.
  typedef std::map<CallId, shared_ptr<internal::CallInProgress> > CallMap;
  CallMap calls_;

  // The call id that will be used for the next RPC.
  CallId next_call_id_;

  // The buffer used for the reader loop. See above for details.
  std::vector<uint8_t> rbuf_;
  size_t rbuf_consumed_;
  size_t rbuf_valid_;

  error_code err_;
  std::string host_;
  uint32_t port_;

  static const uint8_t kRpcVersion = 7;
  static const uint8_t kSimpleAuthMethod = 80;
  static const uint8_t kProtobufSerializationType = 0;
};

}
