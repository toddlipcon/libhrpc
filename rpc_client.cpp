// Copyright (c) 2013, Cloudera, inc.
#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/bind.hpp>
#include <glog/logging.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <iostream>
#include "rpc_client.hpp"

#include "proto/IpcConnectionContext.pb.h"
#include "proto/RpcPayloadHeader.pb.h"
#include "proto/hadoop_rpc.pb.h"

namespace asio = boost::asio;
using std::string;
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;
using google::protobuf::io::StringOutputStream;
using boost::asio::ip::tcp;
using boost::shared_ptr;

namespace hrpc {

using internal::CallInProgress;

static const int kMaxVarint32Size = 5;

RpcClient::RpcClient(
    shared_ptr<asio::io_service> ios,
    const std::string &host, uint32_t port) :
  ios_(ios),
  resolver_(*ios),
  next_call_id_(0),
  rbuf_(0),
  rbuf_consumed_(0),
  rbuf_valid_(0),
  host_(host),
  port_(port)
{
}

void RpcClient::Connect(Callback cb) {
  asio::ip::tcp::resolver::query query(host_, boost::lexical_cast<std::string>(port_));
  resolver_.async_resolve(query,
      boost::bind(&RpcClient::ResolveCallback, this,
          asio::placeholders::error,
          asio::placeholders::iterator,
          cb));
}

void RpcClient::ResolveCallback(
    const boost::system::error_code &err,
    tcp::resolver::iterator endpoint_iterator,
    Callback callback) {
  if (!err) {
    tcp::endpoint endpoint = *endpoint_iterator;
    socket_.reset(new tcp::socket(*ios_));
    socket_->async_connect(endpoint,
        boost::bind(&RpcClient::ConnectCallback, this,
           asio::placeholders::error,
           ++endpoint_iterator,
           callback));
  } else {
    LOG(WARNING) << "error in resolve: " << err.message() ;
    err_ = err;
    callback();
  }
}

void RpcClient::ConnectCallback(
    const boost::system::error_code &err,
    tcp::resolver::iterator endpoint_iterator,
    Callback callback) {
  if (err) {
    err_ = err;
    callback();
    return;
  }
  DLOG(INFO) << "writing conn header" ;
  // Write connection header
  std::string header;
  PrepareConnectionHeader(header);
  asio::async_write(*socket_,
    boost::asio::buffer(header),
    boost::bind(&RpcClient::ConnHeaderWrittenCallback, this,
      asio::placeholders::error,
      callback));
}

static void AppendBigEndian32(std::string *str, uint32_t data) {
  uint32_t network_order = htonl(data);
  str->append(reinterpret_cast<const char *>(&network_order), sizeof(network_order));
}
static uint32_t ReadBigEndian32(const void *buf) {
  return ntohl(*(reinterpret_cast<const uint32_t *>(buf)));
}


void RpcClient::PrepareConnectionHeader(std::string &str) {
  /**
   * Write the connection header - this is sent when connection is established
   * +----------------------------------+
   * |  "hrpc" 4 bytes                  |      
   * +----------------------------------+
   * |  Version (1 bytes)               |      
   * +----------------------------------+
   * |  Authmethod (1 byte)             |      
   * +----------------------------------+
   * |  IpcSerializationType (1 byte)   |      
   * +----------------------------------+
   */
  str.append("hrpc");
  str.push_back((char)kRpcVersion);
  str.push_back((char)kSimpleAuthMethod);
  str.push_back((char)kProtobufSerializationType);

  // Following the header is the IpcConnectionContextProto
  // which gives authentication info, etc.
  // Prepare IpcConnectionContextProto
  IpcConnectionContextProto pb;
  pb.set_protocol("org.apache.hadoop.hdfs.protocol.ClientProtocol");
  pb.mutable_userinfo()->set_effectiveuser("todd");

  AppendBigEndian32(&str, pb.ByteSize());
  pb.AppendToString(&str);
}

void RpcClient::ConnHeaderWrittenCallback(
    const boost::system::error_code &err,
    Callback callback) {
  if (err) {
    // TODO: close socket?
    err_ = err;
    callback();
    return;
  }

  StartReader();

  callback();
}


// Move data in rbuf_ which falls between
// rbuf_consumed_ and rbuf_valid_ such that rbuf_consumed_ = 0 and
// any other buffered data follows it.
void RpcClient::CopyBufferedDataToStart() {
  size_t valid_len = rbuf_available();
  if (valid_len > 0) {
    memmove(&rbuf_[0], &rbuf_[rbuf_consumed_], valid_len);
  }
  rbuf_valid_ -= rbuf_consumed_;
  rbuf_consumed_ = 0;
  assert(rbuf_valid_ >= 0);
}

void RpcClient::StartReader() {
  CopyBufferedDataToStart();

  EnsureAvailableLength(1,
      boost::bind(&RpcClient::ReadResponseHeaderLengthCallback, this,
        asio::placeholders::error));
}

void RpcClient::ReadResponseHeaderLengthCallback(
    const boost::system::error_code &err) {
  if (err) {
    // TODO: kill all calls
    err_ = err;
    return;
  }

  uint32_t length;
  CodedInputStream cis(&rbuf_[rbuf_consumed_], rbuf_available());
  bool success = cis.ReadVarint32(&length);
  if (!success) {
    if (rbuf_available() >= kMaxVarint32Size) {
      // TODO: error handling
      LOG(FATAL) << "bad varint";
    }
    // Not enough bytes in buffer to read varint, ask for at least another byte
    EnsureAvailableLength(rbuf_available() + 1,
        boost::bind(&RpcClient::ReadResponseHeaderLengthCallback, this,
          asio::placeholders::error));
    return;
  }

  DLOG(INFO) << "read length for response: " << length ;
  rbuf_consumed_ += rbuf_available() - cis.BytesUntilLimit();
  DLOG(INFO) << "consumed " << rbuf_consumed_ ;

  EnsureAvailableLength(length,
    boost::bind(&RpcClient::ResponseHeaderReadCallback, this,
      boost::asio::placeholders::error,
      length));
}

void RpcClient::ResponseHeaderReadCallback(
  const boost::system::error_code &err,
  size_t header_length) {

  if (err) {
    DLOG(INFO) << "could not read response header" ;
    return;
  }

  assert(rbuf_available() >= header_length);

  DLOG(INFO) << "read response header" ;
  shared_ptr<RpcResponseHeaderProto> hdr(new RpcResponseHeaderProto());
  bool parsed = hdr->ParseFromArray(&rbuf_[rbuf_consumed_], header_length);
  if (!parsed) {
    LOG(WARNING) << "Could not parse: " << hdr->InitializationErrorString() ;
    return;
  }
  rbuf_consumed_ += header_length;

  hdr->PrintDebugString();

  EnsureAvailableLength(4,
    boost::bind(&RpcClient::ResponseLengthReadCallback, this,
      boost::asio::placeholders::error,
      hdr));
}

void RpcClient::ResponseLengthReadCallback(
  const boost::system::error_code &err,
  shared_ptr<RpcResponseHeaderProto> hdr) {
  if (err) {
    LOG(WARNING) << "could not read response length" ;
    return;
  }

  assert(rbuf_available() >= 4);

  size_t resp_length = ReadBigEndian32(&rbuf_[rbuf_consumed_]);
  rbuf_consumed_ += sizeof(uint32_t);

  EnsureAvailableLength(resp_length,
    boost::bind(&RpcClient::ResponseReceivedCallback, this,
      boost::asio::placeholders::error,
      hdr, resp_length));
}

void RpcClient::ResponseReceivedCallback(
    const boost::system::error_code &err,
    shared_ptr<RpcResponseHeaderProto> hdr,
    size_t length) {
  if (err) {
    LOG(WARNING) << "could not read response length" ;
    return;
  }

  assert(rbuf_available() >= length);

  CallMap::const_iterator it = calls_.find(hdr->callid());
  if (it == calls_.end()) {
    LOG(WARNING) << "Got response for call " << hdr->callid()
        << " but no such call was in the pending call map!";
    // Still need to consume whatever data it returned,
    // even if we have no idea what it is
    rbuf_consumed_ += length;
    return;
  }

  shared_ptr<CallInProgress> call = (*it).second;

  bool parsed = call->result_->ParseFromArray(&rbuf_[rbuf_consumed_], length);
  if (!parsed) {
    LOG(FATAL) << "Could not parse call: " << call->result_->InitializationErrorString() ;
    call->callback_();
  }
  call->result_->PrintDebugString();

  rbuf_consumed_ += length;
  call->callback_();

  StartReader();
}

////////////////////////////////////////////////////////////////////////////////
// Reader utility functions
////////////////////////////////////////////////////////////////////////////////

void RpcClient::EnsureAvailableLength(size_t length, EnsureLengthCallback handler) {

  int already_in_buffer = rbuf_valid_ - rbuf_consumed_;
  int needed = length - already_in_buffer;
  if (needed <= 0) {
    DLOG(INFO) << "needed " << needed << ", already had "
        << already_in_buffer << " in buffer" ;
    handler(boost::system::error_code());
    return;
  }

  // Expand buffer to fit the whole requested length, if necessary.
  rbuf_.resize(std::max(rbuf_consumed_ + length, rbuf_.capacity()));
  size_t buf_avail = rbuf_.size() - rbuf_valid_;

  DLOG(INFO) << "need " << needed << " more bytes in buffer"
    << ". Reading up to " << buf_avail << " bytes at " << rbuf_valid_;
  
  asio::async_read(*socket_,
    boost::asio::buffer(&rbuf_[rbuf_valid_], buf_avail),
    boost::asio::transfer_at_least(needed),
    boost::bind(&RpcClient::EnsureAvailableLengthCallback, this,
      boost::asio::placeholders::error,
      boost::asio::placeholders::bytes_transferred,
      handler));
}

void RpcClient::EnsureAvailableLengthCallback(
  const boost::system::error_code &err,
  size_t length_read,
  EnsureLengthCallback handler) {

  DLOG(INFO) << "successfully read " << length_read << "bytes";
  rbuf_valid_ += length_read;
  handler(err);
}

////////////////////////////////////////////////////////////////////////////////
// Actual call invocation  
////////////////////////////////////////////////////////////////////////////////
  

void RpcClient::Call(const string &declaring_class,
          uint64_t protocol_version,
          const string &method_name,
          const pb::Message *request,
          shared_ptr<pb::Message> result,
          Callback callback) {

  call_id_t call_id = next_call_id_++;
  shared_ptr<CallInProgress> call(new CallInProgress(call_id,
    result, callback));

  // Set up the Payload Header
  RpcPayloadHeaderProto header;
  header.set_rpckind(RPC_PROTOCOL_BUFFER);
  header.set_rpcop(RPC_FINAL_PAYLOAD);
  header.set_callid(call_id);
  header.ByteSize();

  // Set up the request itself
  HadoopRpcRequestProto req;
  req.set_methodname(method_name);
  req.set_declaringclassprotocolname(declaring_class);
  req.set_clientprotocolversion(protocol_version);
  req.set_request(request->SerializeAsString());
  req.ByteSize();

  std::string buf;
  buf.reserve(10 + req.GetCachedSize() + header.GetCachedSize());

  StringOutputStream sos(&buf);
  CodedOutputStream cos(&sos);

  // Write the payload header.
  cos.WriteVarint32(header.GetCachedSize());
  header.SerializeWithCachedSizes(&cos);

  // Write the req itself
  cos.WriteVarint32(req.GetCachedSize());
  req.SerializeWithCachedSizes(&cos);

  // The length prefix
  string length_prefix;
  AppendBigEndian32(&length_prefix, buf.size());

  std::vector<boost::asio::const_buffer> bufs;
  bufs.push_back(boost::asio::buffer(length_prefix));
  bufs.push_back(boost::asio::buffer(buf));

  calls_[call_id] = call;

  asio::async_write(*socket_,
    bufs,
    boost::bind(&RpcClient::CallWrittenCallback, this,
      asio::placeholders::error,
      call));
}

void RpcClient::CallWrittenCallback(
    const boost::system::error_code &err,
    shared_ptr<CallInProgress> call) {
  if (err) {
    calls_.erase(call->call_id_);
    err_ = err;
    call->callback_();
    return;
  }
}

}
