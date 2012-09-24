find_package( Protobuf REQUIRED )

PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS
  IpcConnectionContext.proto
  hadoop_rpc.proto
  hdfs.proto
  RpcPayloadHeader.proto
  ClientNamenodeProtocol.proto)
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})

SET(PROTO_DEPS ${PROTO_SRCS} ${PROTO_HDRS})

add_library(protos ${PROTO_DEPS})
