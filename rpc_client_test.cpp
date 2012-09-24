// Copyright (c) 2013, Cloudera, inc.
#include "rpc_client.hpp"

#include <iostream>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/future.hpp>
#include <boost/utility.hpp>
#include <glog/logging.h>

#include "proto/ClientNamenodeProtocol.pb.h"

namespace asio = boost::asio;

using boost::shared_ptr;
using namespace hrpc;

class Latch : boost::noncopyable {
public:
  Latch() : done_(false) {}

  void Trigger() {
    {
      boost::lock_guard<boost::mutex> lock(mutex_);
      done_ = true;
    }
    cond_.notify_all();
  }

  void Wait() {
    boost::unique_lock<boost::mutex> lock(mutex_);
    while (!done_) {
      cond_.wait(lock);
    }
  }

private:
  boost::condition_variable cond_;
  boost::mutex mutex_;
  bool done_;
};

void WorkerThread(shared_ptr<asio::io_service> ios) {
  DLOG(INFO) << "starting worker";
  ios->run();
  DLOG(INFO) << "stop worker";;
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);

  shared_ptr<asio::io_service> service(new asio::io_service());
  boost::shared_ptr< boost::asio::io_service::work > work(
          new boost::asio::io_service::work(*service));
  boost::thread worker(boost::bind(&WorkerThread, service));

  RpcClient client(service, "localhost", 8020);

  {
    Latch latch;
    client.Connect(boost::bind(&Latch::Trigger, boost::ref(latch)));

    DLOG(INFO)  << "waiting on latch";
    latch.Wait();
  }

  if (client.err()) {
      LOG(FATAL) << "Error: " << client.err().message();
    return 1;
  }

  GetListingRequestProto req;
  req.set_src("/");
  req.set_startafter("");
  req.set_needlocation(true);

  for (int i = 0; i < 1000; i++) {
    shared_ptr<GetListingResponseProto> resp(new GetListingResponseProto());
    {
      Latch latch;
      client.Call("org.apache.hadoop.hdfs.protocol.ClientProtocol", 1, "getListing", &req, resp,
        boost::bind(&Latch::Trigger, boost::ref(latch)));
      latch.Wait();
    }
  }


  // Stop working.
  work.reset();
  worker.join();
  return 0;
}
