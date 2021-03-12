#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include "masterworker.grpc.pb.h"

#include <thread>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <string>
#include <queue>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <chrono>
#include <unistd.h>

#define MAP 0
#define REDUCE 1
#define CHECK 2

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using masterworker::Fragment;
using masterworker::HeartbeatMsg;
using masterworker::JobRequest;
using masterworker::Masterworker;
using masterworker::Reply;

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master
{

public:
	/* DON'T change the function signature of this constructor */
	Master(const MapReduceSpec &, const std::vector<FileShard> &);

	/* DON'T change this function's signature */
	bool run();

	void Map(const std::string &port, const FileShard &shard)
	{
		// Data we are sending to the server.
		JobRequest request;
		request.set_id(shard.id);
		request.set_type(MAP);
		request.set_num(spec.n_output_files);
		for (int i = 0; i < shard.files.size(); ++i)
		{
			Fragment *fragment = request.add_shard();
			fragment->set_file(shard.files[i]);
			fragment->set_start(shard.starts[i]);
			fragment->set_end(shard.ends[i]);
		}

		// Call object to store rpc data
		AsyncClientCall *call = new AsyncClientCall;

		// stub_->PrepareAsyncSayHello() creates an RPC object, returning
		// an instance to store in "call" but does not actually start the RPC
		// Because we are using the asynchronous API, we need to hold on to
		// the "call" instance in order to get updates on the ongoing RPC.
		call->response_reader =
			stubs_[port]->PrepareAsyncJob(&call->context, request, &cq_);

		// StartCall initiates the RPC call
		call->response_reader->StartCall();

		// Request that, upon completion of the RPC, "reply" be updated with the
		// server's response; "status" with the indication of whether the operation
		// was successful. Tag the request with the memory address of the call object.
		call->response_reader->Finish(&call->reply, &call->status, (void *)call);
	}

	void Reduce(const std::string &port, const std::vector<std::string> &shard, const int &id, const std::string &dir)
	{
		// Data we are sending to the server.
		JobRequest request;
		request.set_id(id);
		request.set_type(REDUCE);
		request.set_dir(dir);
		for (int i = 0; i < shard.size(); ++i)
		{
			Fragment *fragment = request.add_shard();
			fragment->set_file(shard[i]);
		}

		// Call object to store rpc data
		AsyncClientCall *call = new AsyncClientCall;

		// stub_->PrepareAsyncSayHello() creates an RPC object, returning
		// an instance to store in "call" but does not actually start the RPC
		// Because we are using the asynchronous API, we need to hold on to
		// the "call" instance in order to get updates on the ongoing RPC.
		call->response_reader =
			stubs_[port]->PrepareAsyncJob(&call->context, request, &cq_);

		// StartCall initiates the RPC call
		call->response_reader->StartCall();

		// Request that, upon completion of the RPC, "reply" be updated with the
		// server's response; "status" with the indication of whether the operation
		// was successful. Tag the request with the memory address of the call object.
		call->response_reader->Finish(&call->reply, &call->status, (void *)call);
	}

	void Heartbeat(const std::string &port, const int &status)
	{
		HeartbeatMsg request;
		request.set_port(port);
		request.set_status(status);

		AsyncHeartbeat *call = new AsyncHeartbeat;

		call->response_reader =
			stubs_[port]->PrepareAsyncHeartbeat(&call->context, request, &heartbeat_cq_);

		call->response_reader->StartCall();

		call->response_reader->Finish(&call->reply, &call->status, (void *)call);
	}

	void AsyncCompleteRpc()
	{
		void *got_tag;
		bool ok = false;

		// Block until the next result is available in the completion queue "cq".
		while (done == false && cq_.Next(&got_tag, &ok))
		{
			// The tag in this example is the memory location of the call object
			AsyncClientCall *call = static_cast<AsyncClientCall *>(got_tag);

			// Verify that the request was completed successfully. Note that "ok"
			// corresponds solely to the request for updates introduced by Finish().
			GPR_ASSERT(ok);

			if (call->status.ok())
			{
				{
					std::unique_lock<std::mutex> lock(mutex);
					auto worker = call->reply.port();
					if (heartbeat_check[worker] == true)
					{
						heartbeat_status[worker] = 0;
						workers.push(worker);
						auto type = call->reply.type();
						auto files = call->reply.file();
						if (type == MAP)
						{
							for (int i = 0; i < files.size(); ++i)
							{
								intermediate[files.Get(i).start()].push_back(files.Get(i).file());
							}
							count += 1;
							if (call->status.ok())
								std::cout << "Map received: " << call->reply.port() << std::endl;
							else
								std::cout << "RPC failed" << std::endl;
						}
						if (type == REDUCE)
						{
							output.push_back(files.Get(0).file());
							count += 1;
							if (spec.n_output_files == count)
							{
								done = true;
							}
							if (call->status.ok())
								std::cout << "Reduce received: " << call->reply.port() << std::endl;
							else
								std::cout << "RPC failed" << std::endl;
						}
					}
					else
					{
						heartbeat_check[worker] == true;
						heartbeat_status[worker] = 0;
						workers.push(worker);
					}
				}
				// cv.notify_one();
			}

			// if (call->status.ok())
			// 	std::cout << "Greeter received: " << call->reply.port() << std::endl;
			// else
			// 	std::cout << "RPC failed" << std::endl;

			// Once we're complete, deallocate the call object.
			delete call;
		}
	}

	void AsyncCompleteHeartbeat()
	{
		void *got_tag;
		bool ok = false;

		// Block until the next result is available in the completion queue "cq".
		while (heartbeat_cq_.Next(&got_tag, &ok))
		{
			AsyncHeartbeat *call = static_cast<AsyncHeartbeat *>(got_tag);

			GPR_ASSERT(ok);

			if (call->status.ok())
				std::cout << "Heartbeat received: " << call->reply.port() << std::endl;
			else
				std::cout << "RPC failed" << std::endl;

			delete call;
		}
	}

	void CheckAlive()
	{
		while (done == false)
		{
			sleep(4);
			{
				std::unique_lock<std::mutex> lock(mutex);
				if (done == true)
				{
					break;
				}
				std::cout << "Check Alive" << std::endl;
				auto time = std::chrono::system_clock::now();
				for (auto worker : spec.worker_ipaddr_ports)
				{
					if (heartbeat_check[worker] == true && heartbeat_status[worker] > 0)
					{
						std::chrono::duration<double> interval = time - heartbeat_time[worker];
						if (interval.count() >= 5.0)
						{
							heartbeat_check[worker] = false;
							if (heartbeat_status[worker] == 1)
							{
								map_jobs.push(worker_map_jobs[worker]);
							}
							if (heartbeat_status[worker] == 2)
							{
								reduce_jobs.push(worker_reduce_jobs[worker]);
							}
							heartbeat_status[worker] = 0;
						}
					}
				}
			}
			// cv.notify_one();
		}
	}

private:
	/* NOW you can add below, data members and member functions as per the need of your implementation*/
	MapReduceSpec spec;
	std::vector<FileShard> shards;
	std::queue<FileShard> map_jobs;
	std::queue<int> reduce_jobs;
	std::queue<std::string> workers;
	std::unordered_map<int, std::vector<std::string>> intermediate;
	std::vector<std::string> output;
	int count;
	bool done;
	std::mutex mutex;
	std::condition_variable cv;
	std::unordered_map<std::string, int> heartbeat_status;
	std::unordered_map<std::string, bool> heartbeat_check;
	std::unordered_map<std::string, std::chrono::_V2::system_clock::time_point> heartbeat_time;
	std::unordered_map<std::string, FileShard> worker_map_jobs;
	std::unordered_map<std::string, int> worker_reduce_jobs;

	struct AsyncClientCall
	{
		// Container for the data we expect from the server.
		Reply reply;

		// Context for the client. It could be used to convey extra information to
		// the server and/or tweak certain RPC behaviors.
		ClientContext context;

		// Storage for the status of the RPC upon completion.
		Status status;

		std::unique_ptr<ClientAsyncResponseReader<Reply>> response_reader;
	};

	struct AsyncHeartbeat
	{
		HeartbeatMsg reply;
		ClientContext context;
		Status status;
		std::unique_ptr<ClientAsyncResponseReader<HeartbeatMsg>> response_reader;
	};

	// Out of the passed in Channel comes the stub, stored here, our view of the
	// server's exposed services.
	std::unique_ptr<Masterworker::Stub> stub_;
	std::unordered_map<std::string, std::unique_ptr<Masterworker::Stub>> stubs_;

	// The producer-consumer queue we use to communicate asynchronously with the
	// gRPC runtime.
	CompletionQueue cq_;
	CompletionQueue heartbeat_cq_;

	std::thread thread_;
	// std::thread heartbeat_thread_;
	std::thread checkAlive_thread_;
};

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec &mr_spec, const std::vector<FileShard> &file_shards)
{
	spec = mr_spec;
	shards = file_shards;
	for (auto shard : shards)
	{
		map_jobs.push(shard);
	}
	for (int i = 0; i < spec.n_output_files; ++i)
	{
		reduce_jobs.push(i);
	}
	count = 0;
	done = false;
	for (auto port : mr_spec.worker_ipaddr_ports)
	{
		workers.push(port);
		heartbeat_status[port] = 0;
		heartbeat_check[port] = true;
		heartbeat_time[port] = std::chrono::system_clock::now();
		std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(port, grpc::InsecureChannelCredentials());
		stubs_.insert(std::make_pair(port, Masterworker::NewStub(channel)));
	}
	thread_ = std::thread(&Master::AsyncCompleteRpc, this);
	// heartbeat_thread_ = std::thread(&Master::AsyncCompleteHeartbeat, this);
	checkAlive_thread_ = std::thread(&Master::CheckAlive, this);
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run()
{
	if (opendir(spec.output_dir.c_str()) == NULL)
	{
		mkdir(spec.output_dir.c_str(), 0777);
	}
	if (opendir("intermediate") == NULL)
	{
		mkdir("intermediate", 0777);
	}

	while (count < shards.size())
	{
		std::unique_lock<std::mutex> lock(mutex);
		if (count >= shards.size())
		{
			break;
		}
		// while (map_jobs.empty() == true)
		// {
		// 	cv.wait(lock);
		// }
		if (workers.empty() == false && map_jobs.empty() == false)
		{
			std::string port = workers.front();
			workers.pop();
			FileShard shard = map_jobs.front();
			map_jobs.pop();
			heartbeat_status[port] = 1;
			worker_map_jobs[port] = shard;
			heartbeat_time[port] = std::chrono::system_clock::now();
			Map(port, shard);
		}
	}

	{
		std::unique_lock<std::mutex> lock(mutex);
		count = 0;
	}

	while (count < spec.n_output_files)
	{
		std::unique_lock<std::mutex> lock(mutex);
		if (count >= spec.n_output_files)
		{
			break;
		}
		// while (workers.empty() == true || reduce_jobs.empty() == true)
		// {
		// 	cv.wait(lock);
		// }
		if (workers.empty() == false && reduce_jobs.empty() == false)
		{
			std::string port = workers.front();
			workers.pop();
			int group = reduce_jobs.front();
			reduce_jobs.pop();
			heartbeat_status[port] = 2;
			worker_reduce_jobs[port] = group;
			heartbeat_time[port] = std::chrono::system_clock::now();
			Reduce(port, intermediate[group], group, spec.output_dir);
		}
	}

	thread_.join();
	checkAlive_thread_.join();

	system("rm -r intermediate");

	return true;
}