#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"

#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>
#include "masterworker.grpc.pb.h"

#include <fstream>
#include <sstream>
#include <unordered_map>
#include <string>
#include <functional>
#include <thread>

#define MAP 0
#define REDUCE 1

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using masterworker::Fragment;
using masterworker::HeartbeatMsg;
using masterworker::JobRequest;
using masterworker::Masterworker;
using masterworker::Reply;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string &user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string &user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker
{

public:
	/* DON'T change the function signature of this constructor */
	Worker(std::string ip_addr_port);

	/* DON'T change this function's signature */
	bool run();

	~Worker()
	{
		server_->Shutdown();
		cq_->Shutdown();
	}

	std::string getPort()
	{
		return port;
	}

private:
	/* NOW you can add below, data members and member functions as per the need of your implementation*/
	class CallData
	{
	public:
		// Take in the "service" instance (in this case representing an asynchronous
		// server) and the completion queue "cq" used for asynchronous communication
		// with the gRPC runtime.
		CallData(Masterworker::AsyncService *service, ServerCompletionQueue *cq, std::string port)
			: service_(service), cq_(cq), responder_(&ctx_), status_(CREATE), port_(port)
		{
			// Invoke the serving logic right away.
			Proceed();
		}

		void Proceed()
		{
			if (status_ == CREATE)
			{
				// Make this instance progress to the PROCESS state.
				status_ = PROCESS;

				// As part of the initial CREATE state, we *request* that the system
				// start processing SayHello requests. In this request, "this" acts are
				// the tag uniquely identifying the request (so that different CallData
				// instances can serve different requests concurrently), in this case
				// the memory address of this CallData instance.
				service_->RequestJob(&ctx_, &request_, &responder_, cq_, cq_,
									 this);
			}
			else if (status_ == PROCESS)
			{
				// Spawn a new CallData instance to serve new clients while we process
				// the one for this CallData. The instance will deallocate itself as
				// part of its FINISH state.
				new CallData(service_, cq_, port_);

				// The actual processing.
				auto id = request_.id();
				auto type = request_.type();
				if (type == MAP)
				{
					auto mapper = get_mapper_from_task_factory("cs6210");
					auto shard = request_.shard();
					auto num = request_.num();
					for (int i = 0; i < shard.size(); ++i)
					{
						auto fragment = shard.Get(i);
						auto in_filename = fragment.file();
						auto start = fragment.start();
						auto end = fragment.end();
						std::ifstream in_file(in_filename);
						in_file.seekg(start);
						std::string line;
						while (std::getline(in_file, line))
						{
							mapper->map(line);
							if (in_file.tellg() == end)
							{
								break;
							}
						}
					}

					std::unordered_map<int, std::vector<std::pair<std::string, std::string>>> remap;
					std::hash<std::string> hasher;
					for (auto keyVal : mapper->impl_->key_val)
					{
						remap[hasher(keyVal.first) % num].push_back(keyVal);
					}
					for (auto group : remap)
					{
						std::string out_filename = "intermediate/map_" + std::to_string(id) + "_" + std::to_string(group.first);
						std::ofstream out_file(out_filename);
						for (auto keyVal : group.second)
						{
							out_file << keyVal.first << ',' << keyVal.second << '\n';
						}
						out_file.close();
						Fragment *file = reply_.add_file();
						file->set_file(out_filename);
						file->set_start(group.first);
					}
				}

				if (type == REDUCE)
				{
					auto reducer = get_reducer_from_task_factory("cs6210");
					auto shard = request_.shard();
					auto dir = request_.dir();
					std::unordered_map<std::string, std::vector<std::string>> key_vals;
					for (int i = 0; i < shard.size(); ++i)
					{
						auto fragment = shard.Get(i);
						auto in_filename = fragment.file();
						std::ifstream in_file(in_filename);
						std::string line;
						while (std::getline(in_file, line))
						{
							std::stringstream key_val(line);
							std::string key;
							std::string val;
							std::getline(key_val, key, ',');
							std::getline(key_val, val, ',');
							key_vals[key].push_back(val);
						}
					}
					for (auto keyVal : key_vals)
					{
						reducer->reduce(keyVal.first, keyVal.second);
					}

					std::string out_filename = dir + "/reduce_" + std::to_string(id);
					std::ofstream out_file(out_filename);
					std::sort(reducer->impl_->key_val.begin(), reducer->impl_->key_val.end());
					for (auto keyVal : reducer->impl_->key_val)
					{
						out_file << keyVal.first << ' ' << keyVal.second << '\n';
					}
					out_file.close();
					Fragment *file = reply_.add_file();
					file->set_file(out_filename);
				}

				reply_.set_port(port_);
				reply_.set_type(type);

				// And we are done! Let the gRPC runtime know we've finished, using the
				// memory address of this instance as the uniquely identifying tag for
				// the event.
				status_ = FINISH;
				responder_.Finish(reply_, Status::OK, this);
			}
			else
			{
				GPR_ASSERT(status_ == FINISH);
				// Once in the FINISH state, deallocate ourselves (CallData).
				delete this;
			}
		}

	private:
		// The means of communication with the gRPC runtime for an asynchronous
		// server.
		Masterworker::AsyncService *service_;
		// The producer-consumer queue where for asynchronous server notifications.
		ServerCompletionQueue *cq_;
		// Context for the rpc, allowing to tweak aspects of it such as the use
		// of compression, authentication, as well as to send metadata back to the
		// client.
		ServerContext ctx_;

		// What we get from the client.
		JobRequest request_;
		// What we send back to the client.
		Reply reply_;

		// The means to get back to the client.
		ServerAsyncResponseWriter<Reply> responder_;

		// Let's implement a tiny state machine with the following states.
		enum CallStatus
		{
			CREATE,
			PROCESS,
			FINISH
		};
		CallStatus status_; // The current serving state.

		std::string port_;
	};

	// This can be run in multiple threads if needed.
	void HandleRpcs()
	{
		// Spawn a new CallData instance to serve new clients.
		new CallData(&service_, cq_.get(), port);
		void *tag; // uniquely identifies a request.
		bool ok;
		while (true)
		{
			// Block waiting to read the next event from the completion queue. The
			// event is uniquely identified by its tag, which in this case is the
			// memory address of a CallData instance.
			// The return value of Next should always be checked. This return value
			// tells us whether there is any kind of event or cq_ is shutting down.
			GPR_ASSERT(cq_->Next(&tag, &ok));
			GPR_ASSERT(ok);
			static_cast<CallData *>(tag)->Proceed();
		}
	}

	class Heartbeat
	{
	public:
		Heartbeat(Masterworker::AsyncService *service, ServerCompletionQueue *cq, std::string port)
			: service_(service), cq_(cq), responder_(&ctx_), status_(CREATE), port_(port)
		{
			Proceed();
		}

		void Proceed()
		{
			if (status_ == CREATE)
			{
				status_ = PROCESS;
				service_->RequestHeartbeat(&ctx_, &request_, &responder_, cq_, cq_, this);
			}
			else if (status_ == PROCESS)
			{
				new Heartbeat(service_, cq_, port_);

				auto status = request_.status();

				reply_.set_port(port_);
				reply_.set_status(status);

				status_ = FINISH;
				responder_.Finish(reply_, Status::OK, this);
			}
			else
			{
				GPR_ASSERT(status_ == FINISH);
				delete this;
			}
		}

	private:
		Masterworker::AsyncService *service_;
		ServerCompletionQueue *cq_;
		ServerContext ctx_;
		HeartbeatMsg request_;
		HeartbeatMsg reply_;
		ServerAsyncResponseWriter<HeartbeatMsg> responder_;
		enum CallStatus
		{
			CREATE,
			PROCESS,
			FINISH
		};
		CallStatus status_;
		std::string port_;
	};

	void HandleHeartbeat()
	{
		new Heartbeat(&service_, heartbeat_cq_.get(), port);
		void *tag;
		bool ok;
		while (true)
		{
			GPR_ASSERT(heartbeat_cq_->Next(&tag, &ok));
			GPR_ASSERT(ok);
			static_cast<Heartbeat *>(tag)->Proceed();
		}
	}

	std::unique_ptr<ServerCompletionQueue> cq_;
	std::unique_ptr<ServerCompletionQueue> heartbeat_cq_;
	Masterworker::AsyncService service_;
	std::unique_ptr<Server> server_;

	std::string port;
};

/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port)
{
	port = ip_addr_port;
}

// extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string &user_id);
// extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string &user_id);

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run()
{
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */
	// std::cout << "worker.run(), I 'm not ready yet" << std::endl;
	// auto mapper = get_mapper_from_task_factory("cs6210");
	// mapper->map("I m just a 'dummy', a \"dummy line\"");
	// auto reducer = get_reducer_from_task_factory("cs6210");
	// reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
	// return true;

	ServerBuilder builder;
	// Listen on the given address without any authentication mechanism.
	builder.AddListeningPort(port, grpc::InsecureServerCredentials());
	// Register "service_" as the instance through which we'll communicate with
	// clients. In this case it corresponds to an *asynchronous* service.
	builder.RegisterService(&service_);
	// Get hold of the completion queue used for the asynchronous communication
	// with the gRPC runtime.
	cq_ = builder.AddCompletionQueue();
	heartbeat_cq_ = builder.AddCompletionQueue();
	// Finally assemble the server.
	server_ = builder.BuildAndStart();
	std::cout << "Server listening on " << port << std::endl;

	std::thread heartbeat = std::thread(&Worker::HandleHeartbeat, this);

	// Proceed to the server's main loop.
	HandleRpcs();

	return true;
}