#include "threadpool.h"

#include <iostream>
#include <cstdlib>
#include <grpcpp/grpcpp.h>

#include "store.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Status;
using store::Store;
using store::ProductQuery;
using store::ProductReply;
using store::ProductInfo;

// class store { 
// 
// };

class StoreServerImpl final {
	public:
		~StoreServerImpl() {
			server_->Shutdown();
			cq_->Shutdown();
		}
	
		void Run(std::string server_address) {
			ServerBuilder builder;
			builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
			builder.RegisterService(&service_);
			cq_ = builder.AddCompletionQueue();
			server_ = builder.BuildAndStart();
			std::cout << "Server listening on " << server_address << std::endl;
			
			HandleRpcs();
		}

	private:
		class CallData {
			public:
				CallData(Store::AsyncService* service, ServerCompletionQueue* cq)
					: service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
					Proceed();
				}

				void Proceed() {
					if (status_ == CREATE) {
						status_ = PROCESS;
						service_->RequestgetProducts(&ctx_, &request_, &responder_, cq_, cq_, this);
					} else if (status_ == PROCESS) {
						new CallData(service_, cq_);

						ProductInfo* product_info = response_.add_products(); 
						product_info->set_price(200);
						product_info->set_vendor_id("1");

						status_ = FINISH;
						responder_.Finish(response_, Status::OK, this);
					} else {
						GPR_ASSERT(status_ = FINISH);
						delete this;
					}
				}

			private:
				Store::AsyncService* service_;
				ServerCompletionQueue* cq_;
				ServerContext ctx_;

				ProductQuery request_;
				ProductReply response_;

				ServerAsyncResponseWriter<ProductReply> responder_;
				enum CallStatus { CREATE, PROCESS, FINISH };
				CallStatus status_;
		};

		void HandleRpcs() {
			new CallData(&service_, cq_.get());
			void *tag;
			bool ok;
			while (true) {
				GPR_ASSERT(cq_->Next(&tag, &ok));
				GPR_ASSERT(ok);
				static_cast<CallData*>(tag)->Proceed();
			}
		}

		std::unique_ptr<ServerCompletionQueue> cq_;
		Store::AsyncService service_;
		std::unique_ptr<Server> server_;
};

int main(int argc, char** argv) {
	StoreServerImpl server;
	std::string vendor_addr_file;
	std::string store_addr;
	int max_threads;
	switch (argc) {
		case 2:
			vendor_addr_file = std::string(argv[1]);
			store_addr = std::string("localhost:50050");
			max_threads = 1;
			break;
		case 3:
			vendor_addr_file = std::string(argv[1]);
			store_addr = std::string(argv[2]);
			max_threads = 1;
			break;
		case 4:
			vendor_addr_file = std::string(argv[1]);
			store_addr = std::string(argv[2]);
			max_threads = atoi(argv[3]);
			break;
		default:
			std::cout << "Invalid number of args" << std::endl;
			std::cout << "./store <filepath for vendor addresses>" << std::endl;
        	std::cout << "        " << "<ip address:port to listen on for clients>" << std::endl;
			std::cout << "        " << "<maximum number of threads in threadpool>" << std::endl;
			return EXIT_FAILURE;
	}

	server.Run(store_addr);

	return EXIT_SUCCESS;
}

