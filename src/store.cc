#include "threadpool.h"

#include <iostream>
#include <fstream>
#include <vector>
#include <cstdlib>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>

#include "store.grpc.pb.h"
#include "vendor.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using store::Store;
using store::ProductQuery;
using store::ProductReply;
using store::ProductInfo;
using vendor::Vendor;
using vendor::BidQuery;
using vendor::BidReply;

class VendorClient {
	public:
		explicit VendorClient(std::shared_ptr<Channel> channel) 
			: stub_(Vendor::NewStub(channel)) {}
		
		BidReply getProductBid(const std::string& product_name) {
			BidQuery request;
			request.set_product_name(product_name);

			BidReply response;
			ClientContext context;
			CompletionQueue cq;
			Status status;

			std::unique_ptr<ClientAsyncResponseReader<BidReply>> rpc(
				stub_->AsyncgetProductBid(&context, request, &cq));
			rpc->Finish(&response, &status, (void*)1);
			void* got_tag;
			bool ok = false;
			GPR_ASSERT(cq.Next(&got_tag, &ok));
			GPR_ASSERT(got_tag == (void*)1);
			GPR_ASSERT(ok);
			return response;
		}

	private:
		std::unique_ptr<Vendor::Stub> stub_;
};

class StoreServerImpl final {
	public:
		~StoreServerImpl() {
			server_->Shutdown();
			cq_->Shutdown();
		}
	
		void Run(std::string server_address, std::vector<VendorClient*> vendors) {
			ServerBuilder builder;
			builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
			builder.RegisterService(&service_);
			cq_ = builder.AddCompletionQueue();
			server_ = builder.BuildAndStart();
			vendors_ = vendors;
			std::cout << "Server listening on " << server_address << std::endl;
			
			HandleRpcs();
		}

	private:
		class CallData {
			public:
				CallData(Store::AsyncService* service, ServerCompletionQueue* cq, std::vector<VendorClient*> vendors)
					: service_(service), cq_(cq), vendors_(vendors), responder_(&ctx_), status_(CREATE) {
					Proceed();
				}

				void Proceed() {
					if (status_ == CREATE) {
						status_ = PROCESS;
						service_->RequestgetProducts(&ctx_, &request_, &responder_, cq_, cq_, this);
					} else if (status_ == PROCESS) {
						new CallData(service_, cq_, vendors_);

						std::string product_name(request_.product_name());
						for (int i = 0; i < vendors_.size(); i++) {
							BidReply response = vendors_.at(i)->getProductBid(product_name);

							ProductInfo* product_info = response_.add_products(); 
							product_info->set_price(response.price());
							product_info->set_vendor_id(response.vendor_id());
						}
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
				std::vector<VendorClient*> vendors_;

				ProductQuery request_;
				ProductReply response_;

				ServerAsyncResponseWriter<ProductReply> responder_;
				enum CallStatus { CREATE, PROCESS, FINISH };
				CallStatus status_;
		};

		void HandleRpcs() {
			new CallData(&service_, cq_.get(), vendors_);
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
		std::vector<VendorClient*> vendors_;
};

std::vector<VendorClient*> get_vendor_addresses(std::string filename) {
	std::vector<VendorClient*> vendors;
	std::ifstream file(filename);
	if (file.is_open()) {
		std::string line;
		while (std::getline(file, line)) {
			vendors.push_back(new VendorClient(grpc::CreateChannel(
				line, grpc::InsecureChannelCredentials())));
		}
		file.close();
	}
	return vendors;
}

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

	std::vector<VendorClient*> vendors = get_vendor_addresses(vendor_addr_file);

	server.Run(store_addr, vendors);
	return EXIT_SUCCESS;
}
