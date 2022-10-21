#include "threadpool.h"

#include <iostream>
#include <grpcpp/grpcpp.h>

#include "store.grpc.pb.h"

using grpc::ServerContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::Status;
using store::Store;
using store::ProductQuery;
using store::ProductReply;
using store::ProductInfo;

// class store { 
// 
// };

class StoreServiceImpl final : public Store::Service {
	Status getProducts(ServerContext* context, const ProductQuery* request, 
						ProductReply* response) override {
		ProductInfo* product_info = response->add_products();
		product_info->set_price(200);
		product_info->set_vendor_id("1");
		return Status::OK;
	}
};

void runServer() {
	std::string server_address("0.0.0.0:50050");
	StoreServiceImpl service;

	ServerBuilder builder;
	// Listen on the given address without any authentication mechanism.
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
	// Register "service" as the instance through which we'll communicate with
	// clients. In this case it corresponds to an *synchronous* service.
	builder.RegisterService(&service);
	// Finally assemble the server.
	std::unique_ptr<Server> server(builder.BuildAndStart());
	std::cout << "Server listening on " << server_address << std::endl;
	
	// Wait for the server to shutdown. Note that some other thread must be
	// responsible for shutting down the server for this call to ever return.
	server->Wait();
}

int main(int argc, char** argv) {
	runServer();
	return EXIT_SUCCESS;
}

