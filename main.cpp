#include <httplib.h>
#include <iostream>
#include <thread>
#include <chrono>
#include "consumer.cpp"

int main() {
    // Start Kafka Consumer in a separate thread
    std::thread consumer_thread(start_kafka_consumer);

    // Start HTTP Server
    httplib::Server svr;

    svr.Get("/", [](const httplib::Request &, httplib::Response &res) {
        res.set_content("Hello, World!", "text/plain");
    });

    std::cout << "Server is running on http://localhost:8081\n";
    svr.listen("0.0.0.0", 8081);

    // Wait for consumer thread to finish
    consumer_thread.join();

    return 0;
}
