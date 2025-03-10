#include <iostream>
#include <csignal>
#include <librdkafka/rdkafka.h>

#include "common.cpp"

using namespace std;

static volatile sig_atomic_t run = 1;

static void stop(int sig) {
    run = 0;
}

void process_message(rd_kafka_message_t *consumer_message) {
    cout << (char *)consumer_message->payload << endl;
}

void start_kafka_consumer() {
    rd_kafka_t *consumer;
    rd_kafka_conf_t *conf;
    rd_kafka_resp_err_t err;
    char errstr[512];

    // Create client configuration
    conf = rd_kafka_conf_new();

    // User-specific properties that you must set
    set_config(conf, "bootstrap.servers", "localhost:9092");

    // Fixed properties
    set_config(conf, "group.id", "extract-system-tags-group");
    set_config(conf, "auto.offset.reset", "earliest");

    // Create the Consumer instance
    consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!consumer) {
        cerr << "Failed to create new consumer: " << errstr << endl;
        return;
    }
    rd_kafka_poll_set_consumer(consumer);

    // Configuration object is now owned and freed by the rd_kafka_t instance
    conf = nullptr;

    // Convert the list of topics to a format suitable for librdkafka
    const char *topic = "extract-system-tags";
    rd_kafka_topic_partition_list_t *subscription = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(subscription, topic, RD_KAFKA_PARTITION_UA);

    // Subscribe to the list of topics
    err = rd_kafka_subscribe(consumer, subscription);
    if (err) {
        cerr << "Failed to subscribe to " << subscription->cnt << " topics: " << rd_kafka_err2str(err) << endl;
        rd_kafka_topic_partition_list_destroy(subscription);
        rd_kafka_destroy(consumer);
        return;
    }

    rd_kafka_topic_partition_list_destroy(subscription);

    // Install a signal handler for clean shutdown
    signal(SIGINT, stop);

    // Start polling for messages
    while (run) {
        rd_kafka_message_t *consumer_message = rd_kafka_consumer_poll(consumer, 500);
        if (!consumer_message) {
            continue;
        }

        if (consumer_message->err) {
            if (consumer_message->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                // Ignoring end-of-partition message
            } else {
                cerr << "Consumer error: " << rd_kafka_message_errstr(consumer_message) << endl;
                return;
            }
        } else {
            process_message(consumer_message);
        }

        // Free the message
        rd_kafka_message_destroy(consumer_message);
    }

    // Close the consumer
    cout << "Closing consumer" << endl;
    rd_kafka_consumer_close(consumer);

    // Destroy the consumer
    rd_kafka_destroy(consumer);
}
