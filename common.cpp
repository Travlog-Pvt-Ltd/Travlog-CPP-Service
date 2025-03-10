#include <iostream>
#include <cstdlib>
#include <librdkafka/rdkafka.h>

using namespace std;

/* Wrapper to set config values and error out if needed. */
static void set_config(rd_kafka_conf_t *conf, const char *key, const char *value) {
    char errstr[512];
    rd_kafka_conf_res_t res;

    res = rd_kafka_conf_set(conf, key, value, errstr, sizeof(errstr));
    if (res != RD_KAFKA_CONF_OK) {
        cerr << "Unable to set config: " << errstr << endl;
        exit(1);
    }
}
