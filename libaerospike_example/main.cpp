#include <stdio.h>
#include <map>
#include <string>
#include <iostream>
#include <stddef.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

using namespace std;

static bool log_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vprintf(fmt, ap);
	printf("\n");
	va_end(ap);
	return true;
}
struct Options 
{
    uint16_t portno{3000};
    string hostname;
};

int main (int argc, char **argv)
{

    Options o;
    o.hostname = "127.0.0.1";
    o.portno = 3000;
    
    as_log_set_callback(log_callback);

    as_config config;
    as_config_init (&config);
    // Note: doesn't always DTRT with hostnames.
    if (!as_config_add_hosts (&config, o.hostname.c_str (), o.portno)) {
	fprintf(stderr, "Invalid host(s) %s\n", o.hostname.c_str ());
	return -1;
    }

    aerospike as;
    aerospike_init (&as, &config);
    as_error err;
   
    if (aerospike_connect (&as, &err) != AEROSPIKE_OK) {
	fprintf(stderr, "err(%d) %s at [%s:%d]\n", err.code, err.message, err.file, err.line); 
	return -1;
    }
    
    // as_record rec;
    as_key key0;
    as_key_init_str (&key0, "test", "set", "alpha");


    if (aerospike_close(&as, &err) != AEROSPIKE_OK) {
	fprintf(stderr, "err(%d) %s at [%s:%d]\n", err.code, err.message, err.file, err.line);
    }

    aerospike_destroy(&as);	
    return 0;
}



