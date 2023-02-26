#include <stdio.h>
#include "docopt.h"
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


static const char USAGE[] =
    R"(usage: mfac [options]

options:
  -h --help
  --asdb=SOCKADDR     Aerospike Server [default: localhost:3000]
)";

using namespace std;

struct Options 
{
    uint16_t portno{3000};
    string hostname;
};

static bool log_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vprintf(fmt, ap);
	printf("\n");
	va_end(ap);
	return true;
}

int entry (const Options& o)
{
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
    as_key_init_str (&key0, "test", "set", "alpha")


    if (aerospike_close(&as, &err) != AEROSPIKE_OK) {
	fprintf(stderr, "err(%d) %s at [%s:%d]\n", err.code, err.message, err.file, err.line);
    }

    aerospike_destroy(&as);	
    return 0;
}

void parseArguments (int argc, char **argv, Options& o)
{
    auto dargs {docopt::docopt (USAGE, {argv+1, argv+argc})};

    o.hostname = dargs["--asdb"].asString();
    size_t cpos = o.hostname.find(':');
    if (cpos != string::npos) {
	o.portno = stoi (o.hostname.substr (cpos+1));
	o.hostname = o.hostname.substr (0, cpos);
    }
}

int main (int argc, char **argv)
{
    Options options;
    parseArguments (argc, argv, options);
    return entry (options);
}
