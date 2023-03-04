#include <cassert>
#include <stdio.h>
#include "docopt.h"
#include <map>
#include <string>
#include <iostream>
#include <stddef.h>
#include <stdlib.h>
#include <tuple>
#include <chrono>
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_scan.h>
#include <aerospike/aerospike_scan.h>

#include "common.hpp"

static const char USAGE[] =
    R"(usage: list_records [options]

options:
  -h --help
  --asdb=SOCKADDR     Aerospike Server [default: 127.0.0.1:3000]
  --ns=NAMESPACE      Namespace [default: test]
  --sn=SET            Set [default: ]
)";

using namespace std;
uint64_t usec_now (void) { return chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count(); }

static bool log_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vprintf(fmt, ap);
	printf("\n");
	va_end(ap);
	return true;
}

aerospike as;
string ns;
string sn{""};

static bool callback_entry (const as_val *value, void *udata)
{
    if (!value) return false;
    as_record *rec = as_record_fromval (value);
    assert (rec);
    const char *set = (rec->key.set[0] == 0) ? nullptr : rec->key.set;
    
    as_record_iterator it;
    as_record_iterator_init (&it, rec);
    printf ("%s\t", set);
    while (as_record_iterator_has_next (&it)) {
	as_bin *bin = as_record_iterator_next (&it);
	char *valstr = as_val_tostring (as_bin_get_value (bin));
	printf ("%s:%s ", as_bin_get_name (bin), valstr);
	free (valstr);
    }
    printf ("\n");
    as_record_iterator_destroy (&it);
    return true;
}

int entry (void)
{
    as_scan s0;
    as_scan_init(&s0, ns.c_str (), (sn.size () > 0) ? sn.c_str () : nullptr);

    as_error err;
    if (aerospike_scan_foreach(&as, &err, NULL, &s0, callback_entry, NULL) != AEROSPIKE_OK) {
	fprintf(stderr, "err(%d) %s at [%s:%d]\n", err.code, err.message, err.file, err.line); 
	return -1;
    }

    as_scan_destroy (&s0);
    return 0;
}

int main (int argc, char **argv) 
{ 
    string hostname;
    uint64_t portno{3000};
    {
	auto d{docopt::docopt (USAGE, {argv+1, argv+argc})};
	hostname = d["--asdb"].asString ();
	size_t cpos = hostname.find(':');
	if (cpos != string::npos) {
	    portno = stoi (hostname.substr (cpos+1));
	    hostname = hostname.substr (0, cpos);
	}
	ns = d["--ns"].asString (); 
	sn = d["--sn"].asString ();
	cerr << sn << "\n";
   }

    as_log_set_callback(log_callback);

    as_config config;
    as_config_init (&config);
    assert (as_config_add_hosts (&config, hostname.c_str (), portno));
    aerospike_init (&as, &config);
    as_error err;
    assert (aerospike_connect (&as, &err) == AEROSPIKE_OK);

    auto t0 = usec_now ();
    entry ();
    auto t1 = usec_now ();
    auto tdur = t1 - t0;

    assert (aerospike_close(&as, &err) == AEROSPIKE_OK);
    aerospike_destroy(&as);	

    fprintf (stderr, "%f seconds.\n", tdur / 1000000.0);

    return 0;
}
