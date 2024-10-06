#include <atomic>
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

const char g_env_prefix[] = "JP_AS_DUMP_";

inline void __dieunless (const char *msg, const char *file, int line) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", file, line, msg); abort (); }
#define dieunless(EX) ((EX) || (__dieunless (#EX, __FILE__, __LINE__),false))

using namespace std;
uint64_t usec_now (void) { return chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count(); }

aerospike as;
string ns;
string sn{""};

static bool callback_entry (const as_val *value, void *udata)
{
    if (!value) return false;
    as_record *rec = as_record_fromval (value);
    dieunless (rec);
    const char *set = (rec->key.set[0] == 0) ? nullptr : rec->key.set;
    
    as_record_iterator it;
    as_record_iterator_init (&it, rec);
    printf ("%s\t", set);
    while (as_record_iterator_has_next (&it)) {
		as_bin *bin = as_record_iterator_next (&it);
		char *valstr = as_val_tostring (as_bin_get_value (bin));
		printf ("\n\t%s:%s", as_bin_get_name (bin), valstr);
		free (valstr);
    }
    printf ("\n");
    as_record_iterator_destroy (&it);
    return true;
}

static bool callback2_entry (const as_val *value, void *udata)
{
    if (!value) return false;
    as_record *rec = as_record_fromval (value);
    dieunless (rec);
	dieunless (rec->key.ns[0]);

	size_t nbins = rec->bins.size;
	rec->key.digest.value;
	rec->ttl;
	rec->key.valuep; // optional
	for (auto ii=0; ii < rec->bins.size; ++ii) {
		as_bin *bin = rec->bins.entries + ii;
		bin->name;
		(as_val *)bin->valuep;
	}
	
    const char *set = (rec->key.set[0] == 0) ? nullptr : rec->key.set;
    
    as_record_iterator it;
    as_record_iterator_init (&it, rec);
    printf ("%s\t", set);
    while (as_record_iterator_has_next (&it)) {
		as_bin *bin = as_record_iterator_next (&it);
		char *valstr = as_val_tostring (as_bin_get_value (bin));
		printf ("\n\t%s:%s", as_bin_get_name (bin), valstr);
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
	s0.deserialize_list_map = false;
	
    as_error err;
    if (aerospike_scan_foreach(&as, &err, NULL, &s0, callback2_entry, NULL) != AEROSPIKE_OK) {
		fprintf(stderr, "err(%d) %s at [%s:%d]\n", err.code, err.message, err.file, err.line); 
		return -1;
    }

    as_scan_destroy (&s0);
    return 0;
}

atomic<bool> g_running;
const size_t g_env_prefix_sz = char_traits<char>::length (g_env_prefix);
void sigint_handler (int signum) { g_running = false; }

int main (int argc, char **argv, char **envp) 
{ 
    // a sort of "global params"
	unordered_map<string,string> p = {
		{ "ASDB",					"127.0.0.1:3000" },
		{ "NS",						"ns0" },
		{ "SET",					"s0" }
	};

	for (auto ep = *envp; ep; ep = *(++envp))
		if (!strncmp (g_env_prefix, ep, g_env_prefix_sz)) {
			auto vs = strchr (ep, '=');
			auto ks = string (ep).substr (g_env_prefix_sz, (vs - ep) - g_env_prefix_sz);
			if (ks.length ()) p[ks] = string (vs + 1);
		}

    string hostname = p["ASDB"];
    uint64_t portno{3000};
	size_t cpos = hostname.find(':');
	if (cpos != string::npos) {
		portno = stoi (hostname.substr (cpos+1));
		hostname = hostname.substr (0, cpos);
	}

	ns = p["NS"];
	sn = p["SET"];
	sn = "";

    as_config config;
    as_config_init (&config);
    dieunless (as_config_add_hosts (&config, hostname.c_str (), portno));
    aerospike_init (&as, &config);
    as_error err;
    dieunless (aerospike_connect (&as, &err) == AEROSPIKE_OK);

    auto t0 = usec_now ();
    entry ();
    auto t1 = usec_now ();
    auto tdur = t1 - t0;

    dieunless (aerospike_close(&as, &err) == AEROSPIKE_OK);
    aerospike_destroy(&as);	

    fprintf (stderr, "%f seconds.\n", tdur / 1000000.0);

    return 0;
}
