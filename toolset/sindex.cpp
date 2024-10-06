#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_query.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_status.h>
#include <aerospike/as_stringmap.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <atomic>
#include <chrono>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <iostream>
#include <map>
#include <map>
#include <memory>
#include <openssl/sha.h>
#include <signal.h>
#include <sstream>
#include <stddef.h>
#include <stdexcept>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <string>
#include <sys/time.h>
#include <tuple>
#include <unistd.h>
#include <unordered_map>
#include <vector>

using namespace std;

const char g_env_prefix[] = "JP_AS_SINDEX_";
const size_t g_env_prefix_sz = char_traits<char>::length (g_env_prefix);
inline void __dieunless (const char *msg, const char *file, int line) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", file, line, msg); abort (); }
#define dieunless(EX) ((EX) || (__dieunless (#EX, __FILE__, __LINE__),false))

uint64_t usec_now (void) { return chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count(); }

int main (int argc, char **argv, char **envp)
{
	dieunless (argc == 2);
	dieunless (!strcmp ("create", argv[1]) || !strcmp ("remove", argv[1]));
	
	unordered_map<string,string> p = {
		{ "ASDB",					"127.0.0.1:3000" },
		{ "NS",						"ns0" },
		{ "SET",					"str" },
		{ "BIN",					"bal" }
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

    as_config config;
    as_config_init (&config);
    dieunless (as_config_add_hosts (&config, hostname.c_str (), portno));

	aerospike as;
    aerospike_init (&as, &config);

    as_error err;
    dieunless (aerospike_connect (&as, &err) == AEROSPIKE_OK);

	string int_name = "idx_int_" + p["NS"] + "_" + p["SET"] + "_" + p["BIN"];
	string str_name = "idx_str_" + p["NS"] + "_" + p["SET"] + "_" + p["BIN"];
	if (!strcmp ("create", argv[1])) {
		as_index_task task;
		dieunless (aerospike_index_create (&as, &err, &task, NULL,
										   p["NS"].c_str (),
										   p["SET"].c_str (),
										   p["BIN"].c_str (),
										   int_name.c_str (),
										   AS_INDEX_NUMERIC) == AEROSPIKE_OK);
		aerospike_index_create_wait (&err, &task, 0);
		// dieunless (aerospike_index_create (&as, &err, &task, NULL,
		// 								   p["NS"].c_str (),
		// 								   p["SET"].c_str (),
		// 								   p["BIN"].c_str (),
		// 								   str_name.c_str (),
		// 								   AS_INDEX_STRING) == AEROSPIKE_OK);
		// aerospike_index_create_wait (&err, &task, 0);
	} else if (!strcmp ("remove", argv[1])) {
		dieunless (aerospike_index_remove (&as, &err, NULL, p["NS"].c_str (), int_name.c_str ()) == AEROSPIKE_OK);
		dieunless (aerospike_index_remove (&as, &err, NULL, p["NS"].c_str (), str_name.c_str ()) == AEROSPIKE_OK);
	}

    dieunless (aerospike_close(&as, &err) == AEROSPIKE_OK);
    aerospike_destroy(&as);	
	return 0;
}
