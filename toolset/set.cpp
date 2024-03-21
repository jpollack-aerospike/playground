#include <aerospike/aerospike.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_query.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_stringmap.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <concurrentqueue.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <docopt.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <nlohmann/json.hpp>
#include <openssl/sha.h>
#include <signal.h>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <stdio.h>
#include <map>
#include <string>
#include <iostream>
#include <stddef.h>
#include <stdlib.h>
#include <tuple>
#include <chrono>

using namespace std;
using json = nlohmann::json;

const char g_env_prefix[] = "JP_AS_SET_";
const size_t g_env_prefix_sz = char_traits<char>::length (g_env_prefix);
inline void __dieunless (const char *msg, const char *file, int line) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", file, line, msg); abort (); }
#define dieunless(EX) ((EX) || (__dieunless (#EX, __FILE__, __LINE__),false))

uint64_t usec_now (void) { return chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count(); }

as_val *as_val_new_from_json (const json& jel)
{
    switch (jel.type ())
    {
    case json::value_t::object:
    {
		as_orderedmap *mp = as_orderedmap_new (jel.size ());
		for (const auto& [jkey, jval] : jel.items ())
			as_orderedmap_set (mp, as_val_new_from_json (jkey), as_val_new_from_json (jval));

		return (as_val *)mp;
    }
    case json::value_t::array:
    {
		as_arraylist *lp = as_arraylist_new (jel.size (), 0);
		for (const auto& el : jel)
			as_arraylist_append (lp, as_val_new_from_json (el));

		return (as_val *)lp;
    }
    case json::value_t::string:				return (as_val *)as_string_new (strdup (jel.get<string>().c_str ()), true);
    case json::value_t::number_integer:			return (as_val *)as_integer_new (jel.get<int64_t>());
    case json::value_t::number_unsigned:		return (as_val *)as_integer_new (jel.get<uint64_t>());
    case json::value_t::number_float:			return (as_val *)as_double_new (jel.get<double>());
    case json::value_t::boolean:			return (as_val *)as_boolean_new (jel.get<bool>());
    case json::value_t::null:				return (as_val *)&as_nil;
    case json::value_t::binary:				return (as_val *)as_bytes_new_wrap ((uint8_t *)jel.get_binary ().data (),jel.get_binary ().size (), false);
    }

    dieunless (false);
    return nullptr;
}

void as_record_init_from_json (as_record *rec, const json& jel)
{
	dieunless (jel.type () == json::value_t::object);

	for (const auto& [jkey, jval] : jel.items ())
	{
		const char* bname = jkey.c_str ();
		switch (jval.type ()) {
		case json::value_t::object:
			dieunless (as_record_set_map (rec, bname, (as_map *) as_val_new_from_json (jval)));
			break;
		case json::value_t::array:
			dieunless (as_record_set_list (rec, bname, (as_list *) as_val_new_from_json (jval)));
			break;
		case json::value_t::string:
			dieunless (as_record_set_strp (rec, bname, strdup (jval.get<string>().c_str ()), true));
			break;
		case json::value_t::number_integer:
			dieunless (as_record_set_int64 (rec, bname, jval.get<int64_t>()));
			break;
		case json::value_t::number_unsigned:
			dieunless (as_record_set_double (rec, bname, jval.get<uint64_t>()));
			break;
		case json::value_t::number_float:
			dieunless (as_record_set_double (rec, bname, jval.get<uint64_t>()));
			break;
		case json::value_t::boolean:
			dieunless (as_record_set_bool (rec, bname, jval.get<bool>()));
			break;
		case json::value_t::null:
			dieunless (as_record_set_nil (rec, bname));
			break;
		case json::value_t::binary:
			dieunless (as_record_set_bytes (rec, bname, as_bytes_new_wrap ((uint8_t *)jval.get_binary ().data (),jval.get_binary ().size (), false)));
			break;
		}
	}
}


int main (int argc, char **argv, char **envp)
{
	dieunless (argc == 2);
	int64_t ki = atoi (argv[1]);
	
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

    as_config config;
    as_config_init (&config);
    dieunless (as_config_add_hosts (&config, hostname.c_str (), portno));

	aerospike as;
    aerospike_init (&as, &config);

    as_error err;
    dieunless (aerospike_connect (&as, &err) == AEROSPIKE_OK);

	as_key key0;
    as_key_init_int64 (&key0, p["NS"].c_str (), p["SET"].c_str (), ki);
	
	string line;
	getline (std::cin, line);
	json jel = json::parse (line);
	
	as_record rec0;
	as_record_inita (&rec0, jel.size ());
	as_record_init_from_json (&rec0, jel);

	as_policy_write wpol;
    as_policy_write_init (&wpol);

    switch (aerospike_key_put (&as, &err, &wpol, &key0, &rec0)) {
    case AEROSPIKE_ERR_RECORD_TOO_BIG:
		fprintf (stderr, "record %lu too big, not inserted.  original string length: %lu\n", ki, line.length ());
    case AEROSPIKE_OK:
		// as_record_destroy?
		break;
    default:
		fprintf(stderr, "key:%lu\terr(%d) %s at [%s:%d]\n", ki, err.code, err.message, err.file, err.line);
    }

    dieunless (aerospike_close(&as, &err) == AEROSPIKE_OK);
    aerospike_destroy(&as);	
	return 0;
}
