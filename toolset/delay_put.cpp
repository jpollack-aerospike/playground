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
#include <aerospike/as_status.h>
#include <aerospike/as_stringmap.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <openssl/sha.h>
#include <signal.h>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <vector>
#include <unordered_map>

inline void __dieunless (const char *msg, const char *file, int line) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", file, line, msg); abort (); }
// syn: assert.  for normal error checking, not just debugging.
#define dieunless(EX) ((EX) || (__dieunless (#EX, __FILE__, __LINE__),false))

using namespace std;

static const char USAGE[] =
    R"(usage: delayed_put [options]

options:
  -h --help
)";

uint64_t micros ()
{
	using Clock = chrono::steady_clock;
	using Res = chrono::microseconds;
    return chrono::duration_cast<Res>(Clock::now().time_since_epoch()).count();
}

class AerospikeDB
{
public:
    AerospikeDB (const string& hostport);
    ~AerospikeDB ();
    bool put_int64 (const string& ns, const string& set, int64_t ki, int64_t val);
private:
    aerospike m_as;
};

AerospikeDB::AerospikeDB (const string& hostport)
{
    size_t cpos = hostport.find (':');
    dieunless (cpos != string::npos);
    int portno = stoi (hostport.substr (cpos+1));
    string hostname = hostport.substr (0, cpos);

    as_config config;
    as_config_init (&config);
    dieunless (as_config_add_hosts (&config, hostname.c_str (), portno));
    aerospike_init (&m_as, &config);

    as_error err;
    dieunless (aerospike_connect (&m_as, &err) == AEROSPIKE_OK);

}

AerospikeDB::~AerospikeDB ()
{
	as_error err;
    dieunless (aerospike_close (&m_as, &err) == AEROSPIKE_OK);
}

bool AerospikeDB::put_int64 (const string& ns, const string& set, int64_t ki, int64_t val)
{
    as_key key0;
    as_key_init_int64 (&key0, ns.c_str (), set.c_str (), ki);

    as_record rec0;
    as_record_inita (&rec0, 1);
    as_record_set_int64 (&rec0, "num", val);

    as_error err;
	uint64_t t0{micros ()};
    auto rv = aerospike_key_put (&m_as, &err, nullptr, &key0, &rec0);
	uint64_t dur = micros () - t0;
    if (rv != AEROSPIKE_OK) {
		fprintf(stderr, "key:%lu\terr(%d) %s at [%s:%d]\n", ki, err.code, err.message, err.file, err.line);
		dieunless (err.code != AEROSPIKE_ERR_SERVER_FULL);
	}
	
	as_key_destroy (&key0);
	as_record_destroy (&rec0);
    return (rv == AEROSPIKE_OK);
}

uint32_t g_rand_state;

uint32_t lcg_parkmiller ()
{
	uint64_t p = (uint64_t)g_rand_state * 48271;
	uint32_t x = (p & 0x7fffffff) + (p >> 31);
	x = (x & 0x7fffffff) + (x >> 31);
	g_rand_state = x;
	return x;
}

atomic<bool> g_running;
const char g_env_prefix[] = "JP_AS_STT_";
const size_t g_env_prefix_sz = char_traits<char>::length (g_env_prefix);

void sigint_handler (int signum) { g_running = false; }

int main (int argc, char **argv, char **envp)
{

	// Defaults for required variables.
	unordered_map<string,string> p = {
		{ "KEYLB",		"1" },
		{ "KEYUB",		"1000000000" },
		{ "ASDB",		"127.0.0.1:3000" },
		{ "NS",			"ns0" },
		{ "SET",		"s0" },
		{ "DELAY",		"1000000" }
	};
	// Override from environment
	for (auto ep = *envp; ep; ep = *(++envp))
		if (!strncmp (g_env_prefix, ep, g_env_prefix_sz)) {
			auto vs = strchr (ep, '=');
			auto ks = string (ep).substr (g_env_prefix_sz, (vs - ep) - g_env_prefix_sz);
			if (ks.length ()) p[ks] = string (vs + 1);
		}

	int64_t keylb = atoi (p["KEYLB"].c_str ());
	int64_t keyub = atoi (p["KEYUB"].c_str ());
	AerospikeDB db{p["ASDB"]};
	int64_t delay = atoi (p["DELAY"].c_str ());

	g_running = true;
	signal (SIGINT, sigint_handler);
	g_rand_state = 1;

	printf ("Putting records... (press Ctrl-C to stop)\n");
	int64_t ki = keylb;
	while (g_running && (ki < keyub)) {
		dieunless (db.put_int64 (p["NS"], p["SET"], ki, ki));
		usleep (delay);
		ki++;
	}
	
    return 0;
}
