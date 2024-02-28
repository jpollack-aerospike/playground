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

const char g_env_prefix[] = "JP_AS_LOAD_";

inline void __dieunless (const char *msg, const char *file, int line) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", file, line, msg); abort (); }

#define dieunless(EX) ((EX) || (__dieunless (#EX, __FILE__, __LINE__),false))

using namespace std;
using json = nlohmann::json;

as_val * as_val_new_from_json (const json& jel)
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

class AerospikeDB
{
public:
    AerospikeDB (const string& hostport, const string& ns, const string& set, int nthreads = 0);
    ~AerospikeDB ();
    bool putRecord (int64_t ki, const string& jsonstr);
    void queuePut (int64_t ki, const string& str) { m_q.enqueue (make_pair (ki, str)); }
    int queueSize (void) { return m_q.size_approx (); }
private:
    string m_ns;
	string m_set;
    aerospike m_as;
    atomic<bool> m_running;
    vector<thread> m_th;
    moodycamel::ConcurrentQueue<pair<int64_t,string>> m_q;
    void putWorker (void);
};

AerospikeDB::AerospikeDB (const string& hostport, const string& ns, const string& set, int nthreads)
    : m_ns (ns),
	  m_set (set),	  
      m_running (true)
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
    if (nthreads == 0)
		nthreads = thread::hardware_concurrency (); // we're CPU limited on the parse side.

    for (int i = 0; i < nthreads; i++)
		m_th.emplace_back (&AerospikeDB::putWorker, this);
}

void AerospikeDB::putWorker (void)
{
    pair<int64_t,string> od;
    while (1) {
		bool empty; // false on full path
		while ((empty = !m_q.try_dequeue (od)) && m_running.load ())
			this_thread::yield ();
		if (empty) return; // return *only* if the queue is empty
		do {
			dieunless (this->putRecord (od.first, od.second));
		} while (m_q.try_dequeue (od)); // true on full path
		// queue now empty, yield to producers
		this_thread::yield ();
    }
}

AerospikeDB::~AerospikeDB ()
{
    m_running.store (false);
    this_thread::yield ();

    for (auto& th : m_th) th.join ();
    pair<int64_t,string> od;
    dieunless (!m_q.try_dequeue (od));
    as_error err;
    dieunless (aerospike_close (&m_as, &err) == AEROSPIKE_OK);
}

bool AerospikeDB::putRecord (int64_t ki, const string& jsonstr)
{
    const json j = json::parse (jsonstr);
	dieunless (j.type () == json::value_t::object);

    as_key key0;
    as_key_init_int64 (&key0, m_ns.c_str (), m_set.c_str (), ki);

    as_record rec0;
    as_record_inita (&rec0, j.size ());

	for (const auto& [jkey, jval] : j.items ())
	{
		const char* bname = jkey.c_str ();
		switch (jval.type ()) {
		case json::value_t::object:
			dieunless (as_record_set_map (&rec0, bname, (as_map *) as_val_new_from_json (jval)));
			break;
		case json::value_t::array:
			dieunless (as_record_set_list (&rec0, bname, (as_list *) as_val_new_from_json (jval)));
			break;
		case json::value_t::string:
			dieunless (as_record_set_strp (&rec0, bname, strdup (jval.get<string>().c_str ()), true));
			break;
		case json::value_t::number_integer:
			dieunless (as_record_set_int64 (&rec0, bname, jval.get<int64_t>()));
			break;
		case json::value_t::number_unsigned:
			dieunless (as_record_set_double (&rec0, bname, jval.get<uint64_t>()));
			break;
		case json::value_t::number_float:
			dieunless (as_record_set_double (&rec0, bname, jval.get<uint64_t>()));
			break;
		case json::value_t::boolean:
			dieunless (as_record_set_bool (&rec0, bname, jval.get<bool>()));
			break;
		case json::value_t::null:
			dieunless (as_record_set_nil (&rec0, bname));
			break;
		case json::value_t::binary:
			dieunless (as_record_set_bytes (&rec0, bname, as_bytes_new_wrap ((uint8_t *)jval.get_binary ().data (),jval.get_binary ().size (), false)));
			break;
		}
	}

	as_policy_write wpol;
    as_policy_write_init (&wpol);
    if (false) wpol.key = AS_POLICY_KEY_SEND;

    as_error err;
    switch (aerospike_key_put (&m_as, &err, &wpol, &key0, &rec0)) {
    case AEROSPIKE_ERR_RECORD_TOO_BIG:
		fprintf (stderr, "record %lu too big, not inserted.  original string length: %lu\n", ki, jsonstr.length ());
    case AEROSPIKE_OK:
		// as_record_destroy?
		return true;
    default:
		fprintf(stderr, "key:%lu\terr(%d) %s at [%s:%d]\n", ki, err.code, err.message, err.file, err.line);
    }
    return false;

}

static uint64_t usec_now (void) { return chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count(); }

atomic<bool> g_running;
const size_t g_env_prefix_sz = char_traits<char>::length (g_env_prefix);
void sigint_handler (int signum) { g_running = false; }

int main (int argc, char **argv, char **envp)
{
    // a sort of "global params"
	unordered_map<string,string> p = {
		{ "ASDB",					"127.0.0.1:3000" },
		{ "MAXDEPTH",				"256" },
		{ "NS",						"ns0" },
		{ "SET",					"s0" },
		{ "STARTAT",				"1" },
		{ "THREADS",				"0" }
	};

	for (auto ep = *envp; ep; ep = *(++envp))
		if (!strncmp (g_env_prefix, ep, g_env_prefix_sz)) {
			auto vs = strchr (ep, '=');
			auto ks = string (ep).substr (g_env_prefix_sz, (vs - ep) - g_env_prefix_sz);
			if (ks.length ()) p[ks] = string (vs + 1);
		}

	int maxDepth = atoi (p["MAXDEPTH"].c_str ());
	int64_t startAt = atoi (p["STARTAT"].c_str ());
	
    uint64_t btot{0}, lineno{0}, tstart, tlast;
    tstart = tlast = usec_now ();
    {
		AerospikeDB db{p["ASDB"], p["NS"], p["SET"], atoi (p["THREADS"].c_str ())};
		string line;
		printf ("{\"time\":%ld}\n", tstart);
		while (getline (std::cin, line)) {
			btot += line.size ();

			while (db.queueSize () > maxDepth)
				this_thread::yield (); // yields to consumers

			db.queuePut (++lineno + startAt, line);

			// Every 128th record, if more than a second has passed since tlast
			if (lineno && !(lineno & ((1 << 7) - 1)) && (usec_now () >= (tlast + 1000000)))
				printf ("{\"start\":%ld,\"lineno\":%ld,\"dtime\":%lu,\"bytes\":%lu,\"queued\":%d}\n",
						startAt, lineno, (tlast = usec_now ()) - tstart, btot, db.queueSize ());
		}
    } // wait for AerospikeDB's dtor
    printf ("{\"start\":%ld,\"lineno\":%ld,\"dtime\":%lu,\"bytes\":%lu}\n",
			startAt, lineno, usec_now () - tstart, btot);

    return 0;
}
