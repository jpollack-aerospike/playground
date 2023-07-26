#include <unistd.h>
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_query.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_stringmap.h>
#include <atomic>
#include <algorithm>
#include <cstdlib>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <docopt.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <openssl/sha.h>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <string>
#include <sys/time.h>
#include <vector>
#include <signal.h>

inline void __dieunless (const char *msg, const char *file, int line) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", file, line, msg); abort (); }

#define dieunless(EX) ((EX) || (__dieunless (#EX, __FILE__, __LINE__),false))

using namespace std;

static const char USAGE[] =
    R"(usage: test [options]

options:
  -h --help
  --asdb=SOCKADDR       Aerospike server [default: 127.0.0.1:3000]
  --ns=STRING           Namespace [default: test]
  --set=STRING          Set name [default: s0]
  --rpp=NUMBER          Records per partition [default: 100]
  --qwidth=NUMBER       Range query width [default: 1000]

)";

static docopt::Options d;

class AerospikeDB
{
public:
    AerospikeDB (const string& hostport, const string& ns);
    ~AerospikeDB ();
    bool put_int64 (int64_t ki, int64_t val);
	void create_sindex ();
	size_t range_query (int64_t lb, int64_t ub);
	bool callback (const as_val *val);
	static bool callback_wrapper (const as_val *val, void *udata) {
		return reinterpret_cast<AerospikeDB*>(udata)->callback (val);
	}
private:
    string m_ns;
    aerospike m_as;
	atomic<size_t> m_nrec;
};

AerospikeDB::AerospikeDB (const string& hostport, const string& ns)
    : m_ns (ns)
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

    as_index_task task;
	dieunless (aerospike_index_create (&m_as, &err, &task, NULL, m_ns.c_str (), d["--set"].asString ().c_str (), "num", "idx_num", AS_INDEX_NUMERIC) == AEROSPIKE_OK);
    aerospike_index_create_wait (&err, &task, 0);
}

AerospikeDB::~AerospikeDB ()
{
	as_error err;
	dieunless (aerospike_index_remove (&m_as, &err, NULL, m_ns.c_str (), "idx_num") == AEROSPIKE_OK);
    dieunless (aerospike_close (&m_as, &err) == AEROSPIKE_OK);
}

bool AerospikeDB::callback (const as_val *val)
{
	if (!val)	return false;

	as_record *rec = as_record_fromval (val);
	m_nrec++;
	return true;
}

size_t AerospikeDB::range_query (int64_t lb, int64_t ub)
{
	as_query q;
	as_query_init (&q, m_ns.c_str (), d["--set"].asString ().c_str ());

	as_query_where_init(&q, 1);
	as_query_where(&q, "num", as_integer_range (lb, ub));
	as_error err;
	m_nrec = 0;
	dieunless (aerospike_query_foreach (&m_as, &err, NULL, &q, AerospikeDB::callback_wrapper, this) == AEROSPIKE_OK);
	as_query_destroy (&q);
	return m_nrec;
}
bool AerospikeDB::put_int64 (int64_t ki, int64_t val)
{
    as_key key0;
    as_key_init_int64 (&key0, m_ns.c_str (), d["--set"].asString ().c_str (), ki);

    as_record rec0;
    as_record_inita (&rec0, 1);
    as_record_set_int64 (&rec0, "num", val);

    as_error err;
    auto rv = aerospike_key_put (&m_as, &err, nullptr, &key0, &rec0);
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
void sigint_handler (int signum) { g_running = false; }

int main (int argc, char **argv)
{
    d = docopt::docopt (USAGE, {argv+1, argv+argc});
    AerospikeDB db{d["--asdb"].asString (), d["--ns"].asString ()};
	int64_t rpp = (int64_t) d["--rpp"].asLong ();
	int64_t qwidth = (int64_t) d["--qwidth"].asLong ();

	printf ("Loading %lu records...\n", 4096*rpp);

	for (int64_t ii=0; ii<(4096*rpp); ii++)
		db.put_int64 (ii, ii);

	g_running = true;
	signal (SIGINT, sigint_handler);
	g_rand_state = 1;
	auto maxlb = ((4096 * rpp) - (qwidth - 1));
	printf ("Querying... (press Ctrl-C to stop)\n");
	while (g_running) {
		size_t lb = lcg_parkmiller () % maxlb;
		size_t ub = lb + (qwidth - 1);
		size_t nfound = db.range_query (lb, ub);
		if (nfound != qwidth) {
			printf ("Mismatch! got %d records between [%d, %d], expected %d.\n", nfound, lb, ub, qwidth);
		}
	}
	
    return 0;
}
