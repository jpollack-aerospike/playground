#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_query.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_stringmap.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <concurrentqueue.h>
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

inline void __dieunless (const char *msg, const char *file, int line) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", file, line, msg); abort (); }

#define dieunless(EX) ((EX) || (__dieunless (#EX, __FILE__, __LINE__),false))

using namespace std;

static const char USAGE[] =
    R"(usage: invalid_geojson_test [options]

options:
  -h --help
  --asdb=SOCKADDR       Aerospike server [default: 127.0.0.1:3000]
  --ns=STRING           Namespace [default: test]
  --set=STRING          Set name [default: s0]

)";

static docopt::Options d;

class AerospikeDB
{
public:
    AerospikeDB (const string& hostport, const string& ns);
    ~AerospikeDB ();
    bool badput (int64_t ki);
private:
    string m_ns;
    aerospike m_as;
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
}

AerospikeDB::~AerospikeDB ()
{
    as_error err;
    dieunless (aerospike_close (&m_as, &err) == AEROSPIKE_OK);
}

bool AerospikeDB::badput (int64_t ki)
{
    as_key key0;
    as_key_init_int64 (&key0, m_ns.c_str (), d["--set"].asString ().c_str (), ki);
    
    as_record rec0;
    as_record_inita (&rec0, 1);

    char buff[1024];
    snprintf(buff, sizeof(buff), "230530172131416221");
    as_record_set_geojson_str(&rec0, "bin", buff );

    as_error err;
    switch (aerospike_key_put (&m_as, &err, nullptr, &key0, &rec0)) {
    case AEROSPIKE_OK:
	as_record_destroy (&rec0);
	return true;
    default:
	fprintf(stderr, "key:%lu\terr(%d) %s at [%s:%d]\n", ki, err.code, err.message, err.file, err.line);
    }
    return true;
}
int main (int argc, char **argv)
{
    // a sort of "global params"
    d = docopt::docopt (USAGE, {argv+1, argv+argc});
    {
	AerospikeDB db{d["--asdb"].asString (), d["--ns"].asString ()};
	db.badput (100);
    } // wait for AerospikeDB's dtor

    return 0;
}
