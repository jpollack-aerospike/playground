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
#include <algorithm>
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
    bool put_string (int64_t ki, const string& str);
    bool expr_get (int64_t ki);
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

bool AerospikeDB::put_string (int64_t ki, const string& str)
{
    as_key key0;
    as_key_init_int64 (&key0, m_ns.c_str (), d["--set"].asString ().c_str (), ki);

    as_record rec0;
    as_record_inita (&rec0, 1);

    as_record_set_str (&rec0, "str", str.c_str ());

    as_error err;
    auto rv = aerospike_key_put (&m_as, &err, nullptr, &key0, &rec0);
    if (rv != AEROSPIKE_OK)
	fprintf(stderr, "key:%lu\terr(%d) %s at [%s:%d]\n", ki, err.code, err.message, err.file, err.line);

    return (rv == AEROSPIKE_OK);
}

bool AerospikeDB::expr_get (int64_t ki)
{
    as_key key0;
    as_key_init_int64 (&key0, m_ns.c_str (), d["--set"].asString ().c_str (), ki);

    as_policy_read p;
    as_policy_read_init (&p);

    vector<uint8_t> bv = { 0x08, 0x00, 0x00, 0x00, 0x93, 0x01, 0xa2, 0x17, 0x61, 0xa2, 0x03, 0x61 };
    p.base.filter_exp = (as_exp *) bv.data ();

    as_error err;
    as_record* recp{nullptr};

    auto rv = aerospike_key_get (&m_as, &err, &p, &key0, &recp);
    if (rv != AEROSPIKE_OK)
	fprintf(stderr, "key:%lu\terr(%d) %s at [%s:%d]\n", ki, err.code, err.message, err.file, err.line);

    if (recp)	as_record_destroy (recp);

    return (rv == AEROSPIKE_OK);
}

int main (int argc, char **argv)
{
    d = docopt::docopt (USAGE, {argv+1, argv+argc});
    AerospikeDB db{d["--asdb"].asString (), d["--ns"].asString ()};

    db.put_string (1234, "abcd");
    int ri{0};
    while (ri++ < 100000) {
	db.expr_get (1234);
    }
    fprintf (stderr, "Died after %d runs\n", ri);
    return 0;
}
