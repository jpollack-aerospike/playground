#include "docopt.h"
#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <getopt.h>
#include <iomanip>
#include <iostream>
#include <jansson.h>
#include <map>
#include <memory>
#include <openssl/sha.h>
#include <readosm.h>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <string>
#include <sys/time.h>
#include <vector>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_nil.h>

inline void __dieunless (const char *msg, const char *file, int line) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", file, line, msg); abort (); }

#define dieunless(EX) ((EX) || (__dieunless (#EX, __FILE__, __LINE__),false))

using namespace std;
using json = nlohmann::json;

static const char USAGE[] =
    R"(usage: load [options] 

options:
  -h --help
  --asdb=SOCKADDR       Aerospike Server [default: 127.0.0.1:3000]
  --bin=STRING          Bin Name [default: map]
  --ns=STRING           Namespace [default: test]
  --set=STRING          Set Name [default: s0]
  --start=NUMBER        Key Start [default: 0]

)";


as_val * as_val_from_json (const json& jel)
{
    switch (jel.type ())
    {
    case json::value_t::object:
    {
	as_orderedmap *mp = as_orderedmap_new (jel.size ());
	for (const auto& [jkey, jval] : jel.items ())
	    as_orderedmap_set (mp, as_val_from_json (jkey), as_val_from_json (jval));
	
	return (as_val *) mp;
    }
    case json::value_t::array:
    {
	as_arraylist *lp = as_arraylist_new (jel.size (), 0);
	for (const auto& el : jel)
	    as_arraylist_append (lp, as_val_from_json (el));
	
	return (as_val *)lp;
    }
    case json::value_t::string:				return (as_val *)as_string_new (strdup (jel.get<string>().c_str ()), true);
    case json::value_t::number_integer:			return (as_val *)as_integer_new (jel.get<int64_t>());
    case json::value_t::number_unsigned:		return (as_val *)as_integer_new (jel.get<uint64_t>());
    case json::value_t::number_float:			return (as_val *)as_double_new (jel.get<double>());
    case json::value_t::boolean:			return (as_val *)as_boolean_new (jel.get<bool>());
    case json::value_t::null:				return (as_val *)&as_nil;
    }
}


class AerospikeDB
{
public:
    AerospikeDB (const string& hostport, const string& ns, const string& set, const string& bin);
    ~AerospikeDB ();
    bool put (int64_t ki, const string& jsonstr);
private:
    string m_ns;
    string m_set;
    string m_bin;
    aerospike m_as;
};

AerospikeDB::AerospikeDB (const string& hostport, const string& ns, const string& set, const string& bin)
    : m_ns (ns),
      m_set (set),
      m_bin (bin)
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

bool AerospikeDB::put (int64_t ki, const string& jsonstr)
{
    json j = json::parse (jsonstr);
    as_key key0;
    as_key_init_int64 (&key0, m_ns.c_str (), m_set.c_str (), ki);
    as_record rec0;
    as_record_inita (&rec0, 1);
    as_val *asv = as_val_from_json (j);
    as_record_set_map (&rec0, m_bin.c_str (), (as_map *) asv);
    as_error err;
    if (aerospike_key_put (&m_as, &err, NULL, &key0, &rec0) != AEROSPIKE_OK) {

	
	fprintf(stderr, "key:%lu\tas_val:%p\terr(%d) %s at [%s:%d]\n", ki, asv, err.code, err.message, err.file, err.line); 
	return false;
    }
}

static uint64_t usec_now (void) { return chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count(); }
static bool log_callback(as_log_level level, const char * func, const char * file, uint32_t line, const char * fmt, ...)
{
        va_list ap;
        va_start(ap, fmt);
        vprintf(fmt, ap);
        printf("\n");
        va_end(ap);
        return true;
}

int main (int argc, char **argv) 
{ 
    auto d{docopt::docopt (USAGE,{argv+1,argv+argc})};
    int64_t kii = d["--start"].asLong ();
    AerospikeDB db{d["--asdb"].asString (), d["--ns"].asString (), d["--set"].asString (), d["--bin"].asString ()};

    string line;
    while (getline (std::cin, line))	
	dieunless (db.put (kii++, line));

    return 0;
}
