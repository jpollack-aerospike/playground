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
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include "asmsgpack.hpp"
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
using json = jsoncons::json;

static const char USAGE[] =
    R"(usage: findkeys [options]

options:
  -h --help
  --asdb=SOCKADDR       Aerospike Server [default: 127.0.0.1:3000]
  --bin=STRING          Bin Name [default: map]
  --ns=STRING           Namespace [default: test]
  --set=STRING          Set Name [default: s0]
  --start=NUMBER        Key Start [default: 0]
  --threads=NUMBER      Number of threads [default: 1]
  --maxDepth=NUMBER     Maximum size of queue before we sleep [default: 16]

)";
#if 0
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
#endif

string hexstring (const void *vp, size_t sz)
{
    const char hexm[] = "0123456789ABCDEF";
    dieunless (vp && (sz > 0));
    string ret (sz * 2, 0);
    const char *p = static_cast<const char *>(vp);
    for (size_t ii=0; ii<sz; ii++)
    {
        ret[(2*ii)] = hexm[(p[ii]>>4) & 0x0F];
        ret[(2*ii)+1] = hexm[p[ii] & 0x0F];
    }
    return ret;
}

class AerospikeDB
{
public:
    AerospikeDB (const string& hostport, const string& ns, const string& set, const string& bin, int nthreads = 4);
    ~AerospikeDB ();
    bool exists (int64_t ki);
    int64_t nextSpc (int64_t ki);
    as_record *get (int64_t ki);
    // bool put (int64_t ki, const string& jsonstr);
    // void queuePut (int64_t ki, const string& str) { m_q.enqueue (make_pair (ki, str)); }
    // int queueSize (void) { return m_q.size_approx (); }
private:
    string m_ns;
    string m_set;
    string m_bin;
    aerospike m_as;
    atomic<bool> m_running;
    vector<thread> m_th;
    moodycamel::ConcurrentQueue<pair<int64_t,string>> m_q;
    // void putWorker (void);
};

AerospikeDB::AerospikeDB (const string& hostport, const string& ns, const string& set, const string& bin, int nthreads)
    : m_ns (ns),
      m_set (set),
      m_bin (bin),
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
    // for (int i = 0; i < nthreads; i++)
    // 	m_th.emplace_back (&AerospikeDB::putWorker, this);
}

// void AerospikeDB::putWorker (void)
// {
//     pair<int64_t,string> od;
//     while (m_running.load ()) {
// 	while (!m_q.try_dequeue (od)) {
// 	    this_thread::yield ();
// 	    if (!m_running.load ()) return;
// 	}
// 	dieunless (this->put (od.first, od.second));
//     }
// }

AerospikeDB::~AerospikeDB ()
{
    m_running.store (false);
    m_q.enqueue (make_pair (-1, ""));
    this_thread::yield ();

    for (auto& th : m_th) if (th.joinable ()) th.join ();
    as_error err;
    dieunless (aerospike_close (&m_as, &err) == AEROSPIKE_OK);
}

// bool AerospikeDB::put (int64_t ki, const string& jsonstr)
// {
//     json j = json::parse (jsonstr);

//     as_key key0;
//     as_key_init_int64 (&key0, m_ns.c_str (), m_set.c_str (), ki);
//     as_record rec0;
//     as_record_inita (&rec0, 1);
//     as_orderedmap map0;
//     as_orderedmap_init (&map0, j.size ());
//     for (const auto& [jkey, jval] : j.items ())
// 	as_orderedmap_set (&map0, as_val_new_from_json (jkey), as_val_new_from_json (jval));

//     as_record_set_map (&rec0, m_bin.c_str (), (as_map *) &map0);
//     as_error err;
//     switch (aerospike_key_put (&m_as, &err, NULL, &key0, &rec0)) {
//     case AEROSPIKE_ERR_RECORD_TOO_BIG:
// 	fprintf (stderr, "record %lu too big, not inserted.  original string length: %lu\n", ki, jsonstr.length ());
//     case AEROSPIKE_OK:
// 	as_orderedmap_destroy (&map0);
// 	return true;
//     default:
// 	fprintf(stderr, "key:%lu\tas_map:%p\terr(%d) %s at [%s:%d]\n", ki, &map0, err.code, err.message, err.file, err.line);
//     }
//     return false;
// }

bool AerospikeDB::exists (int64_t ki)
{
    as_key key0;
    as_key_init_int64 (&key0, m_ns.c_str (), m_set.c_str (), ki);

    as_record *rec{nullptr};
    as_error err;
    switch (aerospike_key_exists (&m_as, &err, nullptr, &key0, &rec)) {
    case AEROSPIKE_OK:
	if (rec) {
	    as_record_destroy (rec);
	    return true;
	}
	return false;
    case AEROSPIKE_ERR_RECORD_NOT_FOUND:
	return false;
    default:
	fprintf(stderr, "key_exists(%lu):\terr(%d) %s at [%s:%d]\n", ki, err.code, err.message, err.file, err.line);
    }

    dieunless (false);
    return false;
}

as_record *AerospikeDB::get (int64_t ki)
{
    as_key key0;
    as_key_init_int64 (&key0, m_ns.c_str (), m_set.c_str (), ki);

    as_policy_read rpol;
    as_policy_read_init (&rpol);
    rpol.deserialize = false;

    as_error err;
    as_record *rec{nullptr};
    dieunless (aerospike_key_get (&m_as, &err, &rpol, &key0, &rec) == AEROSPIKE_OK);
    return rec;
}

int64_t AerospikeDB::nextSpc (int64_t ki)
{
    if (!exists (ki))
	return 0;    // so, ki exists

    int64_t si{0};
    while (exists (ki + (1 << si)))  // move out with exponential jumps.  could be parallized.
	si++;

    int64_t lp{ki + (1 << (si - 1))}, up{ki + (1 << si)}; // lp exists, up doesn't exist, bisect to find first not found
    while ((up - lp) > 1) {
	const int64_t mp = ((lp ^ up) >> 1) + (lp & up); // overflow safe midpoint
	(exists (mp) ? lp : up) = mp;
    }

    return up - ki;
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
    uint64_t kii = d["--start"].asLong ();
    uint64_t tstart = usec_now ();
    uint64_t kstart{kii}, tlast{tstart}, btot{0};
    AerospikeDB db{d["--asdb"].asString (), d["--ns"].asString (), d["--set"].asString (), d["--bin"].asString (), (int) d["--threads"].asLong ()};
    string line;

    int64_t sr = db.nextSpc (kii);
    printf ("kii== %lu\tsr== %ld\n",kii, sr);
    if (!sr) return 0;

    for (auto ii=0; ii<1; ii++) {
	as_record *rp = db.get (kii + ii);
	dieunless (rp);


	{
	    const char *str = as_record_get_str (rp, "str");
	    dieunless (str);
	    json j0 = json::parse (str);
	    cout << strlen (str) << "\t" << str << "\n";
	    cout << jsoncons::pretty_print (j0) << "\n";
	}
	
	
	{
	    as_bytes *mapb = as_record_get_bytes (rp, "map");
	    dieunless (mapb);
	    // json j1 = jsoncons::asmsgpack::decode_asmsgpack<json>(mapb->value, mapb->value + mapb->size);
	    jsoncons::asmsgpack::basic_msgpack_cursor<jsoncons::binary_iterator_source<uint8_t *>> cursor (jsoncons::binary_iterator_source<uint8_t *>(mapb->value, mapb->value + mapb->size),
													   jsoncons::asmsgpack::msgpack_decode_options ());

	    jsoncons::json_decoder<json> decoder{};
	    std::error_code ec;
	    json j1 = jsoncons::decode_traits<json,char>::decode (cursor, decoder, ec);
	    if (ec) {  JSONCONS_THROW(jsoncons::ser_error(ec, cursor.context().line(), cursor.context().column())); }
	    cout << mapb->size << "\t" << hexstring (mapb->value, mapb->size) << "\n";
	    cout << jsoncons::pretty_print (j1) << "\n";
	}

	{
	    as_bytes *mpkb = as_record_get_bytes (rp, "msgpack");
	    dieunless (mpkb);
	    json j2 = jsoncons::asmsgpack::decode_asmsgpack<json>(mpkb->value, mpkb->value + mpkb->size);
	    cout << mpkb->size << "\t" << hexstring (mpkb->value, mpkb->size) << "\n";
	    cout << jsoncons::pretty_print (j2) << "\n";
	}

	    
	as_record_destroy (rp); 
	printf ("\n");
	

    }
    return 0;
}
