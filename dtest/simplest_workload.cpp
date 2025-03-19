#include "as_proto.hpp"
#include "transaction.hpp"
#include "simple_string.hpp"
#include "util.hpp"
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <endian.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <netdb.h>
#include <nlohmann/json.hpp>
#include <numeric>
#include <random>
#include <signal.h>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <queue>
#include <numeric>
#include <algorithm>
#include <mutex>

using json = nlohmann::json;

using namespace std;
unordered_map<string,string> p;

atomic<bool> g_running;
auto g_rng = std::default_random_engine {};
void sigint_handler (int signum) { g_running = false; }
atomic<uint32_t> g_idx;
vector<uint32_t> g_buf;

size_t hist_bucket (double val, double min, double max, size_t numBuckets)
{
    if (val <= min)		return 0;
    else if (val >= max)	return numBuckets - 1;
    else			return 1 + ((val - min) * (numBuckets - 2)) / (max - min);
}

int tcp_connect (const std::string& str)
{
    int fd;
    auto ab = addr_resolve (str);
    dieunless ((fd = ::socket (AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)) > 0);
    dieunless (::connect (fd, (sockaddr *)ab.data (), ab.size ()) == 0);
    return fd;
}

as_msg *visit (as_msg *msg, int ri, int flags)
{
    msg->clear ();
    msg->flags = flags;
    msg->be_transaction_ttl = htobe32 (1000);
    dieunless (msg->add (as_field::type::t_namespace, "ns0"));
    dieunless (msg->add (as_field::type::t_set, "demo"));
    add_integer_key_digest (msg->add (as_field::type::t_digest_ripe, CF_SHA_DIGEST_LENGTH)->data, "demo", ri);
    return msg;
}
as_msg *set_bin (as_msg *msg, uint16_t bidx, int64_t val)
{
    char buf[16] = {0};
    dieunless (sizeof(buf) > snprintf (buf, sizeof(buf), "b%05d", bidx));
    as_op *op = msg->add (as_op::type::t_write, buf, 8);
    op->data_type = as_particle::type::t_integer;
    *(uint64_t *)op->data () = htobe64 (val);
    return msg;
}

as_msg *get_bin (as_msg *msg, uint16_t bidx)
{
    char buf[16] = {0};
    dieunless (sizeof(buf) > snprintf (buf, sizeof(buf), "b%05d", bidx));
    as_op *op = msg->add (as_op::type::t_read, buf, 0);
    return msg;
}

int64_t bin_value (as_msg *msg)
{
    return be64toh (*(int64_t *)msg->ops_begin ()->data ());
}
void record_set (as_msg *msg, int ri, size_t bidx, int64_t val)
{
    visit (msg, ri, AS_MSG_FLAG_WRITE);
    set_bin (msg, bidx, val);
}

void record_get (as_msg *msg, int ri, size_t bidx)
{
    visit (msg, ri, AS_MSG_FLAG_READ);
    get_bin (msg, bidx);
}

void record_init (as_msg *msg, int ri, size_t numBins, size_t padSize)
{
    visit (msg, ri, (!numBins && !padSize) ? AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE : AS_MSG_FLAG_WRITE);
    if (numBins > 0) {
	vector<size_t> v (numBins);
	std::iota (v.begin (), v.end (), 1);
	std::shuffle (v.begin (), v.end (), g_rng);
	for (const auto& e : v) set_bin (msg, e, e);
    }

    if (padSize > 0) {
	std::string padstring (padSize, 'x');
	dieunless (msg->add (as_op::type::t_write, "padding", padstring.length (), padstring.c_str (), as_particle::type::t_string));
    }
}

void record_size (as_msg *msg, int ri)
{
    visit (msg, ri, AS_MSG_FLAG_READ);
    auto pload = json::to_msgpack ({ { 74 }, 0 });
    dieunless (msg->add (as_op::type::t_exp_read, "size", pload.size (), pload.data ()));
}

void update_worker_entry (int rate)
{
    int fd = tcp_connect (p["ASDB"]);
    auto nbins = stoi (p["NBINS"]);
    auto id_lb = stoi (p["KEYLB"]);
    auto id_ub = stoi (p["KEYUB"]);

    thread_local static std::random_device rd;
    thread_local static std::mt19937 gen(rd());
    std::uniform_int_distribution<> distr(id_lb, id_ub); // define the range
    std::uniform_int_distribution<> distb(1, nbins); // define the range
    std::uniform_int_distribution<> distv(0, std::numeric_limits<int32_t>::max ());

    uint64_t tlast = 0;
    uint64_t tnow;
    int64_t ri;
    int64_t val;
    size_t bidx;
    char buf[2048];
    as_msg *res = (as_msg *)(buf + 64);
    as_msg *req = (as_msg *)(buf + 1024);
    uint64_t duration;
    
    while (g_running.load ()) {
	tlast = tnow;
	record_set (req, distr (gen), distb (gen), distv (gen));
	while (g_running.load () && ((tnow = usec_now ()) < (tlast + (1000000 / rate)))) {
	    uint64_t td = (tlast + (1000000 / rate)) - tnow;
	    usleep ((td>10) ? (td-10) : 1);
	}
	if (!g_running.load ()) {
	    break;
	}

	dieunless ((1024-64) > timed_call (fd, &res, req, duration));
	dieunless (res->result_code == 0);
	auto idx = g_idx.fetch_add (2);
	g_buf [((idx & 1) * (g_buf.size () / 2)) + (idx / 2)] = duration;
    }
    
    close (fd);		

}

void stats_worker_entry (int rate)
{
    uint64_t tlast = 0;
    uint64_t tnow = usec_now ();
    json jo = { { "now", tnow } };

    while (g_running) {
	while (g_running && ((tnow = usec_now ()) < (tlast + (1000000 / rate)))) {
	    uint64_t td = (tlast + (1000000 / rate)) - tnow;
	    usleep ((td>50) ? (td-50) : 10);
	}
	if (!g_running)	    break;
	tlast = tnow;
	jo["now"] = tnow;
	jo["data"] = json::array ();

	auto nidx = !g_idx & 1; // ping pong
	auto lidx = g_idx.exchange (nidx);
	auto idxb = (lidx & 1) * (g_buf.size () / 2);
	jo["data"].get_ptr<json::array_t*>()->reserve (lidx / 2);
	for (auto ii = 0; ii < (lidx / 2); ii++) {
	    jo["data"][ii] = g_buf[idxb + ii];
	    g_buf[idxb + ii] = 0;
	}
	
	printf ("%s\n", jo.dump ().c_str ());
    }

}

void update_entry (void)
{
    int nth = stoi (p["THREADS"]);
    vector<thread> vth;

    g_idx = 0;
    g_buf.resize (1024*1024);
    
    for (int ii=0; ii < nth; ii++)
	vth.emplace_back (update_worker_entry, stoi (p["RATE"]));
    
    vth.emplace_back (stats_worker_entry, 1);
    
    while (g_running) {
	usleep (1000);
    }

    for (auto& th : vth) {
	th.join ();
    }

}

void init_entry (void)
{
    int fd = tcp_connect (p["ASDB"]);
    auto recsize = stoi (p["RECSIZE"]);
    auto nbins = stoi (p["NBINS"]);
    auto id_lb = stoi (p["KEYLB"]);
    auto id_ub = stoi (p["KEYUB"]);
    char *buf = (char *)malloc (2 * 1024 * 1024);
    as_msg *res = (as_msg *)(buf + 64);
    as_msg *req = (as_msg *)(buf + 1024);

    record_init (req, 0, nbins, 1);
    dieunless ((1024-64) > call (fd, &res, req));
    dieunless (res->result_code == 0);
    record_size (req, 0);
    dieunless ((1024-64) > call (fd, &res, req));
    dieunless (res->result_code == 0);
    auto psize = (recsize - bin_value (res)) + 1;
    if (psize <= 1) psize = 0;
    for (auto id = id_lb; id <= id_ub; id++) {
	json jo = { { "type", "insert" }, { "id", id }, { "bins", nbins }, 
		    { "bytes", recsize } };
	record_init (req, id, nbins, psize);
	jo["now"] = usec_now ();
	uint64_t duration;
	dieunless ((1024-64) > timed_call (fd, &res, req, duration));
	dieunless (res->result_code == 0);
	jo["us"] = duration;
	printf ("%s\n", jo.dump ().c_str ());
    }
    free (buf);
}

const char g_ep_str[] = "JP_WORKLOAD_";
const size_t g_ep_str_sz = char_traits<char>::length (g_ep_str);

int main (int argc, char **argv, char **envp)
{
    dieunless (argc >= 2);
    // srand ((unsigned int)clock ());
    // Defaults for required variables.
    p = {
	{ "KEYLB",		"1" },
	{ "KEYUB",		"10" },
	{ "ASDB",		"localhost:3000" },
	{ "NS",			"ns0" },
	{ "NBINS",		"20000" },
	{ "RECSIZE",		"500000" },
	{ "HISTRES",		"100" },
	{ "RATE",		"200" },
	{ "THREADS",		"1" },
    };
    // Override from environment
    for (auto ep = *envp; ep; ep = *(++envp))
	if (!strncmp (g_ep_str, ep, g_ep_str_sz)) {
	    auto vs = strchr (ep, '=');
	    auto ks = string (ep).substr (g_ep_str_sz, (vs - ep) - g_ep_str_sz);
	    if (ks.length ()) p[ks] = string (vs + 1);
	}

    signal (SIGINT, sigint_handler);
    g_running = true;

    // Make a client fd and connect
    string cmd (argv[1]);
    
    if (!cmd.compare ("init")) {			init_entry ();
    } else if (!cmd.compare ("update")) {		update_entry ();
    } else if (!cmd.compare ("debug")) {		debug_entry ();
    } else if (!cmd.compare ("display")) {		display_entry ();
    } else {
	fprintf (stderr, "usage:\n %s init|update\n", argv[0]);
    }
    

    return 0;
}
