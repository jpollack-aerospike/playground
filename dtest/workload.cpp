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

using json = nlohmann::json;

using namespace std;
unordered_map<string,string> p;

const char g_ep_str[] = "JP_WORKLOAD_";
const size_t g_ep_str_sz = char_traits<char>::length (g_ep_str);
uint64_t usec_now (void) { return chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count(); }

atomic<bool> g_running;
void sigint_handler (int signum) { g_running = false; }
atomic<int> g_ntx;


void init_entry (void)
{
    int fd;
    auto ab = addr_resolve (p["ASDB"]);
    dieunless ((fd = socket (AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)) > 0);
    dieunless (connect (fd, (sockaddr *)ab.data (), ab.size ()) == 0);

    dieunless (str_truncate (fd));

    int64_t id_lb = stoi (p["KEYLB"]);
    int64_t id_ub = stoi (p["KEYUB"]);
    int64_t txnmax = stoi (p["TXNMAX"]);
    int64_t ntx = 0;
    transaction ts;
    
    for (auto id = id_lb; id <= id_ub; id++) {
	int64_t bal = 10;
	json jo = { { "id", id }, { "bal", bal } };
	// dieunless (ts.set (fd, id, jo.dump ()) == 0);
	dieunless (ts.set (fd, id, bal) == 0);


	if (++ntx == txnmax) {
	    dieunless (ts.commit (fd));
	    ts.finish (fd);
	    ntx = 0;
	}
    }

    dieunless (ts.commit (fd));
    ts.finish (fd);
    close (fd);
}

void debug_entry (void)
{
    int fd;
    auto ab = addr_resolve (p["ASDB"]);
    dieunless ((fd = socket (AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)) > 0);
    dieunless (connect (fd, (sockaddr *)ab.data (), ab.size ()) == 0);

    int64_t id_lb = stoi (p["KEYLB"]);
    int64_t id_ub = stoi (p["KEYUB"]);
    
    for (auto id = id_lb; id <= id_ub; id++) {
	string str;
	str_debug (fd, id, str);
	printf ("%d:\t%s\n", id, str.c_str ());
    }

    close (fd);
}

int transfer_str (int fd, int64_t ka, int64_t kb, int amt)
{
    transaction ts;
    string str;
    json ja, jb;
    string dbl;

    if (ts.get (fd, ka, str)) {
	return 0;
    }
    ja = json::parse (str);
    dbl += str;
    str.clear ();
    if (ts.get (fd, kb, str)) {
	return 0;
    }
    jb = json::parse (str);
    dbl += str;
    
    int bala = ja["bal"];
    if (bala < amt) {
	return 2;
    }

    int balb = jb["bal"];

    ja["bal"] = bala - amt;
    jb["bal"] = balb + amt;
    
    dbl += ja.dump ();
    dbl += jb.dump ();

    if (ts.set (fd, ka, ja.dump ())) {
	ts.finish (fd);
	return 0;
    }
    
    if (ts.set (fd, kb, jb.dump ())) {
	ts.finish (fd);
	return 0;
    }
    
    if (!ts.commit (fd)) {
	ts.finish (fd);
	return 0;
    }
    
    ts.finish (fd);
    return 1;
}

int transfer_int (int fd, int64_t ka, int64_t kb, int amt)
{
    transaction ts;
    string str;
    json ja, jb;
    string dbl;
    int64_t bala, balb;

    if (ts.get (fd, ka, bala)) {
	return 0;
    }

    if (ts.get (fd, kb, balb)) {
	return 0;
    }

    if (bala < amt) {
	return 2;
    }

    bala -= amt;
    balb += amt;

    if (ts.set (fd, ka, bala)) {
	ts.finish (fd);
	return 0;
    }
    
    if (ts.set (fd, kb, balb)) {
	ts.finish (fd);
	return 0;
    }
    
    if (!ts.commit (fd)) {
	ts.finish (fd);
	return 0;
    }
    
    ts.finish (fd);
    return 1;
}

void update_worker_entry (int rate)
{
    int cfd;
    auto ab = addr_resolve (p["ASDB"]);
    dieunless ((cfd = socket (AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)) > 0);
    dieunless (connect (cfd, (sockaddr *)ab.data (), ab.size ()) == 0);

    int64_t id_lb = stoi (p["KEYLB"]);
    int64_t id_ub = stoi (p["KEYUB"]);

    thread_local static std::random_device rd;
    thread_local static std::mt19937 gen(rd());
    std::uniform_int_distribution<> distr(id_lb, id_ub); // define the range
    uint64_t tlast = 0;
    uint64_t tnow;
    int64_t ka, kb;
    std::queue<array<int64_t,2>> wq;
    while (g_running.load ()) {
	while (g_running.load () && ((tnow = usec_now ()) < (tlast + (1000000 / rate)))) {
	    uint64_t td = (tlast + (1000000 / rate)) - tnow;
	    usleep ((td>10) ? (td-10) : 1);
	}
	if (!g_running.load ()) {
	    break;
	}
	tlast = tnow;

	if (wq.size () && (distr (gen) % 2)) {
	    ka = wq.front ()[0];
	    kb = wq.front ()[1];
	    wq.pop ();
	} else {
	    
	    do {
		ka = distr (gen);
		kb = distr (gen);
	    } while (ka == kb);
	
	}
	int ret;
	int rty = 0;
	while (((ret = transfer_int (cfd, ka, kb, 1)) == 0)
	       && (distr (gen) % 2)) {
	    usleep (1);
	}
	
	if (ret == 0) {
	    wq.push ({ka, kb});
	}

	if (ret == 1) {
	    g_ntx++;
	}
    }

    close (cfd);		

}

int get_all_str (int fd, vector<int>& vec, int64_t lb, int64_t ub)
{
    transaction ts;
    string str;
    int64_t val;
    for (int ii=lb; ii<=ub; ii++) {
	int rc = ts.get (fd, ii, str);
	if (rc) return rc;
	json jo = json::parse (str);
	vec[ii-lb] = jo["bal"];
    }
    if (!ts.commit (fd)) {
	ts.finish (fd);
	return -1;
    }
    ts.finish (fd);
    return 0;
}

int get_all_int (int fd, vector<int>& vec, int64_t lb, int64_t ub)
{
    transaction ts;
    int64_t val;
    for (int ii=lb; ii<=ub; ii++) {
	int rc = ts.get (fd, ii, val);
	if (rc) return rc;
	vec[ii-lb] = val;
    }
    if (!ts.commit (fd)) {
	ts.finish (fd);
	return -1;
    }
    ts.finish (fd);
    return 0;
}

json get_all_json (int fd, int64_t lb, int64_t ub)
{
    vector<int> vec;
    vec.resize ((ub-lb) + 1);
    int rty = 0;
    int ret;
    while ((++rty < 32) && ((ret = get_all_int (fd, vec, lb, ub)) != 0)) {
	usleep (1);
    }
    json jo;
    jo["d"] = vec;
    if (ret == 0) {
	sort (vec.begin (), vec.end ());
	jo["s"] = vec;
	jo["sum"] = reduce (vec.begin (), vec.end ());
    } else if (ret == -1) {
	jo["err"] = "verify failed";
    } else {
	jo["err"] = { { "ts.get", ret } };
    }
    jo["rty"] = rty;
    return jo;
}

void display_worker_entry (int rate)
{
    int cfd;
    auto ab = addr_resolve (p["ASDB"]);
    dieunless ((cfd = socket (AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)) > 0);
    dieunless (connect (cfd, (sockaddr *)ab.data (), ab.size ()) == 0);

    int64_t id_lb = stoi (p["KEYLB"]);
    int64_t id_ub = stoi (p["KEYUB"]);

    uint64_t tlast = 0;
    uint64_t tnow = usec_now ();

    while (g_running) {
	while (g_running && ((tnow = usec_now ()) < (tlast + (1000000 / rate)))) {
	    uint64_t td = (tlast + (1000000 / rate)) - tnow;
	    usleep ((td>50) ? (td-50) : 10);
	}
	if (!g_running)	    break;
	tlast = tnow;
	
	json jo = get_all_json (cfd, id_lb, id_ub);
	printf ("%s\n", jo.dump ().c_str ());
	
    }
    close (cfd);
}

void stats_worker_entry (int rate)
{
    uint64_t tlast = 0;
    uint64_t tnow = usec_now ();

    cout << "sinter\n";

    while (g_running) {
	while (g_running && ((tnow = usec_now ()) < (tlast + (1000000 / rate)))) {
	    uint64_t td = (tlast + (1000000 / rate)) - tnow;
	    usleep ((td>50) ? (td-50) : 10);
	}
	if (!g_running)	    break;
	tlast = tnow;
	int ntx = g_ntx.exchange (0);
	json jo = { { "now", tnow }, { "ntx", ntx } };
	printf ("%s\n", jo.dump ().c_str ());
    }

    cout << "sexit";

}

void update_entry (void)
{
    int nth = 10;
    vector<thread> vth;

    for (int ii=0; ii < nth; ii++)
	vth.emplace_back (update_worker_entry, 200);
    
    vth.emplace_back (stats_worker_entry, 1);
    
    while (g_running) {
	usleep (1000);
    }

    for (auto& th : vth) {
	th.join ();
    }

}

void display_entry (void)
{
    std::thread dth (display_worker_entry, 1);
    
    while (g_running) {
	usleep (1000);
    }
    
    dth.join ();
}

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
	{ "TXNMAX",		"10" }
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
