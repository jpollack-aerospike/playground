#include "as_proto.hpp"
#include "transactional_string.hpp"
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

void scan_start (int fd, int nrec)
{
    char buf[65536];
    as_msg *msg = (as_msg *)(buf + 8);

    vector<uint16_t> parr (4096);
    iota (parr.begin (), parr.end (), 0);

    msg->clear ();
    msg->flags = AS_MSG_FLAG_READ | AS_MSG_FLAG_PARTITION_DONE;
    dieunless (msg->add (as_field::type::t_namespace, "ns0"));
    dieunless (msg->add (as_field::type::t_pid_array, parr.size () * 2, parr.data ()));
    if (nrec) {
	uint64_t tmp = htobe64 (nrec);
	dieunless (msg->add (as_field::type::t_sample_max, 8, &tmp));
    }
    
    dieunless (write (fd, msg));
}

bool scan_next (int fd)
{
    char *bp = nullptr;
    size_t bn = read (fd, (void **)&bp);
    as_msg *msg = (as_msg *)bp;
    bool rv = bp && (msg->result_code == 0) && !(msg->flags & AS_MSG_FLAG_LAST);
    while (rv && bn) {
	size_t msz = msg->end () - (uint8_t *)msg;
	json jm = to_json (msg);
	printf ("%s\n", jm.dump ().c_str ());
	msg = (as_msg *)msg->end ();
	bn -= msz;
    }

    if (bp) {
	free (bp);
    }
    return rv;
}

int main (int argc, char **argv, char **envp)
{
    p = {
	{ "ASDB",		"localhost:3000" },
	{ "NS",			"ns0" },
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

    int fd;
    auto ab = addr_resolve (p["ASDB"]);
    dieunless ((fd = socket (AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)) > 0);
    dieunless (connect (fd, (sockaddr *)ab.data (), ab.size ()) == 0);
    
    scan_start (fd, 0);
    
    while (scan_next (fd)) {


    }

    close (fd);
    return 0;
}
