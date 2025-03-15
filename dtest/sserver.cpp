#include "as_listener.hpp"
#include "as_proto.hpp"
#include <algorithm>
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
#include <numeric>
#include <signal.h>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <string>
#include <sys/time.h>
#include <sys/uio.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "util.hpp"

using namespace std;

atomic<bool> g_running;
const char g_ep_str[] = "JP_SERVER_";
const size_t g_ep_str_sz = char_traits<char>::length (g_ep_str);

void sigint_handler (int signum) { g_running = false; }

// envp is POSIX but not C++
int main (int argc, char **argv, char **envp)
{
    // Defaults for required variables.
    unordered_map<string,string> p = {
	{ "ASDB",		"localhost:3000" },
	{ "NS",			"ns0" },
	{ "SET",		"s0" }
    };
    // Override from environment
    for (auto ep = *envp; ep; ep = *(++envp))
	if (!strncmp (g_ep_str, ep, g_ep_str_sz)) {
	    auto vs = strchr (ep, '=');
	    auto ks = string (ep).substr (g_ep_str_sz, (vs - ep) - g_ep_str_sz);
	    if (ks.length ()) p[ks] = string (vs + 1);
	}
    
    g_running = true;
    signal (SIGINT, sigint_handler);
    
    as_listener al;
    al.onMsg = [](as_msg **dst, const as_msg* src) -> size_t {
	printf ("%s\n", to_json (src).dump ().c_str ());
	return 0;
    };
    
    al.start ("0.0.0.0:4000");

    while (g_running) usleep (1000);

    al.stop ();

    return 0;
}
