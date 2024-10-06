#include <endian.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <memory>
#include <netdb.h>
#include <signal.h>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <numeric>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <thread>
#include "as_proto.hpp"
#include "util.hpp"
#include "simple_string.hpp"

#include <string.h>

using namespace std;
unordered_map<string,string> p;

const char g_ep_str[] = "JP_INIT_";
const size_t g_ep_str_sz = char_traits<char>::length (g_ep_str);

string info_get_roster (int cfd, const string& ns) {
    
    char buf[1024] = {0};
    char *bp = buf;
    size_t bn = call (cfd, (void **) &bp, "roster:namespace=" + ns + "\n");
    bp[bn-1] = 0;
    
    char *tc = strchr (bp, '\t');
    dieunless (tc);
    tc++;

    auto rr = string (tc);
    return string (tc);
}

bool info_set_roster (int cfd, const string& ns, const string& str) {
    
    char buf[1024] = {0};
    char *bp = buf;
    size_t bn = call (cfd, (void **) &bp, "roster-set:namespace=" + ns + ";nodes=" + str + "\n");
    bp[bn-1] = 0;
    
    char *tc = strchr (bp, '\t');
    dieunless (tc);
    tc++;

    return (memcmp ("ok", tc, 2) == 0);
}

bool info_recluster (int cfd) {
    
    char buf[1024] = {0};
    char *bp = buf;
    size_t bn = call (cfd, (void **) &bp, "recluster:\n");
    bp[bn-1] = 0;
    
    char *tc = strchr (bp, '\t');
    dieunless (tc);
    tc++;

    return (memcmp ("ok", tc, 2) == 0);
}

int main (int argc, char **argv, char **envp)
{
    // Defaults for required variables.
    p = {
	{ "KEYLB",		"1" },
	{ "KEYUB",		"100000" },
	{ "ASDB",		"localhost:3000" },
	{ "NS",			"ns0" }
    };
    // Override from environment
    for (auto ep = *envp; ep; ep = *(++envp))
	if (!strncmp (g_ep_str, ep, g_ep_str_sz)) {
	    auto vs = strchr (ep, '=');
	    auto ks = string (ep).substr (g_ep_str_sz, (vs - ep) - g_ep_str_sz);
	    if (ks.length ()) p[ks] = string (vs + 1);
	}

    auto ab = addr_resolve (p["ASDB"]);

    // Make a client fd and connect
    int cfd;
    dieunless ((cfd = socket (AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)) > 0);
    dieunless (connect (cfd, (sockaddr *)ab.data (), ab.size ()) == 0);

    auto ret = info_get_roster (cfd, "ns0");
    printf ("%s\n", ret.c_str ());
    string observed = get_labeled (ret, "observed_nodes");
    string roster = get_labeled (ret, "roster");
    if (roster.compare (observed)) {
	dieunless (info_set_roster (cfd, "ns0", observed));
	dieunless (info_recluster (cfd));
    }
    
    close (cfd);

    return 0;
}
