#include <algorithm>
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
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

inline void __dieunless (const char *msg, const char *file, int line) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", file, line, msg); abort (); }
// syn: assert.  for normal error checking, not just debugging.
#define dieunless(EX) ((EX) || (__dieunless (#EX, __FILE__, __LINE__),false))

using namespace std;

uint64_t micros ()
{
    using Clock = chrono::steady_clock;
    using Res = chrono::microseconds;
    return chrono::duration_cast<Res>(Clock::now().time_since_epoch()).count();
}

atomic<bool> g_running;
const char g_ep_str[] = "JP_AS_DCT_";
const size_t g_ep_str_sz = char_traits<char>::length (g_ep_str);

void sigint_handler (int signum) { g_running = false; }

struct as_msg
{
	uint8_t header_sz; // size of this header - 22
	uint8_t info1;
	uint8_t info2;
	uint8_t info3;
	uint8_t info4;
	uint8_t result_code;
	uint32_t generation;
	uint32_t record_ttl;
	uint32_t transaction_ttl;
	uint16_t n_fields;
	uint16_t n_ops;
	uint8_t data[0]; // first fields, then ops	
} __attribute__((__packed__));

vector<uint8_t> addr_resolve (const string& hostport)
{
    // Resolve hostname:port string to something we can pass to connect(2).
    size_t cpos = hostport.find (':');
    dieunless (cpos != string::npos);
    string f1=hostport.substr (0, cpos), f2=hostport.substr (cpos + 1);

    addrinfo *addri;
    addrinfo hints{};
    hints.ai_family = AF_INET; // I want ipv4  AF_UNSPEC for ipv6
    hints.ai_flags = AI_NUMERICHOST | AI_ADDRCONFIG;
    if (std::any_of (f1.begin (), f1.end (),
		     [](const char c) { return (c < '.') || (c > '9'); }))
	hints.ai_flags &= ~AI_NUMERICHOST;
    dieunless (getaddrinfo (f1.c_str (), f2.c_str (), &hints, &addri) == 0);

    // just take the first in a linked list of addrinfo.  lazy.
    vector<uint8_t> ret (addri->ai_addrlen);
    memcpy (ret.data (), addri->ai_addr, ret.size ());
    freeaddrinfo (addri);
    return ret;
}

size_t info_sync (int fd, char **obuf, const string& istr)
{
    dieunless (obuf);
    size_t sz = istr.length ();
    dieunless (sz < 256);
    uint64_t hdr = 258 + ((uint64_t)sz << 56);
    const struct iovec iov[2] = {
		{ .iov_base=&hdr,			.iov_len=8 },
		{ .iov_base=(void*)istr.c_str (),	.iov_len=sz }
    };
    dieunless (writev (fd, iov, 2) == 8+sz);
    dieunless (read (fd, &hdr, 8) == 8);
    sz = (hdr >> 56) + ((0x00FF & (hdr >> 48)) << 8);
    dieunless (sz < 4096);
    if (*obuf == nullptr) {
		*obuf = (char*)malloc (sz);
    }
    dieunless (read (fd, *obuf, sz) == sz);
	*obuf[sz] = 0;
    return sz;
}

// envp is POSIX but not C++
int main (int argc, char **argv, char **envp)
{
    // Defaults for required variables.
    unordered_map<string,string> p = {
	{ "KEYLB",		"1" },
	{ "KEYUB",		"1000000000" },
	{ "ASDB",		"localhost:3000" },
	{ "NS",			"test" },
	{ "SET",		"s0" }
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

    char *res = nullptr;
    auto sz = info_sync (cfd, &res, "node\npartition-generation\npartitions\nreplicas\nfeatures\n");
    res[sz] = 0;
    cout << "got: " << sz << " bytes\n";
    cout << res << "\n";
    // cout << info_sync (cfd, "service\n");

    close (cfd);
    return 0;
}
