#include <algorithm>
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
#include "cf_ripemd160.h"
#include "as_proto.hpp"

inline void __dul (const char *m, const char *f, int l) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", f, l, m); abort (); }
#define dieunless(EX) ((EX) || (__dul (#EX, __FILE__, __LINE__),false))

using namespace std;

uint64_t micros ()
{
    using Clock = chrono::steady_clock;
    using Res = chrono::microseconds;
    return chrono::duration_cast<Res>(Clock::now().time_since_epoch()).count();
}

atomic<bool> g_running;
const char g_ep_str[] = "JP_ASCONN_";
const size_t g_ep_str_sz = char_traits<char>::length (g_ep_str);

void sigint_handler (int signum) { g_running = false; }

size_t make_set_any (void *dst, const string& ns, const string& sn, const string& bn, uint64_t ki,
		     as_particle::type dt, size_t bs, const void *bd);


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

size_t add_as_field_hdr (void *dst, as_field::type t, size_t sz)
{
    uint8_t *p = (uint8_t *)dst;
    *(as_field *)p = (as_field){
	.be_sz = htobe32 (sz + 1),
	.t = t
    };
    p += sizeof(as_field);
    return sizeof(as_field);
}

size_t add_as_field (void *dst, as_field::type t, size_t sz, const void *src)
{
    uint8_t *p = (uint8_t *)dst;
    p += add_as_field_hdr (p, t, sz);

    memcpy (p, src, sz);
    p += sz;

    return p - (uint8_t *)dst;
}

size_t add_integer_key_digest (void *dst, const string& sn, uint64_t ki)
{
    cf_RIPEMD160_CTX ctx;
    cf_RIPEMD160_Init (&ctx);
    cf_RIPEMD160_Update (&ctx, sn.c_str (), sn.length ());
    {
	char buf[9];
	buf[0] = (char) as_particle::type::t_integer; // key type
	*(uint64_t *)&buf[1] = htobe64 (ki);
	cf_RIPEMD160_Update (&ctx, buf, 9);
    }

    cf_RIPEMD160_Final ((uint8_t *)dst, &ctx);
    return CF_SHA_DIGEST_LENGTH;

}
size_t get_resp (int fd, char **obuf)
{
    uint8_t hdr[8];
    dieunless (read (fd, &hdr[0], 8) == 8);

    size_t sz = (size_t)hdr[7] + ((size_t)hdr[6] << 8)
	+ ((size_t)hdr[5] << 16) + ((size_t)hdr[4] << 24);

    if (*obuf == nullptr) {
	*obuf = (char *)malloc (sz);
    }
    dieunless (read (fd, *obuf, sz));
    return sz;
}

void to_hex (char *dst, uint8_t *src, size_t n)
{
    const char lut[] = "0123456789ABCDEF";
    for (size_t ii = 0; ii < n; ii++)
    {
	uint8_t v = src[ii];
	dst[(2 * ii)] = lut[(v & 0xF0) >> 4];
	dst[(2 * ii) + 1] = lut[(v & 0x0F)];
    }
}

size_t make_set_integer (void *dst, const string& ns, const string& sn, const string& bn, uint64_t ki, uint64_t val)
{
    uint64_t fval = htobe64 (val);
    return make_set_any (dst, ns, sn, bn, ki, as_particle::type::t_integer, 8, &fval);
}

size_t make_set_string (void *dst, const string& ns, const string& sn, const string& bn, uint64_t ki, const string& val)
{
    return make_set_any (dst, ns, sn, bn, ki, as_particle::type::t_string, val.length (), val.c_str ());
}


size_t make_set_any (void *dst, const string& ns, const string& sn, const string& bn, uint64_t ki,
		     as_particle::type dt, size_t bs, const void *bd)
{
    uint8_t *p = (uint8_t *)dst;
    size_t sz = sizeof (as_msg) + (sizeof(as_field) * 3) + 20 + ns.length () + sn.length () + sizeof(as_op) + bn.length () + bs;

    as_header *hdr = (as_header *)p;
    hdr->version = 2;
    hdr->type = 3;
    hdr->size (sz);
    p += 8;

    *(as_msg *)p = (as_msg){
	._res0 = 22,
	.flags = AS_MSG_FLAG_WRITE,
	.be_transaction_ttl = htobe32 (1000),
	.be_fields = htobe16 (3),
	.be_ops = htobe16 (1)
    };
    p += 22;

    p += add_as_field (p, as_field::type::t_namespace, ns.length (), ns.c_str ());
    p += add_as_field (p, as_field::type::t_set, sn.length (), sn.c_str ());
    p += add_as_field_hdr (p, as_field::type::t_digest_ripe, CF_SHA_DIGEST_LENGTH);
    p += add_integer_key_digest (p, sn, ki);

    *(as_op *)p = (as_op){
	.be_sz = htobe32 (bn.length () + bs + 4),
	.op_type = as_op::type::t_write,
	.data_type = dt,
	.flags = 0,
	.name_sz = (uint8_t) bn.length ()
    };
    p += sizeof(as_op);

    memcpy (p, bn.c_str (), bn.length ());
    p += bn.length ();

    memcpy (p, bd, bs);
    p += bs;

    size_t ret = p - (uint8_t *)dst;
    dieunless (ret == sz + sizeof(as_header));
    return ret;
}

size_t make_get_bin (void *dst, const string& ns, const string& sn, const string& bn, uint64_t ki)
{
    uint8_t *p = (uint8_t *)dst;
    size_t sz = sizeof (as_msg) + (sizeof(as_field) * 3) + 20 + ns.length () + sn.length () + sizeof(as_op) + bn.length ();

    as_header *hdr = (as_header *)p;
    hdr->version = 2;
    hdr->type = 3;
    hdr->size (sz);
    p += 8;

    *(as_msg *)p = (as_msg){
	._res0 = 22,
	.flags = AS_MSG_FLAG_READ,
	.be_transaction_ttl = htobe32 (1000),
	.be_fields = htobe16 (3),
	.be_ops = htobe16 (1)
    };
    p += 22;

    p += add_as_field (p, as_field::type::t_namespace, ns.length (), ns.c_str ());
    p += add_as_field (p, as_field::type::t_set, sn.length (), sn.c_str ());
    p += add_as_field_hdr (p, as_field::type::t_digest_ripe, CF_SHA_DIGEST_LENGTH);
    p += add_integer_key_digest (p, sn, ki);

    *(as_op *)p = (as_op){
	.be_sz = htobe32 (bn.length () + 4),
	.op_type = as_op::type::t_read,
	.name_sz = (uint8_t) bn.length ()
    };
    p += sizeof(as_op);

    memcpy (p, bn.c_str (), bn.length ());
    p += bn.length ();

    size_t ret = p - (uint8_t *)dst;
    dieunless (ret == sz + sizeof(as_header));
    return ret;
}

size_t make_scan_start (void *dst, const string& ns)
{
    vector<uint16_t> parr (4096);
    iota (parr.begin (), parr.end (), 0);

    uint8_t *p = (uint8_t *)dst;
    size_t sz = sizeof (as_msg) + (sizeof(as_field) * 2) + (parr.size () * 2) + ns.length ();

    as_header *hdr = (as_header *)p;
    hdr->version = 2;
    hdr->type = 3;
    hdr->size (sz);
    p += 8;

    *(as_msg *)p = (as_msg){
	._res0 = 22,
	.flags = AS_MSG_FLAG_READ | AS_MSG_FLAG_PARTITION_DONE,
	.be_fields = htobe16 (2)
    };
    p += 22;

    p += add_as_field (p, as_field::type::t_namespace, ns.length (), ns.c_str ());
    // p += add_as_field (p, as_field::type::t_set, sn.length (), sn.c_str ());
    p += add_as_field (p, as_field::type::t_pid_array, parr.size () * 2, parr.data ());

    size_t ret = p - (uint8_t *)dst;
    dieunless (ret == sz + sizeof(as_header));
    return ret;
}

// struct as_packet
// {
//     as_header proto;
//     as_msg header;
//     uint8_t data[0];
//     as_field *add (as_field::type t, size_t sz);
// } __attribute__((__packed__));
   

// envp is POSIX but not C++
int main (int argc, char **argv, char **envp)
{
    // Defaults for required variables.
    unordered_map<string,string> p = {
	{ "KEYLB",		"1" },
	{ "KEYUB",		"1000000000" },
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

    auto ab = addr_resolve (p["ASDB"]);

    // Make a client fd and connect
    int cfd;
    dieunless ((cfd = socket (AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)) > 0);
    dieunless (connect (cfd, (sockaddr *)ab.data (), ab.size ()) == 0);

    // set-config:context=xdr;dc=dc0;node-adress-port=127.0.0.1:3333;

    char buf[4096];
    // // char *res = (char *)malloc (4096);
    void *res = &(buf[0]);
    auto sz = call (cfd, &res, "node\npartition-generation\nfeatures\npeers-clear-std\npartitions\nreplicas\n");
    printf ("%s<<\n", res);

    char vbuf[1024] = {0};

    size_t bn = make_set_integer (buf, p["NS"], p["SET"], "bin", 100, 100);
    dieunless (write (cfd, buf, bn) == bn);
    char *b = &buf[0];
    bn = get_resp (cfd, &b);
    to_hex (vbuf, (uint8_t *)&buf[0], bn);
    vbuf[2*bn] = 0;
    printf ("%s\n", vbuf);


    bn = make_set_integer (buf, p["NS"], p["SET"], "bin", 200, 200);
    dieunless (write (cfd, buf, bn) == bn);
    b = &buf[0];
    bn = get_resp (cfd, &b);
    to_hex (vbuf, (uint8_t *)&buf[0], bn);
    vbuf[2*bn] = 0;
    printf ("%s\n", vbuf);


    // // bn = make_set_string (buf, p["NS"], p["SET"], "bin", 1005, "a whole new world");
    // // dieunless (write (cfd, buf, bn) == bn);
    // // b = &buf[0];
    // // bn = get_resp (cfd, &b);

    // // to_hex (vbuf, (uint8_t *)&buf[0], bn);
    // // printf ("%s\n", vbuf);

    // for (auto ii=1000; ii < 1006; ii++) {
    // 	bn = make_get_bin (buf, p["NS"], p["SET"], "bin", ii);
    // 	dieunless (write (cfd, buf, bn) == bn);

    // 	b = &buf[0];
    // 	bn = get_resp (cfd, &b);
    // 	to_hex (vbuf, (uint8_t *)&buf[0], bn);
    // 	vbuf[2*bn] = 0;
    // 	printf ("%s\n", vbuf);
    // }

    // printf ("\nscan\n");
    // bn = make_scan_start (&buf[0], p["NS"]);
    // dieunless (write (cfd, buf, bn) == bn);

    // bool gotlast = false;
    // as_msg *m;
    // do {
    // 	b = &buf[0];
    // 	bn = get_resp (cfd, &b);

    // 	to_hex (vbuf, (uint8_t *)&buf[0], bn);
    // 	vbuf[2*bn] = 0;
    // 	printf ("%s\n", vbuf);
    // 	m = (as_msg *)buf;
    // 	gotlast = (m->info3 & AS_MSG_INFO3_LAST);
    // } while (!gotlast);

    close (cfd);
    // free (buf);

    return 0;
}
