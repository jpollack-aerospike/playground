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
#include <cassert>
#include <string>
#include <rocksdb/db.h>

using json = nlohmann::json;
using namespace std;
unordered_map<string,string> p;
const char g_ep_str[] = "JP_DORE_";
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
void server_entry (void)
{



}

void backup_entry (void)
{
    int fd;
    auto ab = addr_resolve (p["ASDB"]);
    dieunless ((fd = socket (AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)) > 0);
    dieunless (connect (fd, (sockaddr *)ab.data (), ab.size ()) == 0);

    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, p["ROCKSDB"], &db);
    assert(status.ok());

    scan_start (fd, 0);
    bool last;
    do {
	as_msg *msg = nullptr;
	size_t bn = read (fd, (void **)&msg);
	last = msg->flags & AS_MSG_FLAG_LAST;
	void *op = msg;
	do {
	    size_t msz = msg->end () - (uint8_t *)msg;
	    auto f = msg->field (as_field::type::t_digest_ripe);
	    if (f) {
		string keyd ((const char *)f->data, 20);
		size_t sz = (msg->end () - (uint8_t *)msg->ops_begin ());
		string bins ((const char *)msg->ops_begin (), sz);
		rocksdb::Status status = db->Put(rocksdb::WriteOptions(), keyd, bins);
		assert(status.ok());
	    }
	    msg = (as_msg *)msg->end ();
	    bn -= msz;
	} while (bn);

	free (op);
    } while (!last);

    close (fd);
}

void restore_entry (void)
{
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = false;
    rocksdb::Status status = rocksdb::DB::Open(options, p["ROCKSDB"], &db);
    assert(status.ok());
    
    int fd;
    auto ab = addr_resolve (p["ASDB"]);
    dieunless ((fd = socket (AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)) > 0);
    dieunless (connect (fd, (sockaddr *)ab.data (), ab.size ()) == 0);

    char buf[1048576]; // maxrecsize
    as_msg *msg = (as_msg *)(buf + 8);
    msg->clear ();
    msg->flags = AS_MSG_FLAG_WRITE;
    msg->be_transaction_ttl = htobe32 (1000);
    dieunless (msg->add (as_field::type::t_namespace, "ns0"));
    dieunless (msg->add (as_field::type::t_set, "str"));
    auto df = msg->add (as_field::type::t_digest_ripe, 20);
    
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    as_op *ob = msg->ops_begin ();
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
	if (it->key ().size () != 20) {
	    continue;
	}
	memcpy (&(df->data[0]), it->key ().data (), 20);
	size_t bsz = it->value ().size ();
	memcpy (ob, it->value ().data (), bsz);
	int nop = 0;
	for (as_op *op = ob; ((uint8_t *)op - (uint8_t *)ob) < bsz; op = op->next ()) {
	    op->op_type = as_op::type::t_write;
	    nop++;
	}
	msg->be_ops = htobe16 (nop);
	as_msg *resp = nullptr;
	size_t bn = call (fd, (void **)&resp, msg);
	dieunless (resp->result_code == 0);
	free (resp);
    }
    assert(it->status().ok()); // Check for any errors found during the scan
    delete it;    
}
void display_entry (void)
{
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = false;
    rocksdb::Status status = rocksdb::DB::Open(options, p["ROCKSDB"], &db);
    assert(status.ok());
    
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
	cout << it->key().ToString(true) << ": " << it->value().ToString(true) << endl;
    }
    assert(it->status().ok()); // Check for any errors found during the scan
    delete it;    
}

int main(int argc, char** argv, char** envp) 
{
    dieunless (argc >= 2);
    p = {
	{ "ASDB",		"localhost:3000" },
	{ "NS",			"ns0" },
	{ "ROCKSDB",		"/tmp/testdb" }
    };
    // Override from environment
    for (auto ep = *envp; ep; ep = *(++envp))
	if (!strncmp (g_ep_str, ep, g_ep_str_sz)) {
	    auto vs = strchr (ep, '=');
	    auto ks = string (ep).substr (g_ep_str_sz, (vs - ep) - g_ep_str_sz);
	    if (ks.length ()) p[ks] = string (vs + 1);
	}

    // Make a client fd and connect
    string cmd (argv[1]);
    
    if (!cmd.compare ("backup")) {			backup_entry ();
    } else if (!cmd.compare ("restore")) {		restore_entry ();
    } else if (!cmd.compare ("display")) {		display_entry ();
    } else if (!cmd.compare ("server")) {		server_entry ();
    } else {
	fprintf (stderr, "usage:\n %s init|update\n", argv[0]);
    }
    
    return 0;
}
