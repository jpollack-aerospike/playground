#include <string>
#include <map>
#include <algorithm>
#include <cstdio>
#include <cassert>
#include <cstring>
#include <getopt.h>
#include <iomanip>
#include <iostream>
#include <jansson.h>
#include <memory>
#include <map>
#include <chrono>
#include <openssl/sha.h>
#include <stdexcept>
#include <stdint.h>
#include <sys/time.h>
#include <vector>
#include "docopt.h"
#include <osmium/io/reader_with_progress_bar.hpp>
#include <osmium/util/file.hpp>
#include <osmium/util/progress_bar.hpp>
#include <osmium/handler.hpp>
#include <osmium/io/pbf_input.hpp>
#include <osmium/visitor.hpp>

using namespace std;

static const char USAGE[] =
    R"(usage: osm_test [options] INFILE

options:
  -h --help
  --asdb=SOCKADDR        Aerospike Server [default: 127.0.0.1:3000]
  --ns=NAMESPACE         Namespace [default: test]
  --skip_nodes           Skip importing nodes
  --skip_ways            Skip importing ways
  --skip_relations       Skip importing relations
)";

bool skip_nodes;
bool skip_ways;
bool skip_relations;
uint16_t portno;
string hostname;
string ns;
string osmfname;
uint64_t num_nodes{0}, num_ways{0}, num_relations{0};

uint64_t usec_now (void) { return chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count(); }

inline void TSC_MarkStart (uint64_t &ret) // Takes ~220 cycles
{
    __asm__ __volatile__ ("cpuid\n\t"
                          "rdtsc\n\t"
                          "salq $32, %%rdx\n\t"
                          "addq %%rdx, %%rax\n\t"
                          : "=a" (ret) : : "%rbx", "%rcx", "%rdx" );
}
                  

inline void TSC_MarkStop (uint64_t &ret) // Takes ~250 cycles
{
    __asm__ __volatile__ ("rdtscp\n\t"
                          "salq $32, %%rdx\n\t"
                          "addq %%rdx, %%rax\n\t"
                          "movq %%rax, %0\n\t"
                          "cpuid\n\t" : "=r" (ret) : : "%rax", "%rbx", "%rcx", "%rdx" );
}

struct LoadHandler : public osmium::handler::Handler {

    void node(const osmium::Node &node) {
	num_nodes++;
    }
    void way(const osmium::Way &way) {
	num_ways++;
    }
    void relation(const osmium::Relation &relation) {
	num_relations++;
    }

};

int main (int argc, char **argv) 
{ 
    LoadHandler handler;
    {
	auto d{docopt::docopt (USAGE, {argv+1, argv+argc})};
	skip_nodes = d["--skip_nodes"].asBool ();
	skip_ways = d["--skip_ways"].asBool ();
	skip_relations = d["--skip_relations"].asBool ();
	portno = 3000;
	hostname = d["--asdb"].asString();
	ns = d["--ns"].asString ();
	size_t cpos = hostname.find (':');
	if (cpos != string::npos) {
	    portno = stoi (hostname.substr (cpos+1));
	    hostname = hostname.substr (0, cpos);
	}
	osmfname = d["INFILE"].asString ();
    }

    uint64_t c0, c1;

    auto t0 = usec_now ();
    TSC_MarkStart (c0);
    {
	osmium::io::File infile{osmfname};
	osmium::io::ReaderWithProgressBar reader{osmium::isatty(2), infile};
	osmium::apply (reader, handler);
	reader.close ();
    }

    TSC_MarkStop (c1);
    auto t1 = usec_now ();
    auto tdur = t1 - t0;
    fprintf (stderr, "%lld nodes, %lld ways, %lld relations, %f seconds.\n", num_nodes, num_ways, num_relations, tdur / 1000000.0);

    return 0;
}
