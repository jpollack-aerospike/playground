#include "docopt.h"
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <getopt.h>
#include <iomanip>
#include <iostream>
#include <jansson.h>
#include <map>
#include <memory>
#include <openssl/sha.h>
#include <readosm.h>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <string>
#include <sys/time.h>
#include <vector>
#include <nlohmann/json.hpp>

using namespace std;
using json = nlohmann::json;

static const char USAGE[] =
    R"(usage: osm2json [options] INFILE

options:
  -h --help
  --skip_nodes           Skip importing nodes
  --skip_ways            Skip importing ways
)";

bool skip_nodes;
bool skip_ways;
string osmfname;
uint64_t usec_now (void) { return chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count(); }

string readosm_node_to_json_string (const readosm_node *node) {
    assert (node->id && node->latitude && node->longitude);

    stringstream s;

    s.setf (ios::fixed, ios::floatfield);
    s.precision (7);
    s.width (1);

    s << "{";

    if (node->changeset != READOSM_UNDEFINED)	       	
	s << "\"changeset\":" << to_string (node->changeset) << ",";
    
    s << "\"coordinates\":[" << node->latitude << "," << node->longitude << "]"
      << ",\"id\":" << to_string (node->id);
    
    if (node->tag_count) {
	s << ",\"tags\":{" 
	  << json (node->tags->key).dump () << ":" << json (node->tags->value).dump ();
	for (auto ii=1; ii<node->tag_count; ii++)
	    s << "," << json ((node->tags + ii)->key).dump () << ":" << json ((node->tags + ii)->value).dump ();
	s << "}";
    }

    if (node->timestamp != NULL)			s << ",\"timestamp\":\"" << node->timestamp << "\"";
    if (node->uid != READOSM_UNDEFINED)			s << ",\"uid\":" << node->uid;
    if (node->user != NULL)				s << ",\"user\":" << json (node->user).dump ();
    if (node->version != READOSM_UNDEFINED)		s << ",\"version\":" << node->version;

    s << "}";

    return s.str ();
}

string readosm_way_to_json_string (const readosm_way *way) {
    assert (way->id);

    stringstream s;

    s.setf (ios::fixed, ios::floatfield);
    s.precision (7);
    s.width (1);

    s << "{";

    if (way->changeset != READOSM_UNDEFINED)	       	
	s << "\"changeset\":" << to_string (way->changeset) << ",";
    
    s  << "\"id\":" << to_string (way->id);
    
    if (way->node_ref_count) {
	s << ",\"refs\":[" << to_string (way->node_refs[0]);
	for (auto ii=1; ii<way->node_ref_count; ii++)
	    s << "," << to_string (way->node_refs[ii]);
	s << "]";
    }

    if (way->tag_count) {
	s << ",\"tags\":{" 
	  << json (way->tags->key).dump () << ":" << json (way->tags->value).dump ();
	for (auto ii=1; ii<way->tag_count; ii++)
	    s << "," << json ((way->tags + ii)->key).dump () << ":" << json ((way->tags + ii)->value).dump ();
	s << "}";
    }

    if (way->timestamp != NULL)			s << ",\"timestamp\":\"" << way->timestamp << "\"";
    if (way->uid != READOSM_UNDEFINED)			s << ",\"uid\":" << way->uid;
    if (way->user != NULL)				s << ",\"user\":" << json (way->user).dump ();
    if (way->version != READOSM_UNDEFINED)		s << ",\"version\":" << way->version;

    s << "}";

    return s.str ();
}

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

uint64_t num_ways{0}, num_nodes{0};
int on_way (const void *udata, const readosm_way *way) {
    num_ways++;
    std::cout << readosm_way_to_json_string (way) << "\n";
    return READOSM_OK;
}

int on_node (const void *udata, const readosm_node *node) {
    num_nodes++;
    std::cout << readosm_node_to_json_string (node) << "\n";
    return READOSM_OK;
}

void readosm_parsefile (const string& fname, const void *udata)
{
    const void* osm_handle;
    assert (readosm_open (fname.c_str (), &osm_handle) == READOSM_OK);
    assert (readosm_parse (osm_handle, udata, 
			   skip_nodes ? nullptr : on_node, 
			   skip_ways ? nullptr : on_way,
			   nullptr) == READOSM_OK);
    readosm_close (osm_handle);
}

int main (int argc, char **argv) 
{ 
    {
	auto d{docopt::docopt (USAGE, {argv+1, argv+argc})};
	skip_nodes = d["--skip_nodes"].asBool ();
	skip_ways = d["--skip_ways"].asBool ();
	osmfname = d["INFILE"].asString ();
    }

    uint64_t c0, c1;

    auto t0 = usec_now ();
    TSC_MarkStart (c0);
    readosm_parsefile (osmfname, nullptr);
    TSC_MarkStop (c1);
    auto t1 = usec_now ();

    auto tdur = t1 - t0;
    fprintf (stderr, "%lld nodes, %lld ways, %f seconds.\n", num_nodes, num_ways, tdur / 1000000.0);
    
    return 0;
}
