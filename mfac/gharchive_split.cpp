#include "docopt.h"
#include <algorithm>
#include <array>
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
#include <unordered_map>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>

using namespace std;
using json = nlohmann::json;

inline void __dieunless (const char *msg, const char *file, int line) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", file, line, msg); abort (); }

static uint64_t usec_now (void) { return chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count(); }
static const char USAGE[] =
    R"(usage: gharchive_split [options]

options:
  -h --help
  --prefix=STRING        Prefix [default: type_]

)";


int main (int argc, char **argv)
{
    auto d{docopt::docopt (USAGE,{argv+1,argv+argc})};
    string opf = d["--prefix"].asString ();

    unordered_map<string, ofstream*> fhm;

    string line;
    ofstream *osp{nullptr};

    uint64_t tstart = usec_now ();
    uint64_t tlast = tstart;
    uint64_t lineno = 0;
    uint64_t btot = 0;
    while (getline (std::cin, line)) {
	json j = json::parse (line);
	const string& tstr = j["type"];
	auto s = fhm.find (tstr);
	if (s == fhm.end ()) {
	    fhm.emplace (tstr, osp = (new ofstream(opf + tstr + ".ndjson")));
	} else {
	    osp = s->second;
	}
	*osp << line << "\n";
	if (!(++lineno & 0xFF) && (usec_now () >= (tlast + 1000000)))
	    printf ("{\"lineno\":%llu,\"time\":%llu,\"bytes\":%llu}\n", lineno, (tlast = usec_now ()) - tstart, btot);
	btot += line.size ();
    }
    printf ("{\"lineno\":%llu,\"time\":%llu,\"bytes\":%llu}\n", lineno, (tlast = usec_now ()) - tstart, btot);

    for (auto& [k, v]: fhm)	v->close ();

    return 0;
}
