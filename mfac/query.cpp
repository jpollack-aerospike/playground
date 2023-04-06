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
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <iostream>
#include <fstream>

using namespace std;
using json = jsoncons::json;

inline void __dieunless (const char *msg, const char *file, int line) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", file, line, msg); abort (); }

static uint64_t usec_now (void) { return chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count(); }
static const char USAGE[] =
    R"(usage: query [options] QUERY ...

options:
  -h --help

)";


int main (int argc, char **argv) 
{ 
    auto d{docopt::docopt (USAGE,{argv+1,argv+argc})};
    vector<string> queries (d["QUERY"].asStringList ());
    string line;

    while (getline (std::cin, line)) {
	json j = json::parse (line);
	for (const auto& q: queries) 	    std::cout << jsoncons::jsonpath::json_query (j, q) << "\t";
	std::cout << "\n";
    }

	
    return 0;
}
