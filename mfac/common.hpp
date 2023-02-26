#pragma once
#include "aerospike/aerospike.h"
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_record_iterator.h>
#include <string>
#include <vector>
#include <cassert>

using namespace std;

#define LOG(_fmt, ...) { printf(_fmt "\n", ##__VA_ARGS__); fflush(stdout); }

class StringRecord
{
public:
    StringRecord (const as_record *r)
	: m_asrec (nullptr) { populate (r); }
    ~StringRecord () { populate (nullptr); }
    const string& key () const { return m_key; } 
    const vector<pair<string,string>>& bins () const { return m_bins; }
    uint16_t gen () const { assert (m_asrec); return m_asrec->gen; }
    uint32_t ttl () const { assert (m_asrec); return m_asrec->ttl; }

private:
    void populate (const as_record *r);
    const as_record *m_asrec;
    vector<pair<string,string>> m_bins;
    string m_key;
};

string hexstring (const void *vp, size_t sz);

//void parseArguments (int argc, char **argv, Options& o);
