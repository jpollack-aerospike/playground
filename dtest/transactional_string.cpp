#include "transactional_string.hpp"
#include "util.hpp"
#include <cstring>
#include "simple_string.hpp"
#include <type_traits>
#include <cstdint>
#include <random>

transaction::transaction (){    clear ();}
transaction::~transaction () {}


void transaction::clear (void)
{
    thread_local static std::random_device rd;
    thread_local static std::mt19937 gen(rd());

    std::uniform_int_distribution<uint64_t> dist(std::numeric_limits<std::uint64_t>::min(),
						 std::numeric_limits<std::uint64_t>::max());
    this->id = dist (gen);
    this->deadline = 0;
    this->rdir = false;
    this->hasmon = false;
    this->m_map.clear ();
}

int transaction::set (int fd, int64_t keyid, const std::string& val) 
{
    recstate rs;
    rs.isRead = false;
    auto f = m_map.find (keyid);
    if (f != m_map.end ())
	memcpy (&rs, &f->second, sizeof(recstate));

    if ((f == m_map.end ()) || rs.isRead) {
	int rc = txn_add_str (fd, this->id, keyid, &this->deadline);
	if (rc) return rc;
	this->hasmon = true;
    }
    
    int rc = str_set (fd, this->id, keyid, val, rs.isRead ? rs.gen : 0, this->deadline);
    rs.isRead = false;
    m_map[keyid] = rs;

    if (rc) return rc;
    return 0;
}

int transaction::get (int fd, int64_t keyid, std::string& val) 
{
    recstate rs;
    rs.isRead = true;
    auto f = m_map.find (keyid);
    if (f != m_map.end ()) { // 
	memcpy (&rs, &f->second, sizeof(recstate));
    }
    int rc = str_get (fd, this->id, keyid, val, &rs.gen, &rs.ver);
    if (rc) return rc;

    m_map[keyid] = rs;
    return 0;
}

void transaction::finish (int fd)
{
    bool useMonitor = false;
    for (auto& [k, v] : this->m_map) {
	if (!v.isRead) {
	    dieunless (str_roll (fd, this->id, k, this->rdir) == 0);
	    useMonitor = true;
	}
    }

    if (this->hasmon) {
	auto rc = txn_delete (fd, this->id);
	if (rc != 0) {
	    fprintf (stderr, "Failed to delete monitor with code: %d\n", rc);
	    dieunless (false);
	}
    }
    clear ();
}

bool transaction::commit (int fd)
{
    if (this->rdir)
	return true;

    bool useMonitor = false;
    for (auto& [k, v] : this->m_map) {
	if (v.isRead) {  // verify
	    if (str_verify (fd, k, &v.ver) != 0) {
		return false;
	    }
	} else {
	    useMonitor = true;
	}
    }
    if (useMonitor) {
	dieunless (txn_fwd (fd, this->id) == 0);
	this->rdir = true;
    }
    return true;
}
