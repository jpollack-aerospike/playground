#pragma once

#include "as_proto.hpp"
#include <map>
#include <array>

struct transaction
{
public:
    transaction ();
    ~transaction ();
    int set (int fd, int64_t keyid, const std::string& val);
    int get (int fd, int64_t keyid, std::string& val);
    void finish (int fd);
    bool commit (int fd);
private:
    void clear (void);
    struct recstate
    {
	uint32_t gen;
	std::array<uint8_t,7> ver;
	bool isRead;
    };
    uint64_t id;
    uint32_t deadline;
    bool rdir;
    bool hasmon;
    std::map<int64_t, recstate> m_map;
};
