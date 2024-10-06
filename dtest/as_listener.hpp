#pragma once
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "as_proto.hpp"
#include <thread>
#include <functional>

using InfoHandler = std::function<void(std::string&,const std::string&)>;
using MsgHandler = std::function<size_t(as_msg **, const as_msg *)>;

class as_listener
{
public:
    as_listener ();
    ~as_listener ();
    void start (const std::string& hostaddr);
    void stop ();
    InfoHandler onInfo;
    MsgHandler onMsg;
private:
    bool m_started;
    int m_sfd;
    std::thread *m_mainThread;
    size_t on_cmd (as_msg **dst, const as_msg *src, size_t sz);
    size_t on_info_msg (uint8_t **dst, char *src, size_t sz);
    std::string on_info (const std::string& icmd);
    size_t on_all_msg (uint8_t **dst, uint8_t type, uint8_t *src, size_t sz);
    void accept_entry ();
    void conn_entry (int connfd);
    std::unordered_set<int> m_conns;
    std::unordered_map<std::string,std::string> m_sinfo;
};

