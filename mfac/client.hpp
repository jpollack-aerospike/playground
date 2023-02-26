#pragma once
#include <cstdlib>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>

namespace Aerospike
{

    class Client
    {
    public:
	// Client (ClientPolicy policy, const std::string& hostname, int port);
	Client (const std::string& hostname, int port);
	~Client ();

    private:

    };
}
