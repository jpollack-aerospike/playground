#include "simple_string.hpp"
#include "as_proto.hpp"
#include "util.hpp"
#include <cstring>
#include <tuple>
#include <array>
#include <nlohmann/json.hpp>
#include <vector>
#include <thread>

using json = nlohmann::json;

int str_set (int fd, uint64_t mrtid, int64_t id, const std::string& str, uint32_t gen, uint32_t deadline)
{
    char buf[1024];
    as_msg *msg = (as_msg *)(buf + 8);

    int64_t tmp0 = 0;
    msg->clear ();
    msg->flags = AS_MSG_FLAG_WRITE;
    msg->be_transaction_ttl = htobe32 (1000);
    if (gen) {
	msg->be_generation = gen;
	msg->flags |= AS_MSG_FLAG_GENERATION;
    }
    
    dieunless (msg->add (as_field::type::t_namespace, "ns0"));
    dieunless (msg->add (as_field::type::t_set, "str"));
    add_integer_key_digest (msg->add (as_field::type::t_digest_ripe, CF_SHA_DIGEST_LENGTH)->data, "str", id);
    if (mrtid) {
	dieunless (msg->add (as_field::type::t_mrtid, 8, &mrtid));
    }
    if (deadline) {
	dieunless (msg->add (as_field::type::t_mrt_deadline, 4, &deadline));
    }

    dieunless (msg->add (as_op::type::t_write, "str", str.length (), str.c_str (), as_particle::type::t_string));

    size_t bn = call (fd, (void **)&msg, msg);
    return msg->result_code;
}

int str_get (int fd, uint64_t mrtid, int64_t id, std::string& str, uint32_t *gen, void *recver)
{
    char buf[1024];
    as_msg *msg = (as_msg *)(buf + 8);
    char buf1[1024];
    as_msg *msg1 = (as_msg *)(buf1 + 8);
    
    int64_t tmp0 = 0;
    msg->clear ();
    msg->flags = AS_MSG_FLAG_READ;
    msg->be_transaction_ttl = htobe32 (1000);
    dieunless (msg->add (as_field::type::t_namespace, "ns0"));
    dieunless (msg->add (as_field::type::t_set, "str"));
    add_integer_key_digest (msg->add (as_field::type::t_digest_ripe, CF_SHA_DIGEST_LENGTH)->data, "str", id);
    if (mrtid) {
	dieunless (msg->add (as_field::type::t_mrtid, 8, &mrtid));
    }

    dieunless (msg->add (as_op::type::t_read, "str", 0));
    
    size_t bn = call (fd, (void **)&msg1, msg);
    if (msg1->result_code != 0) {
	return msg1->result_code;
    }
    
    if (gen) {
	*gen = msg1->be_generation;
    }
    
    as_op *op = msg1->ops_begin ();
    for (auto ii = msg1->n_ops (); ii--; op = op->next ()) {
	if (!memcmp (op->name, "str", 3)) {
	    str.clear ();
	    str.assign ((char *)op->data (), op->data_sz ());
	} else {
	    printf ("unexpected bin %s on response\n", op->name);
	}
    }

    if (mrtid) {
	if (recver && msg1->field (as_field::type::t_record_version)) {
	    auto vf = msg1->field (as_field::type::t_record_version);
	    memcpy (recver, vf->data, vf->data_sz ());
	}
    }
    
    return msg1->result_code;

}

int int_set (int fd, uint64_t mrtid, int64_t id, int64_t val, uint32_t gen, uint32_t deadline)
{
    char buf[1024];
    as_msg *msg = (as_msg *)(buf + 8);

    int64_t tmp0 = 0;
    msg->clear ();
    msg->flags = AS_MSG_FLAG_WRITE;
    msg->be_transaction_ttl = htobe32 (1000);
    if (gen) {
	msg->be_generation = gen;
	msg->flags |= AS_MSG_FLAG_GENERATION;
    }
    
    dieunless (msg->add (as_field::type::t_namespace, "ns0"));
    dieunless (msg->add (as_field::type::t_set, "str"));
    add_integer_key_digest (msg->add (as_field::type::t_digest_ripe, CF_SHA_DIGEST_LENGTH)->data, "str", id);
    if (mrtid) {
	dieunless (msg->add (as_field::type::t_mrtid, 8, &mrtid));
    }
    if (deadline) {
	dieunless (msg->add (as_field::type::t_mrt_deadline, 4, &deadline));
    }

    tmp0 = htobe64 (val);
    dieunless (msg->add (as_op::type::t_write, "bal", 8, &tmp0, as_particle::type::t_integer));

    size_t bn = call (fd, (void **)&msg, msg);
    return msg->result_code;
}

int int_get (int fd, uint64_t mrtid, int64_t id, int64_t& val, uint32_t *gen, void *recver)
{
    char buf[1024];
    as_msg *msg = (as_msg *)(buf + 8);
    char buf1[1024];
    as_msg *msg1 = (as_msg *)(buf1 + 8);
    
    int64_t tmp0 = 0;
    msg->clear ();
    msg->flags = AS_MSG_FLAG_READ;
    msg->be_transaction_ttl = htobe32 (1000);
    dieunless (msg->add (as_field::type::t_namespace, "ns0"));
    dieunless (msg->add (as_field::type::t_set, "str"));
    add_integer_key_digest (msg->add (as_field::type::t_digest_ripe, CF_SHA_DIGEST_LENGTH)->data, "str", id);
    if (mrtid) {
	dieunless (msg->add (as_field::type::t_mrtid, 8, &mrtid));
    }

    dieunless (msg->add (as_op::type::t_read, "bal", 0));
    
    size_t bn = call (fd, (void **)&msg1, msg);
    if (msg1->result_code != 0) {
	return msg1->result_code;
    }
    
    if (gen) {
	*gen = msg1->be_generation;
    }
    
    as_op *op = msg1->ops_begin ();
    for (auto ii = msg1->n_ops (); ii--; op = op->next ()) {
	if (!memcmp (op->name, "bal", 3)) {
	    val = be64toh (*(int64_t *)op->data ());
	} else {
	    printf ("unexpected bin %s on response\n", op->name);
	}
    }

    if (mrtid) {
	if (recver && msg1->field (as_field::type::t_record_version)) {
	    auto vf = msg1->field (as_field::type::t_record_version);
	    memcpy (recver, vf->data, vf->data_sz ());
	}
    }
    
    return msg1->result_code;

}

int str_debug (int fd, int64_t id, std::string& str)
{
    std::array<uint8_t,20> keyd;
    add_integer_key_digest (keyd.data (), "str", id);
    
    char buf[] = "debug-record:namespace=ns0;keyd=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX;mode=raw\n";
    to_hex (buf+32, keyd.data (), 20);

    size_t sz = call_info (fd, str, buf);
    str[sz-1] = '\0';
    
    size_t tc = str.find ('\t');
    dieunless (tc != std::string::npos);
    str = str.substr (tc+1, std::string::npos);
    return 0;
}
bool str_truncate (int fd)
{
    char buf[64] = {0};
    char *bp = buf;
    size_t bn = call (fd, (void **) &bp, "truncate:namespace=ns0;set=str;\n");
    bp[bn-1] = 0;
    
    char *tc = strchr (bp, '\t');
    dieunless (tc);
    tc++;

    return (memcmp ("ok", tc, 2) == 0);
}


int str_verify (int fd, int64_t id, void *recver)
{
    char buf0[1024];
    as_msg *msg0 = (as_msg *)(buf0 + 8);
    
    msg0->clear ();
    msg0->flags = AS_MSG_FLAG_READ | AS_MSG_FLAG_MRT_VERIFY_READ | AS_MSG_FLAG_SC_READ_TYPE; // linearized mrt verify

    dieunless (msg0->add (as_field::type::t_namespace, "ns0"));
    dieunless (msg0->add (as_field::type::t_set, "str"));
    add_integer_key_digest (msg0->add (as_field::type::t_digest_ripe, CF_SHA_DIGEST_LENGTH)->data, "str", id);
    dieunless (msg0->add (as_field::type::t_record_version, 7, recver));

    size_t bn = call (fd, (void **)&msg0, msg0);
    return msg0->result_code;
}

int str_roll (int fd, uint64_t mrtid, int64_t id, bool is_fwd)
{
    char buf0[1024];
    as_msg *msg0 = (as_msg *)(buf0 + 8);
    
    msg0->clear ();
    msg0->flags = AS_MSG_FLAG_WRITE | (is_fwd ? AS_MSG_FLAG_MRT_ROLL_FORWARD : AS_MSG_FLAG_MRT_ROLL_BACK);
    
    dieunless (msg0->add (as_field::type::t_namespace, "ns0"));
    dieunless (msg0->add (as_field::type::t_set, "str"));
    add_integer_key_digest (msg0->add (as_field::type::t_digest_ripe, CF_SHA_DIGEST_LENGTH)->data, "str", id);
    dieunless (msg0->add (as_field::type::t_mrtid, 8, &mrtid));

    size_t bn = call (fd, (void **)&msg0, msg0);
    return msg0->result_code;
}

int str_delete (int fd, uint64_t mrtid, int64_t id, uint32_t deadline)
{
    char buf0[1024];
    as_msg *msg0 = (as_msg *)(buf0 + 8);
    
    msg0->clear ();
    msg0->flags = AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE | AS_MSG_FLAG_DURABLE_DELETE;
    
    dieunless (msg0->add (as_field::type::t_namespace, "ns0"));
    add_integer_key_digest (msg0->add (as_field::type::t_digest_ripe, CF_SHA_DIGEST_LENGTH)->data, "str", id);
    if (mrtid) {
	dieunless (msg0->add (as_field::type::t_mrtid, 8, &mrtid));
    }
    if (deadline) {
	dieunless (msg0->add (as_field::type::t_mrt_deadline, 4, &deadline));
    }
    size_t bn = call (fd, (void **)&msg0, msg0);
    return msg0->result_code;
}

int str_insert (int fd, uint64_t mrtid, int64_t id, const std::string& str, uint32_t gen, uint32_t deadline)
{
    char buf[1024];
    as_msg *msg = (as_msg *)(buf + 8);

    int64_t tmp0 = 0;
    msg->clear ();
    msg->flags = AS_MSG_FLAG_WRITE;
    msg->be_transaction_ttl = htobe32 (1000);
    if (gen) {
	msg->be_generation = gen;
	msg->flags |= AS_MSG_FLAG_GENERATION;
    }
    
    dieunless (msg->add (as_field::type::t_namespace, "ns0"));
    dieunless (msg->add (as_field::type::t_set, "str"));
    add_integer_key_digest (msg->add (as_field::type::t_digest_ripe, CF_SHA_DIGEST_LENGTH)->data, "str", id);
    if (mrtid) {
	dieunless (msg->add (as_field::type::t_mrtid, 8, &mrtid));
    }
    if (deadline) {
	dieunless (msg->add (as_field::type::t_mrt_deadline, 4, &deadline));
    }
    auto pload = json::to_msgpack ({0x01, str, 0x01, 0x01}); //list_append, <val>, ordered, add_unique
    dieunless (msg->add (as_op::type::t_cdt_modify, "list", pload.size (), pload.data (), as_particle::type::t_blob));
    
    size_t bn = call (fd, (void **)&msg, msg);
    return msg->result_code;
}

// ==============================================================================

int txn_add_str (int fd, uint64_t mrtid, int64_t id, uint32_t *deadline = nullptr)
{
    char buf0[1024];
    as_msg *msg0 = (as_msg *)(buf0 + 8);
    
    char buf1[1024];
    as_msg *msg1 = (as_msg *)(buf1 + 8);

    msg0->clear ();
    msg0->flags = AS_MSG_FLAG_WRITE;
    msg0->be_transaction_ttl = htobe32 (1000);
    dieunless (msg0->add (as_field::type::t_namespace, "ns0"));

    dieunless (msg0->add (as_field::type::t_set, MONITOR_SET_NAME));
    add_integer_key_digest (msg0->add (as_field::type::t_digest_ripe, CF_SHA_DIGEST_LENGTH)->data, MONITOR_SET_NAME, mrtid);

    if (deadline && (!*deadline)) {
	uint64_t tmp0 = htobe64 (mrtid);
	dieunless (msg0->add (as_op::type::t_write, "id", sizeof(int64_t), &tmp0, as_particle::type::t_integer));
    }
    std::vector<uint8_t> dg (20);
    add_integer_key_digest (dg.data (), "str", id);
    auto op = msg0->add (as_op::type::t_cdt_modify, "keyds", 0);

    auto pl = json::to_msgpack ({0x01, json::binary (dg), 0x01, 0x01});
    memcpy (op->data (), pl.data (), pl.size ());
    op->data_sz (pl.size ());
    
    size_t bn = call (fd, (void **)&msg1, msg0);
    if (deadline && (!*deadline)) {
	auto df = msg1->field (as_field::type::t_mrt_deadline);
	if (!msg1->result_code)	    
	    dieunless (df);
	if (df)	    
	    memcpy (deadline, df->data, df->data_sz ());
    }
    // printf ("%ld\tadd txn: 0x%016llx\tid: %ld\trc: %ld\n", std::this_thread::get_id (), mrtid, id, msg1->result_code);
    return msg1->result_code;
}

int txn_fwd (int fd, uint64_t mrtid)
{
    char buf0[1024];
    as_msg *msg0 = (as_msg *)(buf0 + 8);
    
    msg0->clear ();
    msg0->flags = AS_MSG_FLAG_WRITE;
    msg0->be_transaction_ttl = htobe32 (1000);
    dieunless (msg0->add (as_field::type::t_namespace, "ns0"));
    dieunless (msg0->add (as_field::type::t_set, MONITOR_SET_NAME));
    add_integer_key_digest (msg0->add (as_field::type::t_digest_ripe, CF_SHA_DIGEST_LENGTH)->data, MONITOR_SET_NAME, mrtid);

    bool fwd = true;
    dieunless (msg0->add (as_op::type::t_write, "fwd", 1, &fwd, as_particle::type::t_boolean));
    
    size_t bn = call (fd, (void **)&msg0, msg0);
    // printf ("%ld\tfwd txn: 0x%016llx\trc: %ld\n", std::this_thread::get_id (), mrtid, msg0->result_code);
    return msg0->result_code;
}

int txn_delete (int fd, uint64_t mrtid)
{
    char buf0[1024];
    as_msg *msg0 = (as_msg *)(buf0 + 8);
    
    msg0->clear ();
    msg0->flags = AS_MSG_FLAG_WRITE | AS_MSG_FLAG_DELETE | AS_MSG_FLAG_DURABLE_DELETE;
    msg0->be_transaction_ttl = htobe32 (1000);
    dieunless (msg0->add (as_field::type::t_namespace, "ns0"));
    dieunless (msg0->add (as_field::type::t_set, MONITOR_SET_NAME));
    add_integer_key_digest (msg0->add (as_field::type::t_digest_ripe, CF_SHA_DIGEST_LENGTH)->data, MONITOR_SET_NAME, mrtid);

    size_t bn = call (fd, (void **)&msg0, msg0);
    // printf ("%ld\tdel txn: 0x%016llx\trc: %ld\n", std::this_thread::get_id (), mrtid, msg0->result_code);
    return msg0->result_code;
}
