#include "as_listener.hpp"

#include "util.hpp"
#include <iostream>
#include <nlohmann/json.hpp>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <netinet/in.h>

using json = nlohmann::json;
using namespace std;

as_listener::as_listener ()
    : onInfo (nullptr),
      onMsg (nullptr),
      m_started (false),
      m_sfd (-1),
      m_mainThread (nullptr)
{
    m_sinfo["features"] = "batch-any;batch-index;blob-bits;cdt-index;cdt-list;cdt-map;cluster-stable;float;geo;sindex-exists;peers;pipelining;pquery;pscans;query-show;relaxed-sc;replicas;replicas-all;replicas-master;replicas-max;truncate-namespace;udf";
    m_sinfo["node"] = "DEADBEEFBADC0FE";
    //    BB9C5F8FAA11BDC
    //     bb9c5f8faa11bdc
    m_sinfo["partition-generation"] = "1";
    m_sinfo["partitions"] = "4096";
    m_sinfo["peers-clear-std"] = "0,3000,[]";
    m_sinfo["peers-generation"] = "0";
    m_sinfo["replicas"] = "ns0:1,1,//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////8=";
    m_sinfo["replicas:max=1"] = m_sinfo["replicas"];
    m_sinfo["status"] = "ok";
    m_sinfo["namespace/ns0"] = "ns_cluster_size=1;effective_replication_factor=1;objects=2;tombstones=0;xdr_tombstones=0;xdr_bin_cemeteries=0;mrt_provisionals=0;master_objects=2;master_tombstones=0;prole_objects=0;prole_tombstones=0;non_replica_objects=0;non_replica_tombstones=0;unreplicated_records=0;dead_partitions=0;unavailable_partitions=0;auto_revived_partitions=0;clock_skew_stop_writes=false;stop_writes=false;hwm_breached=false;current_time=463372560;non_expirable_objects=0;expired_objects=0;evicted_objects=0;evict_ttl=0;evict_void_time=0;smd_evict_void_time=0;nsup_cycle_duration=0;nsup_cycle_deleted_pct=0.00;truncate_lut=0;truncating=false;sindex_gc_cleaned=0;xmem_id=1;index_used_bytes=128;set_index_used_bytes=0;sindex_used_bytes=0;data_total_bytes=1073741824;data_used_bytes=128;data_used_pct=0;data_avail_pct=98;data_compression_ratio=1.000;storage-engine.stripe[0].used_bytes=128;storage-engine.stripe[0].free_wblocks=126;storage-engine.stripe[0].writes=0;storage-engine.stripe[0].defrag_q=0;storage-engine.stripe[0].defrag_reads=1;storage-engine.stripe[0].defrag_writes=0;storage-engine.stripe[0].backing_write_q=0;storage-engine.stripe[0].partial_writes=0;storage-engine.stripe[0].defrag_partial_writes=1;storage-engine.stripe[0].age=-1;record_proto_uncompressed_pct=0.000;record_proto_compression_ratio=1.000;query_proto_uncompressed_pct=0.000;query_proto_compression_ratio=1.000;pending_quiesce=false;effective_is_quiesced=false;nodes_quiesced=0;effective_prefer_uniform_balance=true;effective_active_rack=0;migrate_tx_partitions_imbalance=0;migrate_tx_instances=0;migrate_rx_instances=0;migrate_tx_partitions_active=0;migrate_rx_partitions_active=0;migrate_tx_partitions_initial=0;migrate_tx_partitions_remaining=0;migrate_tx_partitions_lead_remaining=0;migrate_rx_partitions_initial=0;migrate_rx_partitions_remaining=0;migrate_records_skipped=0;migrate_records_transmitted=0;migrate_record_retransmits=0;migrate_record_receives=0;migrate_signals_active=0;migrate_signals_remaining=0;migrate_fresh_partitions=0;appeals_tx_active=0;appeals_rx_active=0;appeals_tx_remaining=0;appeals_records_exonerated=0;client_tsvc_error=0;client_tsvc_timeout=0;client_proxy_complete=0;client_proxy_error=0;client_proxy_timeout=0;client_read_success=0;client_read_error=0;client_read_timeout=0;client_read_not_found=0;client_read_filtered_out=0;client_write_success=0;client_write_error=0;client_write_timeout=0;client_write_filtered_out=0;xdr_client_write_success=0;xdr_client_write_error=0;xdr_client_write_timeout=0;client_delete_success=0;client_delete_error=0;client_delete_timeout=0;client_delete_not_found=0;client_delete_filtered_out=0;xdr_client_delete_success=0;xdr_client_delete_error=0;xdr_client_delete_timeout=0;xdr_client_delete_not_found=0;client_udf_complete=0;client_udf_error=0;client_udf_timeout=0;client_udf_filtered_out=0;client_lang_read_success=0;client_lang_write_success=0;client_lang_delete_success=0;client_lang_error=0;from_proxy_tsvc_error=0;from_proxy_tsvc_timeout=0;from_proxy_read_success=0;from_proxy_read_error=0;from_proxy_read_timeout=0;from_proxy_read_not_found=0;from_proxy_read_filtered_out=0;from_proxy_write_success=0;from_proxy_write_error=0;from_proxy_write_timeout=0;from_proxy_write_filtered_out=0;xdr_from_proxy_write_success=0;xdr_from_proxy_write_error=0;xdr_from_proxy_write_timeout=0;from_proxy_delete_success=0;from_proxy_delete_error=0;from_proxy_delete_timeout=0;from_proxy_delete_not_found=0;from_proxy_delete_filtered_out=0;xdr_from_proxy_delete_success=0;xdr_from_proxy_delete_error=0;xdr_from_proxy_delete_timeout=0;xdr_from_proxy_delete_not_found=0;from_proxy_udf_complete=0;from_proxy_udf_error=0;from_proxy_udf_timeout=0;from_proxy_udf_filtered_out=0;from_proxy_lang_read_success=0;from_proxy_lang_write_success=0;from_proxy_lang_delete_success=0;from_proxy_lang_error=0;batch_sub_tsvc_error=0;batch_sub_tsvc_timeout=0;batch_sub_proxy_complete=0;batch_sub_proxy_error=0;batch_sub_proxy_timeout=0;batch_sub_read_success=0;batch_sub_read_error=0;batch_sub_read_timeout=0;batch_sub_read_not_found=0;batch_sub_read_filtered_out=0;batch_sub_write_success=0;batch_sub_write_error=0;batch_sub_write_timeout=0;batch_sub_write_filtered_out=0;batch_sub_delete_success=0;batch_sub_delete_error=0;batch_sub_delete_timeout=0;batch_sub_delete_not_found=0;batch_sub_delete_filtered_out=0;batch_sub_udf_complete=0;batch_sub_udf_error=0;batch_sub_udf_timeout=0;batch_sub_udf_filtered_out=0;batch_sub_lang_read_success=0;batch_sub_lang_write_success=0;batch_sub_lang_delete_success=0;batch_sub_lang_error=0;from_proxy_batch_sub_tsvc_error=0;from_proxy_batch_sub_tsvc_timeout=0;from_proxy_batch_sub_read_success=0;from_proxy_batch_sub_read_error=0;from_proxy_batch_sub_read_timeout=0;from_proxy_batch_sub_read_not_found=0;from_proxy_batch_sub_read_filtered_out=0;from_proxy_batch_sub_write_success=0;from_proxy_batch_sub_write_error=0;from_proxy_batch_sub_write_timeout=0;from_proxy_batch_sub_write_filtered_out=0;from_proxy_batch_sub_delete_success=0;from_proxy_batch_sub_delete_error=0;from_proxy_batch_sub_delete_timeout=0;from_proxy_batch_sub_delete_not_found=0;from_proxy_batch_sub_delete_filtered_out=0;from_proxy_batch_sub_udf_complete=0;from_proxy_batch_sub_udf_error=0;from_proxy_batch_sub_udf_timeout=0;from_proxy_batch_sub_udf_filtered_out=0;from_proxy_batch_sub_lang_read_success=0;from_proxy_batch_sub_lang_write_success=0;from_proxy_batch_sub_lang_delete_success=0;from_proxy_batch_sub_lang_error=0;udf_sub_tsvc_error=0;udf_sub_tsvc_timeout=0;udf_sub_udf_complete=0;udf_sub_udf_error=0;udf_sub_udf_timeout=0;udf_sub_udf_filtered_out=0;udf_sub_lang_read_success=0;udf_sub_lang_write_success=0;udf_sub_lang_delete_success=0;udf_sub_lang_error=0;ops_sub_tsvc_error=0;ops_sub_tsvc_timeout=0;ops_sub_write_success=0;ops_sub_write_error=0;ops_sub_write_timeout=0;ops_sub_write_filtered_out=0;dup_res_ask=0;dup_res_respond_read=0;dup_res_respond_no_read=0;retransmit_all_read_dup_res=0;retransmit_all_write_dup_res=0;retransmit_all_delete_dup_res=0;retransmit_all_udf_dup_res=0;retransmit_all_batch_sub_read_dup_res=0;retransmit_all_batch_sub_write_dup_res=0;retransmit_all_batch_sub_delete_dup_res=0;retransmit_all_batch_sub_udf_dup_res=0;retransmit_udf_sub_dup_res=0;retransmit_ops_sub_dup_res=0;retransmit_all_read_repl_ping=0;retransmit_all_batch_sub_read_repl_ping=0;retransmit_all_write_repl_write=0;retransmit_all_delete_repl_write=0;retransmit_all_udf_repl_write=0;retransmit_all_batch_sub_write_repl_write=0;retransmit_all_batch_sub_delete_repl_write=0;retransmit_all_batch_sub_udf_repl_write=0;retransmit_udf_sub_repl_write=0;retransmit_ops_sub_repl_write=0;pi_query_short_basic_complete=0;pi_query_short_basic_error=0;pi_query_short_basic_timeout=0;pi_query_long_basic_complete=0;pi_query_long_basic_error=0;pi_query_long_basic_abort=0;pi_query_aggr_complete=0;pi_query_aggr_error=0;pi_query_aggr_abort=0;pi_query_udf_bg_complete=0;pi_query_udf_bg_error=0;pi_query_udf_bg_abort=0;pi_query_ops_bg_complete=0;pi_query_ops_bg_error=0;pi_query_ops_bg_abort=0;si_query_short_basic_complete=0;si_query_short_basic_error=0;si_query_short_basic_timeout=0;si_query_long_basic_complete=0;si_query_long_basic_error=0;si_query_long_basic_abort=0;si_query_aggr_complete=0;si_query_aggr_error=0;si_query_aggr_abort=0;si_query_udf_bg_complete=0;si_query_udf_bg_error=0;si_query_udf_bg_abort=0;si_query_ops_bg_complete=0;si_query_ops_bg_error=0;si_query_ops_bg_abort=0;geo_region_query_reqs=0;geo_region_query_cells=0;geo_region_query_points=0;geo_region_query_falsepos=0;read_touch_tsvc_error=0;read_touch_tsvc_timeout=0;read_touch_success=0;read_touch_error=0;read_touch_timeout=0;read_touch_skip=0;re_repl_tsvc_error=0;re_repl_tsvc_timeout=0;re_repl_success=0;re_repl_error=0;re_repl_timeout=0;mrt_verify_read_success=0;mrt_verify_read_error=0;mrt_verify_read_timeout=0;mrt_roll_forward_success=0;mrt_roll_forward_error=0;mrt_roll_forward_timeout=0;mrt_monitor_roll_forward_success=0;mrt_monitor_roll_forward_error=0;mrt_monitor_roll_forward_timeout=0;mrt_roll_back_success=0;mrt_roll_back_error=0;mrt_roll_back_timeout=0;mrt_monitor_roll_back_success=0;mrt_monitor_roll_back_error=0;mrt_monitor_roll_back_timeout=0;fail_xdr_forbidden=0;fail_key_busy=0;fail_xdr_key_busy=0;fail_generation=0;fail_record_too_big=0;fail_client_lost_conflict=0;fail_xdr_lost_conflict=0;fail_mrt_blocked=0;fail_mrt_version_mismatch=0;deleted_last_bin=0;active-rack=0;allow-ttl-without-nsup=false;auto-revive=false;background-query-max-rps=10000;conflict-resolution-policy=undefined;conflict-resolve-writes=false;default-read-touch-ttl-pct=0;default-ttl=0;disable-cold-start-eviction=false;disable-write-dup-res=false;disallow-expunge=false;disallow-null-setname=false;enable-benchmarks-batch-sub=false;enable-benchmarks-ops-sub=false;enable-benchmarks-read=false;enable-benchmarks-udf=false;enable-benchmarks-udf-sub=false;enable-benchmarks-write=false;enable-hist-proxy=false;evict-hist-buckets=10000;evict-indexes-memory-pct=0;evict-tenths-pct=5;force-long-queries=false;ignore-migrate-fill-delay=false;index-stage-size=134217728;indexes-memory-budget=0;inline-short-queries=false;max-record-size=1048576;migrate-order=5;migrate-retransmit-ms=5000;migrate-sleep=1;mrt-duration=10;nsup-hist-period=3600;nsup-period=0;nsup-threads=1;partition-tree-sprigs=256;prefer-uniform-balance=true;rack-id=0;read-consistency-level-override=all;reject-non-xdr-writes=false;reject-xdr-writes=false;replication-factor=1;sindex-stage-size=134217728;single-query-threads=4;stop-writes-sys-memory-pct=90;strong-consistency=true;strong-consistency-allow-expunge=false;tomb-raider-eligible-age=300;tomb-raider-period=300;transaction-pending-limit=20;truncate-threads=4;write-commit-level-override=all;xdr-bin-tombstone-ttl=86400;xdr-tomb-raider-period=120;xdr-tomb-raider-threads=1;geo2dsphere-within.strict=true;geo2dsphere-within.min-level=1;geo2dsphere-within.max-level=20;geo2dsphere-within.max-cells=12;geo2dsphere-within.level-mod=1;geo2dsphere-within.earth-radius-meters=6371000;index-type=shmem;sindex-type=shmem;storage-engine=memory;storage-engine.stripe[0]=stripe-0.0xad001000;storage-engine.file[0]=/tmp/ns0-small-sc.dat;storage-engine.commit-to-device=false;storage-engine.compression=none;storage-engine.compression-acceleration=0;storage-engine.compression-level=0;storage-engine.data-size=0;storage-engine.defrag-lwm-pct=50;storage-engine.defrag-queue-min=0;storage-engine.defrag-sleep=1000;storage-engine.defrag-startup-minimum=0;storage-engine.direct-files=false;storage-engine.disable-odsync=false;storage-engine.enable-benchmarks-storage=false;storage-engine.encryption-key-file=null;storage-engine.encryption-old-key-file=null;storage-engine.evict-used-pct=0;storage-engine.filesize=1073741824;storage-engine.flush-max-ms=1000;storage-engine.flush-size=1048576;storage-engine.max-write-cache=67108864;storage-engine.stop-writes-avail-pct=5;storage-engine.stop-writes-used-pct=70;storage-engine.tomb-raider-sleep=1000";
    
}

as_listener::~as_listener ()
{
    stop ();
}

void as_listener::accept_entry ()
{
    m_started = true;
    fd_set set;
    struct timeval timeout;

    while (m_started)
    {
	int connfd;
	sockaddr_in clientaddr;
	socklen_t addrlen = sizeof(clientaddr);

	FD_ZERO (&set);
	FD_SET (m_sfd, &set);
	timeout.tv_sec = 0;
	timeout.tv_usec = 100000;
	int rv = select (m_sfd+1, &set, NULL, NULL, &timeout);
	dieunless (rv != -1);
	
	if (rv == 1) {
	    dieunless ((connfd = accept (m_sfd, (sockaddr *)&clientaddr, &addrlen)) >= 0);
	    std::thread thread (&as_listener::conn_entry, this, connfd);
	    thread.detach ();
	    m_conns.insert (connfd);
	}
    }
}

void as_listener::conn_entry (int connfd)
{
    bool lexit = false;
    size_t sbuf_sz = (8 * 1024 * 1024) + 4096;
    uint8_t *sbuf = (uint8_t *)malloc (sbuf_sz);
    char vbuf[1024] = {0};
    
    while (m_started && !lexit)
    {
	as_header hdr;
	size_t br = read (connfd, &hdr, 8);
	if (br != 8) {
	    lexit = true;
	    continue;
	}

	dieunless (read (connfd, sbuf, hdr.size ()) == hdr.size ());
	uint8_t *dst = nullptr;

	size_t rsz = on_all_msg (&dst, hdr.type, sbuf, hdr.size ());
	
	if (dst) {
	    if (rsz) {
		hdr.size (rsz);
		const struct iovec iov[2] = {
		    { .iov_base=&hdr,			.iov_len=8 },
		    { .iov_base=dst,			.iov_len=rsz }
		};

		// to_hex (vbuf, dst, rsz < 511 ? rsz : 511);
		// vbuf[2*rsz] = 0;
		// printf ("sending: %s\n", vbuf);

		dieunless (writev (connfd, iov, 2) == 8+rsz);
	    }
	    free (dst);
	    dst = nullptr;
	}
    }
    free (sbuf);
    close (connfd);
    m_conns.erase (connfd);
}

void as_listener::start (const string& hostaddr)
{
    if (m_started)
	return;
    
    auto ab = addr_resolve (hostaddr);
    dieunless ((m_sfd = socket (AF_INET, SOCK_STREAM, 0)) > 0);
    dieunless (bind (m_sfd, (sockaddr *)ab.data (), ab.size ()) == 0);
    
    dieunless (listen (m_sfd, 5) == 0);
    dieunless (!m_mainThread);
    m_mainThread = new std::thread (&as_listener::accept_entry, this);
}

void as_listener::stop ()
{
    if (!m_started)
	return;

    m_started = false;
    m_mainThread->join ();
    delete m_mainThread;
    m_mainThread = nullptr;
}

size_t as_listener::on_all_msg (uint8_t **dst, uint8_t type, uint8_t *src, size_t sz)
{
    dieunless ((type == 1) || (type == 3));
    return (type == 1) ? on_info_msg (dst, (char *)src, sz) : on_cmd ((as_msg **)dst, (const as_msg*)src, sz);
}

string as_listener::on_info (const string& icmd)
{
    if (m_sinfo.count (icmd)) {
	return m_sinfo[icmd];
    }

    if (!icmd.compare ("cluster-name")) {
	return "dummy";
    }
    string ret = "";

    if (this->onInfo) {
	this->onInfo (ret, icmd);
    }
    return ret;
}

size_t as_listener::on_info_msg (uint8_t **dst, char *src, size_t sz)
{
    // printf ("info: %.*s\n", sz, src);
    // split on newlines
    char *lp = (char *)src - 1;
    char *ep = (char *)src + sz;
    string ret = "";
    while (lp && ((lp + 1) < ep)) {
	char *sp = lp + 1;
	lp = strchr (sp, '\n');
	string icmd = lp ? string (sp, lp - sp) : string (sp);
	string cret = this->on_info (icmd);
	if (cret.length ()) {
	    ret += icmd + "\t" + cret + "\n";
	}
    }
    
    if (! *dst) {
	*dst = (uint8_t *)malloc (ret.length ());
    }
    
    memcpy (*dst, ret.c_str (), ret.length ());
    return ret.length ();
}


size_t as_listener::on_cmd (as_msg **dst, const as_msg *src, size_t sz)
{
    char vbuf[1024] = {0};
    // to_hex (vbuf, src, sz < 511 ? sz : 511);
    // vbuf[2*sz] = 0;
    // printf ("cmd: %s\n", vbuf);


    if (src->flags & AS_MSG_FLAG_BATCH) {
	// skip and go to batch processing
    }

    // get namespace

    // if query..
    // a query is anything from the client without a digest field or a key field
    
    // if we don't have the udf filename field, and AS_MSG_INFO2_WRITE is unset, basic query.  if it's set, ops bg
    // if we do, we also have the udf_op field, which says if we're a aggregate or background
    
    // we must have AS_MSG_INFO3_PARTITION_DONE set

    // get query set (or no set)
    // get query pids (get, from pid_array field, a list of uint16_t, between 0 and 4095.)
    // "has where clause" == has the field index_range.  goes with bval_array. goes with digest_Array
    // get query range, get query rps
    // query range: 0x01, [binn len], bin_name, particle_type,

    

    // else, we have a digest. or we're a batch.
    // if we have AS_MSG_INFO2_WRITE we do that check first
    
    sz = 0;
    if (this->onMsg) {
	sz = this->onMsg (dst, src);
    }

    if (! *dst) {
	*dst = (as_msg *) malloc (sizeof(as_msg));
	as_msg *resp = (as_msg *)*dst;
	resp->clear ();
	resp->be_ops = 0;
	resp->be_fields = 0;
	resp->result_code = 0;
	sz = sizeof(as_msg);
    }
    
    return sz;
}
