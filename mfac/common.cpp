#include "common.hpp"

// callers ensure not voidptr
static string as_val_to_string (const as_val *v)
{
    assert (v);
    char *t0 = as_val_val_tostring (v);
    string ret (t0);
    free (t0);
    return ret;
}

string hexstring (const void *vp, size_t sz)
{
    const char hexm[] = "0123456789ABCDEF";
    assert (vp && (sz > 0));
    string ret (sz * 2, 0);
    const char *p = static_cast<const char *>(vp);
    for (size_t ii=0; ii<sz; ii++)
    {
	ret[(2*ii)] = hexm[(p[ii]>>4) & 0x0F];
	ret[(2*ii)+1] = hexm[p[ii] & 0x0F];
    }
    return ret;
}

void StringRecord::populate (const as_record *r)
{

    if (m_asrec)   // cleanup
    {
	m_bins.clear ();
	m_key.clear ();
	m_asrec = nullptr;
    }

    if (r)
    {
	m_asrec = r;
	// TODO Can't get this to work without crashing, figure out the api.  
	// auto key = r->key;
	// auto digest = as_key_digest (&key);
	// string digstr = hexstring (digest->value, AS_DIGEST_VALUE_SIZE);
	// LOG ("digest: %s ", digstr.c_str ());
	
	m_key = r->key.valuep ? as_val_to_string ((as_val *)r->key.valuep) : "";
	as_record_iterator it;
	as_record_iterator_init(&it, r);
	while (as_record_iterator_has_next(&it)) {
	    const as_bin *b = as_record_iterator_next (&it);
	    m_bins.emplace_back (b->name, (b->valuep
					   ? as_val_to_string ((as_val *)b->valuep) 
					   : ""s));
	}
	as_record_iterator_destroy(&it);
	// maybe sort?
    }
}

