#pragma once

//==========================================================
// Includes.
//
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif



/*
 * Copy of SHA1 API and implementation.
 *
 * For FIPS 140 compliance, this is NOT to be used for crypto purposes!
 */

//==========================================================
// Typedefs & constants.
//

#define CF_SHA_DIGEST_LENGTH 20


//==========================================================
// Public API.
//
/*
 * Copy of RIPEMD160 API and implementation.
 *
 * For FIPS 140 compliance, this is NOT to be used for crypto purposes!
 */

//==========================================================
// Typedefs & constants.
//

#define CF_RIPEMD160_DIGEST_LENGTH 20

// Private! Do not access members.
typedef struct cf_RIPEMD160_CTX_s {
	uint32_t total[2];
	uint32_t state[5];
	unsigned char buffer[64];
} cf_RIPEMD160_CTX;


//==========================================================
// Public API.
//

int cf_RIPEMD160(const unsigned char* input, size_t ilen, unsigned char* output);
int cf_RIPEMD160_Init(cf_RIPEMD160_CTX* ctx);
int cf_RIPEMD160_Update(cf_RIPEMD160_CTX* ctx, const void* v_input, size_t ilen);
int cf_RIPEMD160_Final(unsigned char* output, cf_RIPEMD160_CTX* ctx);


#ifdef __cplusplus
} // end extern "C"
#endif
