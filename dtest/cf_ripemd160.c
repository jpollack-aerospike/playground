/*
 * Copy of RIPEMD160 API and implementation.
 *
 * For FIPS 140 compliance, this is NOT to be used for crypto purposes!
 */

//==========================================================
// Typedefs & constants.
//
#include "cf_ripemd160.h"
#include <stddef.h>
#include <string.h>
#include <stdint.h>

static const unsigned char cf_ripemd160_padding[64] =
{
	0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0,    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};


//==========================================================
// Forward declarations.
//

static int cf_ripemd160_process(cf_RIPEMD160_CTX* ctx, const unsigned char data[64]);


//==========================================================
// Inlines & macros.
//

#define GET_UINT32_LE(n,b,i) \
{ \
	(n) = ((uint32_t) (b)[(i)  ]      ) | \
		((uint32_t) (b)[(i) + 1] <<  8) | \
		((uint32_t) (b)[(i) + 2] << 16) | \
		((uint32_t) (b)[(i) + 3] << 24);  \
}

#define PUT_UINT32_LE(n,b,i) \
{ \
	(b)[(i)    ] = (unsigned char) (((n)      ) & 0xFF); \
	(b)[(i) + 1] = (unsigned char) (((n) >>  8) & 0xFF); \
	(b)[(i) + 2] = (unsigned char) (((n) >> 16) & 0xFF); \
	(b)[(i) + 3] = (unsigned char) (((n) >> 24) & 0xFF); \
}


//==========================================================
// Public API.
//

int
cf_RIPEMD160(const unsigned char* input, size_t ilen, unsigned char* output)
{
	int ret;
	cf_RIPEMD160_CTX ctx;

	if ((ret = cf_RIPEMD160_Init(&ctx)) != 0) {
		return ret;
	}

	if ((ret = cf_RIPEMD160_Update(&ctx, input, ilen)) != 0) {
		return ret;
	}

	if ((ret = cf_RIPEMD160_Final(output, &ctx)) != 0) {
		return ret;
	}

	return 0;
}

int
cf_RIPEMD160_Init(cf_RIPEMD160_CTX* ctx)
{
	memset(ctx, 0, sizeof(cf_RIPEMD160_CTX));

	ctx->total[0] = 0;
	ctx->total[1] = 0;

	ctx->state[0] = 0x67452301;
	ctx->state[1] = 0xEFCDAB89;
	ctx->state[2] = 0x98BADCFE;
	ctx->state[3] = 0x10325476;
	ctx->state[4] = 0xC3D2E1F0;

	return 0;
}

int
cf_RIPEMD160_Update(cf_RIPEMD160_CTX* ctx, const void* v_input, size_t ilen)
{
	const unsigned char* input = (const unsigned char*)v_input;

	int ret;

	if (ilen == 0) {
		return 0;
	}

	size_t left = ctx->total[0] & 0x3F;
	uint32_t fill = (uint32_t)(64 - left);

	ctx->total[0] += (uint32_t) ilen;
	ctx->total[0] &= 0xFFFFFFFF;

	if (ctx->total[0] < (uint32_t) ilen) {
		ctx->total[1]++;
	}

	if (left && ilen >= fill) {
		memcpy((void*)(ctx->buffer + left), input, fill);

		if ((ret = cf_ripemd160_process(ctx, ctx->buffer)) != 0) {
			return ret;
		}

		input += fill;
		ilen  -= fill;
		left = 0;
	}

	while (ilen >= 64) {
		if ((ret = cf_ripemd160_process(ctx, input)) != 0) {
			return ret;
		}

		input += 64;
		ilen  -= 64;
	}

	if (ilen > 0) {
		memcpy((void*)(ctx->buffer + left), input, ilen);
	}

	return 0;
}

int
cf_RIPEMD160_Final(unsigned char* output, cf_RIPEMD160_CTX* ctx)
{
	uint32_t high = (ctx->total[0] >> 29) | (ctx->total[1] <<  3);
	uint32_t low  = (ctx->total[0] <<  3);

	unsigned char msglen[8];

	PUT_UINT32_LE(low,  msglen, 0);
	PUT_UINT32_LE(high, msglen, 4);

	uint32_t last = ctx->total[0] & 0x3F;
	uint32_t padn = (last < 56) ? (56 - last) : (120 - last);

	int ret = cf_RIPEMD160_Update(ctx, cf_ripemd160_padding, padn);

	if (ret != 0) {
		return ret;
	}

	ret = cf_RIPEMD160_Update(ctx, msglen, 8);

	if (ret != 0) {
		return ret;
	}

	PUT_UINT32_LE(ctx->state[0], output,  0);
	PUT_UINT32_LE(ctx->state[1], output,  4);
	PUT_UINT32_LE(ctx->state[2], output,  8);
	PUT_UINT32_LE(ctx->state[3], output, 12);
	PUT_UINT32_LE(ctx->state[4], output, 16);

	return 0;
}


//==========================================================
// Local helpers.
//

static int
cf_ripemd160_process(cf_RIPEMD160_CTX* ctx, const unsigned char data[64])
{
	struct {
		uint32_t A, B, C, D, E, Ap, Bp, Cp, Dp, Ep, X[16];
	} local;

	GET_UINT32_LE(local.X[ 0], data,  0);
	GET_UINT32_LE(local.X[ 1], data,  4);
	GET_UINT32_LE(local.X[ 2], data,  8);
	GET_UINT32_LE(local.X[ 3], data, 12);
	GET_UINT32_LE(local.X[ 4], data, 16);
	GET_UINT32_LE(local.X[ 5], data, 20);
	GET_UINT32_LE(local.X[ 6], data, 24);
	GET_UINT32_LE(local.X[ 7], data, 28);
	GET_UINT32_LE(local.X[ 8], data, 32);
	GET_UINT32_LE(local.X[ 9], data, 36);
	GET_UINT32_LE(local.X[10], data, 40);
	GET_UINT32_LE(local.X[11], data, 44);
	GET_UINT32_LE(local.X[12], data, 48);
	GET_UINT32_LE(local.X[13], data, 52);
	GET_UINT32_LE(local.X[14], data, 56);
	GET_UINT32_LE(local.X[15], data, 60);

	local.A = local.Ap = ctx->state[0];
	local.B = local.Bp = ctx->state[1];
	local.C = local.Cp = ctx->state[2];
	local.D = local.Dp = ctx->state[3];
	local.E = local.Ep = ctx->state[4];

#define F1(x, y, z)   ((x) ^ (y) ^ (z))
#define F2(x, y, z)   (((x) & (y)) | (~(x) & (z)))
#define F3(x, y, z)   (((x) | ~(y)) ^ (z))
#define F4(x, y, z)   (((x) & (z)) | ((y) & ~(z)))
#define F5(x, y, z)   ((x) ^ ((y) | ~(z)))

#define S(x, n) (((x) << (n)) | ((x) >> (32 - (n))))

#define P(a, b, c, d, e, r, s, f, k) \
	do { \
		(a) += f((b), (c), (d)) + local.X[r] + (k); \
		(a) = S((a), (s)) + (e); \
		(c) = S((c), 10); \
	} while (0)

#define P2(a, b, c, d, e, r, s, rp, sp) \
	do { \
		P((a), (b), (c), (d), (e), (r), (s), F, K); \
		P(a ## p, b ## p, c ## p, d ## p, e ## p, \
		   (rp), (sp), Fp, Kp); \
	} while (0)

#define F   F1
#define K   0x00000000
#define Fp  F5
#define Kp  0x50A28BE6
	P2(local.A, local.B, local.C, local.D, local.E,  0, 11,  5,  8);
	P2(local.E, local.A, local.B, local.C, local.D,  1, 14, 14,  9);
	P2(local.D, local.E, local.A, local.B, local.C,  2, 15,  7,  9);
	P2(local.C, local.D, local.E, local.A, local.B,  3, 12,  0, 11);
	P2(local.B, local.C, local.D, local.E, local.A,  4,  5,  9, 13);
	P2(local.A, local.B, local.C, local.D, local.E,  5,  8,  2, 15);
	P2(local.E, local.A, local.B, local.C, local.D,  6,  7, 11, 15);
	P2(local.D, local.E, local.A, local.B, local.C,  7,  9,  4,  5);
	P2(local.C, local.D, local.E, local.A, local.B,  8, 11, 13,  7);
	P2(local.B, local.C, local.D, local.E, local.A,  9, 13,  6,  7);
	P2(local.A, local.B, local.C, local.D, local.E, 10, 14, 15,  8);
	P2(local.E, local.A, local.B, local.C, local.D, 11, 15,  8, 11);
	P2(local.D, local.E, local.A, local.B, local.C, 12,  6,  1, 14);
	P2(local.C, local.D, local.E, local.A, local.B, 13,  7, 10, 14);
	P2(local.B, local.C, local.D, local.E, local.A, 14,  9,  3, 12);
	P2(local.A, local.B, local.C, local.D, local.E, 15,  8, 12,  6);
#undef F
#undef K
#undef Fp
#undef Kp

#define F   F2
#define K   0x5A827999
#define Fp  F4
#define Kp  0x5C4DD124
	P2(local.E, local.A, local.B, local.C, local.D,  7,  7,  6,  9);
	P2(local.D, local.E, local.A, local.B, local.C,  4,  6, 11, 13);
	P2(local.C, local.D, local.E, local.A, local.B, 13,  8,  3, 15);
	P2(local.B, local.C, local.D, local.E, local.A,  1, 13,  7,  7);
	P2(local.A, local.B, local.C, local.D, local.E, 10, 11,  0, 12);
	P2(local.E, local.A, local.B, local.C, local.D,  6,  9, 13,  8);
	P2(local.D, local.E, local.A, local.B, local.C, 15,  7,  5,  9);
	P2(local.C, local.D, local.E, local.A, local.B,  3, 15, 10, 11);
	P2(local.B, local.C, local.D, local.E, local.A, 12,  7, 14,  7);
	P2(local.A, local.B, local.C, local.D, local.E,  0, 12, 15,  7);
	P2(local.E, local.A, local.B, local.C, local.D,  9, 15,  8, 12);
	P2(local.D, local.E, local.A, local.B, local.C,  5,  9, 12,  7);
	P2(local.C, local.D, local.E, local.A, local.B,  2, 11,  4,  6);
	P2(local.B, local.C, local.D, local.E, local.A, 14,  7,  9, 15);
	P2(local.A, local.B, local.C, local.D, local.E, 11, 13,  1, 13);
	P2(local.E, local.A, local.B, local.C, local.D,  8, 12,  2, 11);
#undef F
#undef K
#undef Fp
#undef Kp

#define F   F3
#define K   0x6ED9EBA1
#define Fp  F3
#define Kp  0x6D703EF3
	P2(local.D, local.E, local.A, local.B, local.C,  3, 11, 15,  9);
	P2(local.C, local.D, local.E, local.A, local.B, 10, 13,  5,  7);
	P2(local.B, local.C, local.D, local.E, local.A, 14,  6,  1, 15);
	P2(local.A, local.B, local.C, local.D, local.E,  4,  7,  3, 11);
	P2(local.E, local.A, local.B, local.C, local.D,  9, 14,  7,  8);
	P2(local.D, local.E, local.A, local.B, local.C, 15,  9, 14,  6);
	P2(local.C, local.D, local.E, local.A, local.B,  8, 13,  6,  6);
	P2(local.B, local.C, local.D, local.E, local.A,  1, 15,  9, 14);
	P2(local.A, local.B, local.C, local.D, local.E,  2, 14, 11, 12);
	P2(local.E, local.A, local.B, local.C, local.D,  7,  8,  8, 13);
	P2(local.D, local.E, local.A, local.B, local.C,  0, 13, 12,  5);
	P2(local.C, local.D, local.E, local.A, local.B,  6,  6,  2, 14);
	P2(local.B, local.C, local.D, local.E, local.A, 13,  5, 10, 13);
	P2(local.A, local.B, local.C, local.D, local.E, 11, 12,  0, 13);
	P2(local.E, local.A, local.B, local.C, local.D,  5,  7,  4,  7);
	P2(local.D, local.E, local.A, local.B, local.C, 12,  5, 13,  5);
#undef F
#undef K
#undef Fp
#undef Kp

#define F   F4
#define K   0x8F1BBCDC
#define Fp  F2
#define Kp  0x7A6D76E9
	P2(local.C, local.D, local.E, local.A, local.B,  1, 11,  8, 15);
	P2(local.B, local.C, local.D, local.E, local.A,  9, 12,  6,  5);
	P2(local.A, local.B, local.C, local.D, local.E, 11, 14,  4,  8);
	P2(local.E, local.A, local.B, local.C, local.D, 10, 15,  1, 11);
	P2(local.D, local.E, local.A, local.B, local.C,  0, 14,  3, 14);
	P2(local.C, local.D, local.E, local.A, local.B,  8, 15, 11, 14);
	P2(local.B, local.C, local.D, local.E, local.A, 12,  9, 15,  6);
	P2(local.A, local.B, local.C, local.D, local.E,  4,  8,  0, 14);
	P2(local.E, local.A, local.B, local.C, local.D, 13,  9,  5,  6);
	P2(local.D, local.E, local.A, local.B, local.C,  3, 14, 12,  9);
	P2(local.C, local.D, local.E, local.A, local.B,  7,  5,  2, 12);
	P2(local.B, local.C, local.D, local.E, local.A, 15,  6, 13,  9);
	P2(local.A, local.B, local.C, local.D, local.E, 14,  8,  9, 12);
	P2(local.E, local.A, local.B, local.C, local.D,  5,  6,  7,  5);
	P2(local.D, local.E, local.A, local.B, local.C,  6,  5, 10, 15);
	P2(local.C, local.D, local.E, local.A, local.B,  2, 12, 14,  8);
#undef F
#undef K
#undef Fp
#undef Kp

#define F   F5
#define K   0xA953FD4E
#define Fp  F1
#define Kp  0x00000000
	P2(local.B, local.C, local.D, local.E, local.A,  4,  9, 12,  8);
	P2(local.A, local.B, local.C, local.D, local.E,  0, 15, 15,  5);
	P2(local.E, local.A, local.B, local.C, local.D,  5,  5, 10, 12);
	P2(local.D, local.E, local.A, local.B, local.C,  9, 11,  4,  9);
	P2(local.C, local.D, local.E, local.A, local.B,  7,  6,  1, 12);
	P2(local.B, local.C, local.D, local.E, local.A, 12,  8,  5,  5);
	P2(local.A, local.B, local.C, local.D, local.E,  2, 13,  8, 14);
	P2(local.E, local.A, local.B, local.C, local.D, 10, 12,  7,  6);
	P2(local.D, local.E, local.A, local.B, local.C, 14,  5,  6,  8);
	P2(local.C, local.D, local.E, local.A, local.B,  1, 12,  2, 13);
	P2(local.B, local.C, local.D, local.E, local.A,  3, 13, 13,  6);
	P2(local.A, local.B, local.C, local.D, local.E,  8, 14, 14,  5);
	P2(local.E, local.A, local.B, local.C, local.D, 11, 11,  0, 15);
	P2(local.D, local.E, local.A, local.B, local.C,  6,  8,  3, 13);
	P2(local.C, local.D, local.E, local.A, local.B, 15,  5,  9, 11);
	P2(local.B, local.C, local.D, local.E, local.A, 13,  6, 11, 11);
#undef F
#undef K
#undef Fp
#undef Kp

	local.C       = ctx->state[1] + local.C + local.Dp;
	ctx->state[1] = ctx->state[2] + local.D + local.Ep;
	ctx->state[2] = ctx->state[3] + local.E + local.Ap;
	ctx->state[3] = ctx->state[4] + local.A + local.Bp;
	ctx->state[4] = ctx->state[0] + local.B + local.Cp;
	ctx->state[0] = local.C;

	return 0;

}
