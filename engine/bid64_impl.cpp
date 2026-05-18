// Minimal Intel BID64 (Binary Integer Decimal) implementation.
// The IBKR Decimal.cpp declares these as extern "C" but does not ship
// the Intel libbid library. We provide working implementations here.
//
// BID64 encoding (IEEE 754-2008 decimal64, binary significand):
//   bit 63      : sign
//   bits [62:53]: biased exponent (bias = 398); range 0..767 → exp -398..369
//   bits [52:0] : integer coefficient (≤ 9 999 999 999 999 999 < 2^53)
//
// "Large coefficient" form (bits [62:61] = 11, bits [62:59] ≠ 1111):
//   bits [60:51]: biased exponent (10 bits)
//   bits [50:0] : low 51 bits of coefficient
//   coefficient = 2^53 + bits[50:0]
//
// Infinity / NaN: bits [62:59] = 1111

#include "Decimal.h"
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <climits>

static const unsigned long long SIGN_MASK        = 0x8000000000000000ULL;
static const unsigned long long SPECIAL_MASK     = 0x6000000000000000ULL; // bits[62:61]=11
static const unsigned long long INFINITY_MASK    = 0x7800000000000000ULL; // bits[62:59]=1111
static const unsigned long long NAN_MASK         = 0x7C00000000000000ULL; // bits[62:58]=11111
static const unsigned long long SMALL_COEFF_MASK = 0x001FFFFFFFFFFFFFULL; // bits[52:0]
static const unsigned long long LARGE_COEFF_MASK = 0x0007FFFFFFFFFFFFULL; // bits[50:0]
static const unsigned long long LARGE_COEFF_HIGH = 0x0020000000000000ULL; // 2^53
static const int  BIAS            = 398;
static const int  EXP_SHIFT_SMALL = 53;
static const int  EXP_SHIFT_LARGE = 51;
static const int  EXP_MASK_BITS   = 0x3FF;

// Unpack BID64 → (sign, biased_exp, coeff). Returns 0 for Inf/NaN.
static int bid64_unpack(unsigned long long x, int* sign, int* biased_exp,
                        unsigned long long* coeff)
{
    *sign = (x & SIGN_MASK) ? 1 : 0;
    if ((x & SPECIAL_MASK) == SPECIAL_MASK) {
        if ((x & INFINITY_MASK) == INFINITY_MASK) return 0; // Inf / NaN
        // Large coefficient form
        *biased_exp = (int)((x >> EXP_SHIFT_LARGE) & EXP_MASK_BITS);
        *coeff      = LARGE_COEFF_HIGH | (x & LARGE_COEFF_MASK);
        return 1;
    }
    *biased_exp = (int)((x >> EXP_SHIFT_SMALL) & EXP_MASK_BITS);
    *coeff      = x & SMALL_COEFF_MASK;
    return 1;
}

// Pack (sign, biased_exp, coeff) → BID64. Assumes normal range.
static unsigned long long bid64_pack(int sign, int biased_exp,
                                     unsigned long long coeff)
{
    if (biased_exp < 0)   biased_exp = 0;
    if (biased_exp > 767) biased_exp = 767;
    unsigned long long r;
    if (coeff >= LARGE_COEFF_HIGH) {
        // Large coefficient form
        r = SPECIAL_MASK
            | ((unsigned long long)(biased_exp & EXP_MASK_BITS) << EXP_SHIFT_LARGE)
            | (coeff & LARGE_COEFF_MASK);
    } else {
        r = ((unsigned long long)(biased_exp & EXP_MASK_BITS) << EXP_SHIFT_SMALL)
            | (coeff & SMALL_COEFF_MASK);
    }
    if (sign) r |= SIGN_MASK;
    return r;
}

// Power-of-10 table for exponents 0..22
static const double pow10d[23] = {
    1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,
    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
    1e20, 1e21, 1e22
};
static double fast_pow10(int e)
{
    if (e >= 0 && e <= 22) return pow10d[e];
    return pow(10.0, (double)e);
}

extern "C" {

double __bid64_to_binary64(Decimal x, unsigned int /*rnd*/, unsigned int* flags)
{
    if (flags) *flags = 0;
    int sign, be;
    unsigned long long coeff;
    if (!bid64_unpack(x, &sign, &be, &coeff)) {
        if ((x & NAN_MASK) == NAN_MASK) return std::numeric_limits<double>::quiet_NaN();
        return sign ? -std::numeric_limits<double>::infinity()
                    :  std::numeric_limits<double>::infinity();
    }
    if (coeff == 0) return 0.0;
    int exp = be - BIAS;
    double v = (double)coeff;
    if      (exp > 0) v *= fast_pow10( exp);
    else if (exp < 0) v /= fast_pow10(-exp);
    return sign ? -v : v;
}

Decimal __binary64_to_bid64(double d, unsigned int /*rnd*/, unsigned int* flags)
{
    if (flags) *flags = 0;
    if (std::isnan(d))  return NAN_MASK;
    if (std::isinf(d))  return (d < 0) ? (SIGN_MASK | INFINITY_MASK) : INFINITY_MASK;
    if (d == 0.0)       return (unsigned long long)BIAS << EXP_SHIFT_SMALL;

    int sign = (d < 0) ? 1 : 0;
    double abs_d = sign ? -d : d;

    // Scale to integer coefficient in [1, 10^15)
    int exp = 0;
    while (abs_d >= 1e15) { abs_d /= 10.0; exp++; }
    while (abs_d < 1.0)   { abs_d *= 10.0; exp--; }

    unsigned long long coeff = (unsigned long long)(abs_d + 0.5); // round
    // Re-check after rounding
    if (coeff >= (unsigned long long)1e15) { coeff /= 10; exp++; }

    return bid64_pack(sign, exp + BIAS, coeff);
}

Decimal __bid64_from_string(char* str, unsigned int rnd, unsigned int* flags)
{
    if (flags) *flags = 0;
    if (!str || *str == '\0') return UNSET_DECIMAL;
    char* end = nullptr;
    double d = strtod(str, &end);
    if (end == str) return UNSET_DECIMAL; // no conversion
    return __binary64_to_bid64(d, rnd, flags);
}

void __bid64_to_string(char* buf, Decimal x, unsigned int* flags)
{
    if (flags) *flags = 0;
    if (!buf) return;
    if ((x & NAN_MASK) == NAN_MASK) { strcpy(buf, "+NaN"); return; }
    if ((x & INFINITY_MASK) == INFINITY_MASK) {
        strcpy(buf, (x & SIGN_MASK) ? "-Inf" : "+Inf");
        return;
    }
    int sign, be;
    unsigned long long coeff;
    bid64_unpack(x, &sign, &be, &coeff);
    int exp = be - BIAS;
    // Format: [+/-]CoeffE[+/-]Exp  (matches Intel library output style used by decimalStringToDisplay)
    if (exp >= 0)
        sprintf(buf, "%s%lluE+%d", sign ? "-" : "+", coeff, exp);
    else
        sprintf(buf, "%s%lluE%d",  sign ? "-" : "+", coeff, exp);
}

Decimal __bid64_add(Decimal a, Decimal b, unsigned int rnd, unsigned int* flags)
{
    if (flags) *flags = 0;
    return __binary64_to_bid64(
        __bid64_to_binary64(a, 0, nullptr) + __bid64_to_binary64(b, 0, nullptr), rnd, flags);
}

Decimal __bid64_sub(Decimal a, Decimal b, unsigned int rnd, unsigned int* flags)
{
    if (flags) *flags = 0;
    return __binary64_to_bid64(
        __bid64_to_binary64(a, 0, nullptr) - __bid64_to_binary64(b, 0, nullptr), rnd, flags);
}

Decimal __bid64_mul(Decimal a, Decimal b, unsigned int rnd, unsigned int* flags)
{
    if (flags) *flags = 0;
    return __binary64_to_bid64(
        __bid64_to_binary64(a, 0, nullptr) * __bid64_to_binary64(b, 0, nullptr), rnd, flags);
}

Decimal __bid64_div(Decimal a, Decimal b, unsigned int rnd, unsigned int* flags)
{
    if (flags) *flags = 0;
    return __binary64_to_bid64(
        __bid64_to_binary64(a, 0, nullptr) / __bid64_to_binary64(b, 0, nullptr), rnd, flags);
}

} // extern "C"
