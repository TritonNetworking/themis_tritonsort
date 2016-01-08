/* rand16.h - Header file for 128-bit linear congruential generator
 *
 * Special BSD version
 * Chris Nyberg <chris.nyberg@ordinal.com>
 *
 * Copyright (C) 2009 - 2016, Chris Nyberg
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in 
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the names of Chris Nyberg nor Ordinal Technology Corp
 *       may be used to endorse or promote products derived from this
 *       software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT
 * HOLDER> BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 * OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/* This software is provided 'as-is', without any express or implied
 * warranty.  In no event will the author be held liable for any damages
 * arising from the use of this software.
 */

#if defined(_WIN32)
typedef unsigned __int64        u8;
#else
typedef unsigned long long      u8;
#endif

typedef struct
{
    u8  hi8;
    u8  lo8;
} u16;

/* define the maximum size character buffer necessary to hold an ascii
 * representation of a u16 number.  39 decimal digits and 1 '\0' terminator.
 */
#define U16_ASCII_BUF_SIZE      (39 + 1)


/* hex_to_u16 - convert a hexadecimal string to a u16
 */
u16 hex_to_u16(const char *s);


/* dec_to_u16 - convert an unsigned decimal string to a u16
 */
u16 dec_to_u16(const char *s);


/* u16_to_hex - convert a u16 to a hexadecimal string and write that string
 *              into the provided buffer which must be at least 33 bytes long.
 */
char *u16_to_hex(u16 k, char *buf);

/* u16_to_dec - convert a u16 to a decimal string and write that string
 *              into the provided buffer
 * Args:
 *    k -   the u16 number to be converted to a decimal string.
 *    buf - pointer to a character buffer of size U16_ASCII_BUF_SIZE.
 * Returns:
 *    pointer the "buf" arg which contains the decimal string.
 */
char *u16_to_dec(u16 k, char *buf);


/* mult16 - multiply two 16-byte numbers
 */
u16 mult16(u16 a, u16 b);


/* add16 - add two 16-byte numbers
 */
u16 add16(u16 a, u16 b);


/* skip_ahead_rand - generate the random number that is "advance" steps
 *              from an initial random number of 0.  This is done by
 *              starting with 0, and then advancing the by the
 *              appropriate powers of 2 of the linear congruential
 *              generator.
 */
u16 skip_ahead_rand(u16 advance);


/* next_rand - the sequential 128-bit random number generator.
 *             Advance the random number forward once using the linear 
 *             congruential generator, and then return the new random
 *             number.
 */
u16 next_rand(u16 rand);

