/* rand16.c - 128-bit linear congruential generator
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

/* This file implements a 128-bit linear congruential generator.
 * Specifically, if X0 is the most recently issued 128-bit random
 * number (or a seed of 0 if no random number has already been generated,
 * the next number to be generated, X1, is equal to:
 * X1 = (a * X0 + c) mod 2**128
 * where a is 47026247687942121848144207491837523525
 *            or 0x2360ed051fc65da44385df649fccf645
 *   and c is 98910279301475397889117759788405497857
 *            or 0x4a696d47726179524950202020202001
 * The coefficient "a" is suggested by:
 * Pierre L'Ecuyer, "Tables of linear congruential generators of different
 * sizes and good lattice structure", Mathematics of Computation, 68
 * pp. 249 - 260 (1999)
 * http://www.ams.org/mcom/1999-68-225/S0025-5718-99-00996-5/S0025-5718-99-00996-5.pdf
 * The constant "c" meets the simple suggestion by the same reference that
 * it be odd.
 *
 * There is also a facility for quickly advancing the state of the
 * generator by a fixed number of steps - this facilitates parallel
 * generation.
 *
 *
 */
#include <stdio.h>
#include <string.h>
#include "rand16.h"


/* The "Gen" array contain powers of 2 of the linear congruential generator.
 * The index 0 struct contain the "a" coefficient and "c" constant for the
 * generator.  That is, the generator is:
 *    f(x) = (Gen[0].a * x + Gen[0].c) mod 2**128
 *
 * All structs after the first contain an "a" and "c" that
 * comprise the square of the previous function.
 *
 * f**2(x) = (Gen[1].a * x + Gen[1].c) mod 2**128
 * f**4(x) = (Gen[2].a * x + Gen[2].c) mod 2**128
 * f**8(x) = (Gen[3].a * x + Gen[3].c) mod 2**128
 * ...
 */
static struct
{
    u16 a;
    u16 c;
} Gen[128] =
{
    /* [  0].a */ {{0x2360ed051fc65da4LL, 0x4385df649fccf645LL},
    /* [  0].c */     {0x4a696d4772617952LL, 0x4950202020202001LL}},
    /* [  1].a */ {{0x17bce35bdf69743cLL, 0x529ed9eb20e0ae99LL},
    /* [  1].c */     {0x95e0e48262b3edfeLL, 0x04479485c755b646LL}},
    /* [  2].a */ {{0xf4dd417327db7a9bLL, 0xd194dfbe42d45771LL},
    /* [  2].c */     {0x882a02c315362b60LL, 0x765f100068b33a1cLL}},
    /* [  3].a */ {{0x6347af777a7898f6LL, 0xd1a2d6f33505ffe1LL},
    /* [  3].c */     {0x5efc4abfaca23e8cLL, 0xa8edb1f2dfbf6478LL}},
    /* [  4].a */ {{0xb6a4239f3b315f84LL, 0xf6ef6d3d288c03c1LL},
    /* [  4].c */     {0xf25bd15439d16af5LL, 0x94c1b1bafa6239f0LL}},
    /* [  5].a */ {{0x2c82901ad1cb0cd1LL, 0x82b631ba6b261781LL},
    /* [  5].c */     {0x89ca67c29c9397d5LL, 0x9c612596145db7e0LL}},
    /* [  6].a */ {{0xdab03f988288676eLL, 0xe49e66c4d2746f01LL},
    /* [  6].c */     {0x8b6ae036713bd578LL, 0xa8093c8eae5c7fc0LL}},
    /* [  7].a */ {{0x602167331d86cf56LL, 0x84fe009a6d09de01LL},
    /* [  7].c */     {0x98a2542fd23d0dbdLL, 0xff3b886cdb1d3f80LL}},
    /* [  8].a */ {{0x61ecb5c24d95b058LL, 0xf04c80a23697bc01LL},
    /* [  8].c */     {0x954db923fdb7933eLL, 0x947cd1edcecb7f00LL}},
    /* [  9].a */ {{0x4a5c31e0654c28aaLL, 0x60474e83bf3f7801LL},
    /* [  9].c */     {0x00be4a36657c98cdLL, 0x204e8c8af7dafe00LL}},
    /* [ 10].a */ {{0xae4f079d54fbece1LL, 0x478331d3c6bef001LL},
    /* [ 10].c */     {0x991965329dccb28dLL, 0x581199ab18c5fc00LL}},
    /* [ 11].a */ {{0x101b8cb830c7cb92LL, 0x7ff1ed50ae7de001LL},
    /* [ 11].c */     {0xe1a8705b63ad5b8cLL, 0xd6c3d268d5cbf800LL}},
    /* [ 12].a */ {{0xf54a27fc056b00e7LL, 0x563f3505e0fbc001LL},
    /* [ 12].c */     {0x2b657bbfd6ed9d63LL, 0x2079e70c3c97f000LL}},
    /* [ 13].a */ {{0xdf8a6fc1a833d201LL, 0xf98d719dd1f78001LL},
    /* [ 13].c */     {0x59b60ee4c52fa49eLL, 0x9fe90682bd2fe000LL}},
    /* [ 14].a */ {{0x5480a5015f101a4eLL, 0xa7e3f183e3ef0001LL},
    /* [ 14].c */     {0xcc099c8803067946LL, 0x4fe86aae8a5fc000LL}},
    /* [ 15].a */ {{0xa498509e76e5d792LL, 0x5f539c28c7de0001LL},
    /* [ 15].c */     {0x06b9abff9f9f33ddLL, 0x30362c0154bf8000LL}},
    /* [ 16].a */ {{0x0798a3d8b10dc72eLL, 0x60121cd58fbc0001LL},
    /* [ 16].c */     {0xe296707121688d5aLL, 0x0260b293a97f0000LL}},
    /* [ 17].a */ {{0x1647d1e78ec02e66LL, 0x5fafcbbb1f780001LL},
    /* [ 17].c */     {0x189ffc4701ff23cbLL, 0x8f8acf6b52fe0000LL}},
    /* [ 18].a */ {{0xa7c982285e72bf8cLL, 0x0c8ddfb63ef00001LL},
    /* [ 18].c */     {0x5141110ab208fb9dLL, 0x61fb47e6a5fc0000LL}},
    /* [ 19].a */ {{0x3eb78ee8fb8c56dbLL, 0xc5d4e06c7de00001LL},
    /* [ 19].c */     {0x3c97caa62540f294LL, 0x8d8d340d4bf80000LL}},
    /* [ 20].a */ {{0x72d03b6f4681f2f9LL, 0xfe8e44d8fbc00001LL},
    /* [ 20].c */     {0x1b25cb9cfe5a0c96LL, 0x3174f91a97f00000LL}},
    /* [ 21].a */ {{0xea85f81e4f502c9bLL, 0xc8ae99b1f7800001LL},
    /* [ 21].c */     {0x0c644570b4a48710LL, 0x3c5436352fe00000LL}},
    /* [ 22].a */ {{0x629c320db08b00c6LL, 0xbfa57363ef000001LL},
    /* [ 22].c */     {0x3d0589c28869472bLL, 0xde517c6a5fc00000LL}},
    /* [ 23].a */ {{0xc5c4b9ce268d074aLL, 0x386be6c7de000001LL},
    /* [ 23].c */     {0xbc95e5ab36477e65LL, 0x534738d4bf800000LL}},
    /* [ 24].a */ {{0xf30bbbbed1596187LL, 0x555bcd8fbc000001LL},
    /* [ 24].c */     {0xddb02ff72a031c01LL, 0x011f71a97f000000LL}},
    /* [ 25].a */ {{0x4a1000fb26c9eedaLL, 0x3cc79b1f78000001LL},
    /* [ 25].c */     {0x2561426086d9acdbLL, 0x6c82e352fe000000LL}},
    /* [ 26].a */ {{0x89fb5307f6bf8ce2LL, 0xc1cf363ef0000001LL},
    /* [ 26].c */     {0x64a788e3c118ed1cLL, 0x8215c6a5fc000000LL}},
    /* [ 27].a */ {{0x830b7b3358a5d67eLL, 0xa49e6c7de0000001LL},
    /* [ 27].c */     {0xe65ea321908627cfLL, 0xa86b8d4bf8000000LL}},
    /* [ 28].a */ {{0xfd8a51da91a69fe1LL, 0xcd3cd8fbc0000001LL},
    /* [ 28].c */     {0x53d27225604d85f9LL, 0xe1d71a97f0000000LL}},
    /* [ 29].a */ {{0x901a48b642b90b55LL, 0xaa79b1f780000001LL},
    /* [ 29].c */     {0xca5ec7a3ed1fe55eLL, 0x07ae352fe0000000LL}},
    /* [ 30].a */ {{0x118cdefdf32144f3LL, 0x94f363ef00000001LL},
    /* [ 30].c */     {0x4daebb2e08533065LL, 0x1f5c6a5fc0000000LL}},
    /* [ 31].a */ {{0x0a88c0a91cff4308LL, 0x29e6c7de00000001LL},
    /* [ 31].c */     {0x9d6f1a00a8f3f76eLL, 0x7eb8d4bf80000000LL}},
    /* [ 32].a */ {{0x433bef4314f16a94LL, 0x53cd8fbc00000001LL},
    /* [ 32].c */     {0x158c62f2b31e496dLL, 0xfd71a97f00000000LL}},
    /* [ 33].a */ {{0xc294b02995ae6738LL, 0xa79b1f7800000001LL},
    /* [ 33].c */     {0x290e84a2eb15fd1fLL, 0xfae352fe00000000LL}},
    /* [ 34].a */ {{0x913575e0da8b16b1LL, 0x4f363ef000000001LL},
    /* [ 34].c */     {0xe3dc1bfbe991a34fLL, 0xf5c6a5fc00000000LL}},
    /* [ 35].a */ {{0x2f61b9f871cf4e62LL, 0x9e6c7de000000001LL},
    /* [ 35].c */     {0xddf540d020b9eadfLL, 0xeb8d4bf800000000LL}},
    /* [ 36].a */ {{0x78d26ccbd68320c5LL, 0x3cd8fbc000000001LL},
    /* [ 36].c */     {0x8ee4950177ce66bfLL, 0xd71a97f000000000LL}},
    /* [ 37].a */ {{0x8b7ebd037898518aLL, 0x79b1f78000000001LL},
    /* [ 37].c */     {0x39e0f787c907117fLL, 0xae352fe000000000LL}},
    /* [ 38].a */ {{0x0b5507b61f78e314LL, 0xf363ef0000000001LL},
    /* [ 38].c */     {0x659d2522f7b732ffLL, 0x5c6a5fc000000000LL}},
    /* [ 39].a */ {{0x4f884628f812c629LL, 0xe6c7de0000000001LL},
    /* [ 39].c */     {0x9e8722938612a5feLL, 0xb8d4bf8000000000LL}},
    /* [ 40].a */ {{0xbe896744d4a98c53LL, 0xcd8fbc0000000001LL},
    /* [ 40].c */     {0xe941a65d66b64bfdLL, 0x71a97f0000000000LL}},
    /* [ 41].a */ {{0xdaf63a553b6318a7LL, 0x9b1f780000000001LL},
    /* [ 41].c */     {0x7b50d19437b097faLL, 0xe352fe0000000000LL}},
    /* [ 42].a */ {{0x2d7a23d8bf06314fLL, 0x363ef00000000001LL},
    /* [ 42].c */     {0x59d7b68e18712ff5LL, 0xc6a5fc0000000000LL}},
    /* [ 43].a */ {{0x392b046a9f0c629eLL, 0x6c7de00000000001LL},
    /* [ 43].c */     {0x4087bab2d5225febLL, 0x8d4bf80000000000LL}},
    /* [ 44].a */ {{0xeb30fbb9c218c53cLL, 0xd8fbc00000000001LL},
    /* [ 44].c */     {0xb470abc03b44bfd7LL, 0x1a97f00000000000LL}},
    /* [ 45].a */ {{0xb9cdc30594318a79LL, 0xb1f7800000000001LL},
    /* [ 45].c */     {0x366630eaba897faeLL, 0x352fe00000000000LL}},
    /* [ 46].a */ {{0x014ab453686314f3LL, 0x63ef000000000001LL},
    /* [ 46].c */     {0xa2dfc77e8512ff5cLL, 0x6a5fc00000000000LL}},
    /* [ 47].a */ {{0x395221c7d0c629e6LL, 0xc7de000000000001LL},
    /* [ 47].c */     {0x1e0d25a14a25feb8LL, 0xd4bf800000000000LL}},
    /* [ 48].a */ {{0x4d972813a18c53cdLL, 0x8fbc000000000001LL},
    /* [ 48].c */     {0x9d50a5d3944bfd71LL, 0xa97f000000000000LL}},
    /* [ 49].a */ {{0x06f9e2374318a79bLL, 0x1f78000000000001LL},
    /* [ 49].c */     {0xbf7ab5eb2897fae3LL, 0x52fe000000000000LL}},
    /* [ 50].a */ {{0xbd220cae86314f36LL, 0x3ef0000000000001LL},
    /* [ 50].c */     {0x925b14e6512ff5c6LL, 0xa5fc000000000000LL}},
    /* [ 51].a */ {{0x36fd3a5d0c629e6cLL, 0x7de0000000000001LL},
    /* [ 51].c */     {0x724cce0ca25feb8dLL, 0x4bf8000000000000LL}},
    /* [ 52].a */ {{0x60def8ba18c53cd8LL, 0xfbc0000000000001LL},
    /* [ 52].c */     {0x1af42d1944bfd71aLL, 0x97f0000000000000LL}},
    /* [ 53].a */ {{0x8d500174318a79b1LL, 0xf780000000000001LL},
    /* [ 53].c */     {0x0f529e32897fae35LL, 0x2fe0000000000000LL}},
    /* [ 54].a */ {{0x48e842e86314f363LL, 0xef00000000000001LL},
    /* [ 54].c */     {0x844e4c6512ff5c6aLL, 0x5fc0000000000000LL}},
    /* [ 55].a */ {{0x4af185d0c629e6c7LL, 0xde00000000000001LL},
    /* [ 55].c */     {0x9f40d8ca25feb8d4LL, 0xbf80000000000000LL}},
    /* [ 56].a */ {{0x7a670ba18c53cd8fLL, 0xbc00000000000001LL},
    /* [ 56].c */     {0x9912b1944bfd71a9LL, 0x7f00000000000000LL}},
    /* [ 57].a */ {{0x86de174318a79b1fLL, 0x7800000000000001LL},
    /* [ 57].c */     {0x9c69632897fae352LL, 0xfe00000000000000LL}},
    /* [ 58].a */ {{0x55fc2e86314f363eLL, 0xf000000000000001LL},
    /* [ 58].c */     {0xe1e2c6512ff5c6a5LL, 0xfc00000000000000LL}},
    /* [ 59].a */ {{0xccf85d0c629e6c7dLL, 0xe000000000000001LL},
    /* [ 59].c */     {0x68058ca25feb8d4bLL, 0xf800000000000000LL}},
    /* [ 60].a */ {{0x1df0ba18c53cd8fbLL, 0xc000000000000001LL},
    /* [ 60].c */     {0x610b1944bfd71a97LL, 0xf000000000000000LL}},
    /* [ 61].a */ {{0x4be174318a79b1f7LL, 0x8000000000000001LL},
    /* [ 61].c */     {0x061632897fae352fLL, 0xe000000000000000LL}},
    /* [ 62].a */ {{0xd7c2e86314f363efLL, 0x0000000000000001LL},
    /* [ 62].c */     {0x1c2c6512ff5c6a5fLL, 0xc000000000000000LL}},
    /* [ 63].a */ {{0xaf85d0c629e6c7deLL, 0x0000000000000001LL},
    /* [ 63].c */     {0x7858ca25feb8d4bfLL, 0x8000000000000000LL}},
    /* [ 64].a */ {{0x5f0ba18c53cd8fbcLL, 0x0000000000000001LL},
    /* [ 64].c */     {0xf0b1944bfd71a97fLL, 0x0000000000000000LL}},
    /* [ 65].a */ {{0xbe174318a79b1f78LL, 0x0000000000000001LL},
    /* [ 65].c */     {0xe1632897fae352feLL, 0x0000000000000000LL}},
    /* [ 66].a */ {{0x7c2e86314f363ef0LL, 0x0000000000000001LL},
    /* [ 66].c */     {0xc2c6512ff5c6a5fcLL, 0x0000000000000000LL}},
    /* [ 67].a */ {{0xf85d0c629e6c7de0LL, 0x0000000000000001LL},
    /* [ 67].c */     {0x858ca25feb8d4bf8LL, 0x0000000000000000LL}},
    /* [ 68].a */ {{0xf0ba18c53cd8fbc0LL, 0x0000000000000001LL},
    /* [ 68].c */     {0x0b1944bfd71a97f0LL, 0x0000000000000000LL}},
    /* [ 69].a */ {{0xe174318a79b1f780LL, 0x0000000000000001LL},
    /* [ 69].c */     {0x1632897fae352fe0LL, 0x0000000000000000LL}},
    /* [ 70].a */ {{0xc2e86314f363ef00LL, 0x0000000000000001LL},
    /* [ 70].c */     {0x2c6512ff5c6a5fc0LL, 0x0000000000000000LL}},
    /* [ 71].a */ {{0x85d0c629e6c7de00LL, 0x0000000000000001LL},
    /* [ 71].c */     {0x58ca25feb8d4bf80LL, 0x0000000000000000LL}},
    /* [ 72].a */ {{0x0ba18c53cd8fbc00LL, 0x0000000000000001LL},
    /* [ 72].c */     {0xb1944bfd71a97f00LL, 0x0000000000000000LL}},
    /* [ 73].a */ {{0x174318a79b1f7800LL, 0x0000000000000001LL},
    /* [ 73].c */     {0x632897fae352fe00LL, 0x0000000000000000LL}},
    /* [ 74].a */ {{0x2e86314f363ef000LL, 0x0000000000000001LL},
    /* [ 74].c */     {0xc6512ff5c6a5fc00LL, 0x0000000000000000LL}},
    /* [ 75].a */ {{0x5d0c629e6c7de000LL, 0x0000000000000001LL},
    /* [ 75].c */     {0x8ca25feb8d4bf800LL, 0x0000000000000000LL}},
    /* [ 76].a */ {{0xba18c53cd8fbc000LL, 0x0000000000000001LL},
    /* [ 76].c */     {0x1944bfd71a97f000LL, 0x0000000000000000LL}},
    /* [ 77].a */ {{0x74318a79b1f78000LL, 0x0000000000000001LL},
    /* [ 77].c */     {0x32897fae352fe000LL, 0x0000000000000000LL}},
    /* [ 78].a */ {{0xe86314f363ef0000LL, 0x0000000000000001LL},
    /* [ 78].c */     {0x6512ff5c6a5fc000LL, 0x0000000000000000LL}},
    /* [ 79].a */ {{0xd0c629e6c7de0000LL, 0x0000000000000001LL},
    /* [ 79].c */     {0xca25feb8d4bf8000LL, 0x0000000000000000LL}},
    /* [ 80].a */ {{0xa18c53cd8fbc0000LL, 0x0000000000000001LL},
    /* [ 80].c */     {0x944bfd71a97f0000LL, 0x0000000000000000LL}},
    /* [ 81].a */ {{0x4318a79b1f780000LL, 0x0000000000000001LL},
    /* [ 81].c */     {0x2897fae352fe0000LL, 0x0000000000000000LL}},
    /* [ 82].a */ {{0x86314f363ef00000LL, 0x0000000000000001LL},
    /* [ 82].c */     {0x512ff5c6a5fc0000LL, 0x0000000000000000LL}},
    /* [ 83].a */ {{0x0c629e6c7de00000LL, 0x0000000000000001LL},
    /* [ 83].c */     {0xa25feb8d4bf80000LL, 0x0000000000000000LL}},
    /* [ 84].a */ {{0x18c53cd8fbc00000LL, 0x0000000000000001LL},
    /* [ 84].c */     {0x44bfd71a97f00000LL, 0x0000000000000000LL}},
    /* [ 85].a */ {{0x318a79b1f7800000LL, 0x0000000000000001LL},
    /* [ 85].c */     {0x897fae352fe00000LL, 0x0000000000000000LL}},
    /* [ 86].a */ {{0x6314f363ef000000LL, 0x0000000000000001LL},
    /* [ 86].c */     {0x12ff5c6a5fc00000LL, 0x0000000000000000LL}},
    /* [ 87].a */ {{0xc629e6c7de000000LL, 0x0000000000000001LL},
    /* [ 87].c */     {0x25feb8d4bf800000LL, 0x0000000000000000LL}},
    /* [ 88].a */ {{0x8c53cd8fbc000000LL, 0x0000000000000001LL},
    /* [ 88].c */     {0x4bfd71a97f000000LL, 0x0000000000000000LL}},
    /* [ 89].a */ {{0x18a79b1f78000000LL, 0x0000000000000001LL},
    /* [ 89].c */     {0x97fae352fe000000LL, 0x0000000000000000LL}},
    /* [ 90].a */ {{0x314f363ef0000000LL, 0x0000000000000001LL},
    /* [ 90].c */     {0x2ff5c6a5fc000000LL, 0x0000000000000000LL}},
    /* [ 91].a */ {{0x629e6c7de0000000LL, 0x0000000000000001LL},
    /* [ 91].c */     {0x5feb8d4bf8000000LL, 0x0000000000000000LL}},
    /* [ 92].a */ {{0xc53cd8fbc0000000LL, 0x0000000000000001LL},
    /* [ 92].c */     {0xbfd71a97f0000000LL, 0x0000000000000000LL}},
    /* [ 93].a */ {{0x8a79b1f780000000LL, 0x0000000000000001LL},
    /* [ 93].c */     {0x7fae352fe0000000LL, 0x0000000000000000LL}},
    /* [ 94].a */ {{0x14f363ef00000000LL, 0x0000000000000001LL},
    /* [ 94].c */     {0xff5c6a5fc0000000LL, 0x0000000000000000LL}},
    /* [ 95].a */ {{0x29e6c7de00000000LL, 0x0000000000000001LL},
    /* [ 95].c */     {0xfeb8d4bf80000000LL, 0x0000000000000000LL}},
    /* [ 96].a */ {{0x53cd8fbc00000000LL, 0x0000000000000001LL},
    /* [ 96].c */     {0xfd71a97f00000000LL, 0x0000000000000000LL}},
    /* [ 97].a */ {{0xa79b1f7800000000LL, 0x0000000000000001LL},
    /* [ 97].c */     {0xfae352fe00000000LL, 0x0000000000000000LL}},
    /* [ 98].a */ {{0x4f363ef000000000LL, 0x0000000000000001LL},
    /* [ 98].c */     {0xf5c6a5fc00000000LL, 0x0000000000000000LL}},
    /* [ 99].a */ {{0x9e6c7de000000000LL, 0x0000000000000001LL},
    /* [ 99].c */     {0xeb8d4bf800000000LL, 0x0000000000000000LL}},
    /* [100].a */ {{0x3cd8fbc000000000LL, 0x0000000000000001LL},
    /* [100].c */     {0xd71a97f000000000LL, 0x0000000000000000LL}},
    /* [101].a */ {{0x79b1f78000000000LL, 0x0000000000000001LL},
    /* [101].c */     {0xae352fe000000000LL, 0x0000000000000000LL}},
    /* [102].a */ {{0xf363ef0000000000LL, 0x0000000000000001LL},
    /* [102].c */     {0x5c6a5fc000000000LL, 0x0000000000000000LL}},
    /* [103].a */ {{0xe6c7de0000000000LL, 0x0000000000000001LL},
    /* [103].c */     {0xb8d4bf8000000000LL, 0x0000000000000000LL}},
    /* [104].a */ {{0xcd8fbc0000000000LL, 0x0000000000000001LL},
    /* [104].c */     {0x71a97f0000000000LL, 0x0000000000000000LL}},
    /* [105].a */ {{0x9b1f780000000000LL, 0x0000000000000001LL},
    /* [105].c */     {0xe352fe0000000000LL, 0x0000000000000000LL}},
    /* [106].a */ {{0x363ef00000000000LL, 0x0000000000000001LL},
    /* [106].c */     {0xc6a5fc0000000000LL, 0x0000000000000000LL}},
    /* [107].a */ {{0x6c7de00000000000LL, 0x0000000000000001LL},
    /* [107].c */     {0x8d4bf80000000000LL, 0x0000000000000000LL}},
    /* [108].a */ {{0xd8fbc00000000000LL, 0x0000000000000001LL},
    /* [108].c */     {0x1a97f00000000000LL, 0x0000000000000000LL}},
    /* [109].a */ {{0xb1f7800000000000LL, 0x0000000000000001LL},
    /* [109].c */     {0x352fe00000000000LL, 0x0000000000000000LL}},
    /* [110].a */ {{0x63ef000000000000LL, 0x0000000000000001LL},
    /* [110].c */     {0x6a5fc00000000000LL, 0x0000000000000000LL}},
    /* [111].a */ {{0xc7de000000000000LL, 0x0000000000000001LL},
    /* [111].c */     {0xd4bf800000000000LL, 0x0000000000000000LL}},
    /* [112].a */ {{0x8fbc000000000000LL, 0x0000000000000001LL},
    /* [112].c */     {0xa97f000000000000LL, 0x0000000000000000LL}},
    /* [113].a */ {{0x1f78000000000000LL, 0x0000000000000001LL},
    /* [113].c */     {0x52fe000000000000LL, 0x0000000000000000LL}},
    /* [114].a */ {{0x3ef0000000000000LL, 0x0000000000000001LL},
    /* [114].c */     {0xa5fc000000000000LL, 0x0000000000000000LL}},
    /* [115].a */ {{0x7de0000000000000LL, 0x0000000000000001LL},
    /* [115].c */     {0x4bf8000000000000LL, 0x0000000000000000LL}},
    /* [116].a */ {{0xfbc0000000000000LL, 0x0000000000000001LL},
    /* [116].c */     {0x97f0000000000000LL, 0x0000000000000000LL}},
    /* [117].a */ {{0xf780000000000000LL, 0x0000000000000001LL},
    /* [117].c */     {0x2fe0000000000000LL, 0x0000000000000000LL}},
    /* [118].a */ {{0xef00000000000000LL, 0x0000000000000001LL},
    /* [118].c */     {0x5fc0000000000000LL, 0x0000000000000000LL}},
    /* [119].a */ {{0xde00000000000000LL, 0x0000000000000001LL},
    /* [119].c */     {0xbf80000000000000LL, 0x0000000000000000LL}},
    /* [120].a */ {{0xbc00000000000000LL, 0x0000000000000001LL},
    /* [120].c */     {0x7f00000000000000LL, 0x0000000000000000LL}},
    /* [121].a */ {{0x7800000000000000LL, 0x0000000000000001LL},
    /* [121].c */     {0xfe00000000000000LL, 0x0000000000000000LL}},
    /* [122].a */ {{0xf000000000000000LL, 0x0000000000000001LL},
    /* [122].c */     {0xfc00000000000000LL, 0x0000000000000000LL}},
    /* [123].a */ {{0xe000000000000000LL, 0x0000000000000001LL},
    /* [123].c */     {0xf800000000000000LL, 0x0000000000000000LL}},
    /* [124].a */ {{0xc000000000000000LL, 0x0000000000000001LL},
    /* [124].c */     {0xf000000000000000LL, 0x0000000000000000LL}},
    /* [125].a */ {{0x8000000000000000LL, 0x0000000000000001LL},
    /* [125].c */     {0xe000000000000000LL, 0x0000000000000000LL}},
    /* [126].a */ {{0x0000000000000000LL, 0x0000000000000001LL},
    /* [126].c */     {0xc000000000000000LL, 0x0000000000000000LL}},
    /* [127].a */ {{0x0000000000000000LL, 0x0000000000000001LL},
    /* [127].c */     {0x8000000000000000LL, 0x0000000000000000LL}}
};


/* hex_to_u16 - convert a hexadecimal string to a u16
 */
u16 hex_to_u16(const char *s)
{
    u16         result;
    size_t      len;
    char        sub[16 + 1];
    u8          temp;

    result.hi8 = 0;
    result.lo8 = 0;
    len = strlen(s);
    if (len <= 16)
    {
        sscanf(s, "%llx", &temp);
        result.lo8 = temp;
    }
    else
    {
        strncpy(sub, s + len - 16, 16);
        sscanf(sub, "%llx", &temp);
        result.lo8 = temp;

        if (len <= 32)
        {
            strncpy(sub, s, len - 16);
            sub[len - 16] = '\0';
            sscanf(sub, "%llx", &temp);
            result.hi8 = temp;
        }
        else
        {
            /* this actually an error to have more that 32 hex digits.
             * but only look at low-order 32 digits.
             */
            strncpy(sub, s + len - 32, 16);
            sscanf(sub, "%llx", &temp);
            result.hi8 = temp;
        }
    }
    return (result);
}


/* dec_to_u16 - convert a (unsigned) decimal string to a u16
 */
u16 dec_to_u16(const char *s)
{
    u16         result;
    u16         temp;
    const char  *p;

    result.hi8 = 0;
    result.lo8 = 0;
    for (p = s; *p >= '0' && *p <= '9'; p++)
    {
        /* another digit, multiply result so far by 10
         * the multiplication is done by adding the left shift by 3 (times 8)
         * to the left shift by 1 (times 2).
         */
        temp = result;
        result.hi8 <<= 3;
        result.hi8 += (result.lo8 & 0xE000000000000000ULL) >> 61;
        result.lo8 <<= 3;
        temp.hi8 <<= 1;
        temp.hi8 += (temp.lo8 & 0x8000000000000000ULL) >> 63;
        temp.lo8 <<= 1;
        result = add16(result, temp);  /* add the times 8 to the times 2 */

        /* add in new digit */
        temp.hi8 = 0;
        temp.lo8 = *p - '0';
        result = add16(result, temp);
    }
    return (result);
}


/* u16_to_hex - convert a u16 to a hexadecimal string and write that string
 *              into the provided buffer which must be at least 33 bytes long.
 */
char *u16_to_hex(u16 k, char *buf)
{
    char        *p;

    if (k.hi8 == 0)
        sprintf(buf, "%llx", k.lo8);
    else
    {
        sprintf(buf, "%llx", k.hi8);
        p = buf + strlen(buf);
        sprintf(p, "%016llx", k.lo8);
    }
    return (buf);
}

static u8 hi2loQuot[10] = {
    0ULL,
    1844674407370955161ULL,
    3689348814741910323ULL,
    5534023222112865484ULL,
    7378697629483820646ULL,
    9223372036854775808ULL,
    11068046444225730969ULL,
    12912720851596686131ULL,
    14757395258967641292ULL,
    16602069666338596454ULL
};

static u8 hi2loMod[10] = {
    0,
    6,
    2,
    8,
    4,
    0,
    6,
    2,
    8,
    4
};

/* u16_to_dec - convert a u16 to a decimal string and write that string
 *              into the provided buffer
 * Args:
 *    k -   the u16 number to be converted to a decimal string.
 *    buf - pointer to a character buffer of size U16_ASCII_BUF_SIZE.
 * Returns:
 *    pointer the "buf" arg which contains the decimal string.
 */
char *u16_to_dec(u16 k, char *buf)
{
    u8          hi8 = k.hi8;
    u8          lo8 = k.lo8;
    int         himod;
    int         lomod;
    char        temp[U16_ASCII_BUF_SIZE];
    char        *digit = temp;
    char        *tail;

    while (hi8)
    {
        himod = (int)(hi8 % 10);
        hi8 /= 10;
        lomod = (int)(lo8 % 10);
        lo8 /= 10;

        lo8 += hi2loQuot[himod] ;
        lomod += (int)hi2loMod[himod];

        if (lomod >= 10)       /* if adding to 2 mods caused a "carry" */
        {
            lomod -= 10;
            lo8 += 1;
        }
        *digit++ = '0' + lomod;
    }
    sprintf(buf, "%llu", lo8);
    /* concatenate low order digits computed before hi8 was reduced to 0 */
    tail = buf + strlen(buf);
    while (digit > temp)
        *tail++ = *--digit;
    *tail = '\0';
    return (buf);
}


/* mult16 - multiply two 16-byte numbers
 */
u16 mult16(u16 a, u16 b)
{
    u16 prod;
    u8  ahi4, alow4, bhi4, blow4, temp;
    u8  reshibit, hibit0, hibit1;

    prod.hi8 = 0;

    ahi4 = a.lo8 >> 32;        /* get hi 4 bytes of the low 8 bytes */
    alow4 = (a.lo8 & 0xFFFFFFFFLL);  /* get low 4 bytes of the low 8 bytes */
    bhi4 = b.lo8 >> 32;        /* get hi 4 bytes of the low 8 bytes */
    blow4 = (b.lo8 & 0xFFFFFFFFLL);  /* get low 4 bytes of the low 8 bytes */

    /* assign 8-byte product of the lower 4 bytes of "a" and the lower 4 bytes
     * of "b" to the lower 8 bytes of the result product.
     */
    prod.lo8 = alow4 * blow4;
 
    temp = ahi4 * blow4; /* mult high 4 bytes of "a" by low 4 bytes of "b" */
    prod.hi8 += temp >> 32; /* add high 4 bytes to high 8 result bytes*/
    temp <<= 32;     /* get lower half ready to add to lower 8 result bytes */
    hibit0 = (temp & 0x8000000000000000LL);
    hibit1 = (prod.lo8 & 0x8000000000000000LL);
    prod.lo8 += temp;
    reshibit = (prod.lo8 & 0x8000000000000000LL);
    if ((hibit0 & hibit1) || ((hibit0 ^ hibit1) && reshibit == 0LL))
        prod.hi8++;  /* add carry bit */
 
    temp = alow4 * bhi4; /* mult low 4 bytes of "a" by high 4 bytes of "b" */
    prod.hi8 += temp >> 32; /* add high 4 bytes to high 8 result bytes*/
    temp <<= 32;     /* get lower half ready to add to lower 8 result bytes */
    hibit0 = (temp & 0x8000000000000000LL);
    hibit1 = (prod.lo8 & 0x8000000000000000LL);
    prod.lo8 += temp;
    reshibit = (prod.lo8 & 0x8000000000000000LL);
    if ((hibit0 & hibit1) || ((hibit0 ^ hibit1) && reshibit == 0LL))
        prod.hi8++;  /* add carry bit */

    prod.hi8 += ahi4 * bhi4;
    prod.hi8 += a.lo8 * b.hi8;
    prod.hi8 += a.hi8 * b.lo8;
    return (prod);    
}


/* add16 - add two 16-byte numbers
 */
u16 add16(u16 a, u16 b)
{
    u16 sum;
    u8  reshibit, hibit0, hibit1;

    sum.hi8 = a.hi8 + b.hi8;

    hibit0 = (a.lo8 & 0x8000000000000000LL);
    hibit1 = (b.lo8 & 0x8000000000000000LL);
    sum.lo8 = a.lo8 + b.lo8;
    reshibit = (sum.lo8 & 0x8000000000000000LL);
    if ((hibit0 & hibit1) || ((hibit0 ^ hibit1) && reshibit == 0LL))
        sum.hi8++;  /* add carry bit */
    return (sum);    
}


/* skip_ahead_rand - generate the random number that is "advance" steps
 *              from an initial random number of 0.  This is done by
 *              starting with 0, and then advancing the by the
 *              appropriate powers of 2 of the linear congruential
 *              generator.
 */
u16 skip_ahead_rand(u16 advance)
{
    int         i;
    u16         new;
    u8          bit_map;

    new.hi8 = 0;
    new.lo8 = 0;
    bit_map = advance.lo8;
    for (i = 0; bit_map && i < 64; i++)
    {
        if (bit_map & ((u8)1 << i))
        {
            /* advance random number by f**(2**i) (x)
             */
            new = add16(mult16(new, Gen[i].a), Gen[i].c);
            bit_map &= ~((u8)1 << i);
        }
    }
    bit_map = advance.hi8;
    for (i = 0; bit_map && i < 64; i++)
    {
        if (bit_map & ((u8)1 << i))
        {
            /* advance random number by f**(2**(i + 64)) (x)
             */
            new = add16(mult16(new, Gen[i + 64].a), Gen[i + 64].c);
            bit_map &= ~((u8)1 << i);
        }
    }
    return (new);
}


/* next_rand - the sequential 128-bit random number generator.
 */
u16 next_rand(u16 rand)
{
    /* advance the random number forward once using the linear congruential
     * generator, and then return the new random number
     */
    rand = add16(mult16(rand, Gen[0].a), Gen[0].c);
    return (rand);
}
