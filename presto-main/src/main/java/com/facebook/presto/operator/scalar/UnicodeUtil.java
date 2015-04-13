/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;

/**
 * Ideas for fast counting based on
 * <a href="http://www.daemonology.net/blog/2008-06-05-faster-utf8-strlen.html">Even faster UTF-8 character counting</a>.
 */
final class UnicodeUtil
{
    private UnicodeUtil() {}

    private static final int TOP_MASK32 = 0x80808080;
    private static final long TOP_MASK64 = 0x8080808080808080L;

    private static final int ONE_MULTIPLIER32 = 0x01010101;
    private static final long ONE_MULTIPLIER64 = 0x0101010101010101L;

    /**
     * Counts the code points within UTF8 encoded slice.
     */
    static int countCodePoints(Slice string)
    {
        return countCodePoints(string, string.length());
    }

    /**
     * Counts the code points within UTF8 encoded slice up to {@code end}.
     */
    static int countCodePoints(Slice string, int end)
    {
        // Quick exit if empty string
        if (end == 0) {
            return 0;
        }

        int count = 0;
        int i = 0;
        // Length rounded to 8 bytes
        int length8 = end & 0x7FFFFFF8;
        for (; i < length8; i += 8) {
            // Fetch 8 bytes as long
            long i64 = string.getLong(i);
            // Count bytes which are NOT the start of a code point
            i64 = ((i64 & TOP_MASK64) >>> 7) & (~i64 >>> 6);
            count += (i64 * ONE_MULTIPLIER64) >>> 56;
        }
        // Enough bytes left for 32 bits?
        if (i + 4 < end) {
            // Fetch 4 bytes as integer
            int i32 = string.getInt(i);
            // Count bytes which are NOT the start of a code point
            i32 = ((i32 & TOP_MASK32) >>> 7) & (~i32 >>> 6);
            count += (i32 * ONE_MULTIPLIER32) >>> 24;

            i += 4;
        }
        // Do the rest one by one
        for (; i < end; i++) {
            int i8 = string.getByte(i) & 0xff;
            // Count bytes which are NOT the start of a code point
            count += (i8 >>> 7) & (~i8 >>> 6);
        }

        assert count <= end;
        return end - count;
    }

    /**
     * Finds the index of the first byte of the code point at a position.
     */
    static int findUtf8IndexOfCodePointPosition(Slice string, int codePointPosition)
    {
        // Quick exit if we are sure that the position is after the end
        if (string.length() <= codePointPosition) {
            return string.length();
        }

        int correctIndex = codePointPosition;
        int i = 0;
        // Length rounded to 8 bytes
        int length8 = string.length() & 0x7FFFFFF8;
        // While we have enough bytes left and we need at least 8 characters process 8 bytes at once
        while (i < length8 && correctIndex >= i + 8) {
            // Fetch 8 bytes as long
            long i64 = string.getLong(i);
            // Count bytes which are NOT the start of a code point
            i64 = ((i64 & TOP_MASK64) >>> 7) & (~i64 >>> 6);
            correctIndex += ((i64 * ONE_MULTIPLIER64) >>> 56);

            i += 8;
        }
        // Length rounded to 4 bytes
        int length4 = string.length() & 0x7FFFFFFC;
        // While we have enough bytes left and we need at least 4 characters process 4 bytes at once
        while (i < length4 && correctIndex >= i + 4) {
            // Fetch 4 bytes as integer
            int i32 = string.getInt(i);
            // Count bytes which are NOT the start of a code point
            i32 = ((i32 & TOP_MASK32) >>> 7) & (~i32 >>> 6);
            correctIndex += ((i32 * ONE_MULTIPLIER32) >>> 24);

            i += 4;
        }
        // Do the rest one by one, always check the last byte to find the end of the code point
        while (i < string.length()) {
            int i8 = string.getByte(i) & 0xff;
            // Count bytes which are NOT the start of a code point
            correctIndex += ((i8 >>> 7) & (~i8 >>> 6));
            if (i == correctIndex) {
                return i;
            }

            i++;
        }

        assert i == string.length();
        return string.length();
    }

    /**
     * Find substring within UTF-8 encoding string.
     */
    static int findUtf8IndexOfString(Slice string, int start, int end, Slice substring)
    {
        if (substring.length() == 0) {
            return 0;
        }

        int lastValidIndex = end - substring.length();
        // Do we have enough characters
        if (substring.length() < SizeOf.SIZE_OF_INT || string.length() < SizeOf.SIZE_OF_LONG) {
            // Use the slow compare
            for (int i = start; i <= lastValidIndex; i++) {
                if (string.equals(i, substring.length(), substring, 0, substring.length())) {
                    return i;
                }
            }

            return -1;
        }
        // Using first four bytes for faster search. We are not using eight bytes for long
        // because we want more strings to get use of fast search.
        int head = substring.getInt(0);
        // Take the first byte of head for faster skipping
        // Working as long as presto requires LITTLE_ENDIAN (see PrestoJvmRequirements)
        int firstByteMask = head & 0xff;
        firstByteMask |= firstByteMask << 8;
        firstByteMask |= firstByteMask << 16;

        for (int i = start; i <= lastValidIndex; ) {
            // Read four bytes in sequence
            int value = string.getInt(i);
            // Compare all bytes of value with first byte of search string
            int valueXor = value ^ firstByteMask;
            int hasZeroBytes = (valueXor - 0x01010101) & ~valueXor & 0x80808080;
            // If valueXor doest not have any zero byte then there is no match and we can advance
            if (hasZeroBytes == 0) {
                i += SizeOf.SIZE_OF_INT;
                continue;
            }
            // Try fast match of head and the rest
            if (value == head && string.equals(i, substring.length(), substring, 0, substring.length())) {
                return i;
            }

            i++;
        }

        return -1;
    }

    /**
     * Method returns the length of code point by examine the start byte of a code point.
     */
    static int lengthOfCodePoint(int ch)
    {
        if (ch < 0x80) {
            // normal ASCII
            return 1;
        }
        else if (ch < 0xc0) {
            // 10xxxxxx -- illegal as start
            throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "illegal start 0x" + Integer.toHexString(ch) + " of code point");
        }
        else if (ch < 0xe0) {
            // 110xxxxx 10xxxxxx
            return 2;
        }
        else if (ch < 0xf0) {
            // 1110xxxx 10xxxxxx 10xxxxxx
            return 3;
        }
        else if (ch < 0xf8) {
            // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
            return 4;
        }
        // According to RFC3629 limited to 4 bytes so 5 and 6 bytes are illegal
        throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "illegal start 0x" + Integer.toHexString(ch) + " of code point");
    }

    /**
     * Test if the {@code string} is empty.
     */
    static boolean isEmpty(Slice string)
    {
        return string.length() == 0;
    }
}
