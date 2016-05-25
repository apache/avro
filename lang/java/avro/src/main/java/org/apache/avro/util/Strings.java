/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.util;

import com.google.common.base.Charsets;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.cs.ArrayDecoder;
import sun.nio.cs.ArrayEncoder;

/**
 *
 * @author zfarkas
 */
public final class Strings {

  private Strings() { }

    public static String decode(final CharsetDecoder cd, final byte[] ba, final int off, final int len) {
        if (len == 0) {
            return "";
        }
        int en = (int) (len * (double) cd.maxCharsPerByte());
        char[] ca = Arrays.getCharsTmp(en);
        if (cd instanceof ArrayDecoder) {
            int clen = ((ArrayDecoder) cd).decode(ba, off, len, ca);
            return new String(ca, 0, clen);
        }
        cd.reset();
        ByteBuffer bb = ByteBuffer.wrap(ba, off, len);
        CharBuffer cb = CharBuffer.wrap(ca);
        try {
            CoderResult cr = cd.decode(bb, cb, true);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
            cr = cd.flush(cb);
            if (!cr.isUnderflow()) {
                cr.throwException();
            }
        } catch (CharacterCodingException x) {
            throw new Error(x);
        }
        return new String(ca, 0, cb.position());
    }


    public static final Charset UTF_8 = Charset.forName("UTF-8");

    private static final ThreadLocal<CharsetDecoder> UTF8_DECODER = new ThreadLocal<CharsetDecoder>() {

        @Override
        protected CharsetDecoder initialValue() {
            return UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPLACE)
                    .onUnmappableCharacter(CodingErrorAction.REPLACE);
        }

    };

    private static final ThreadLocal<CharsetEncoder> UTF8_ENCODER = new ThreadLocal<CharsetEncoder>() {

        @Override
        protected CharsetEncoder initialValue() {
            return Charsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPLACE)
                    .onUnmappableCharacter(CodingErrorAction.REPLACE);
        }

    };

    public static CharsetEncoder getUTF8CharsetEncoder() {
        return UTF8_ENCODER.get();
    }


    public static String fromUtf8(final byte[] bytes) {
        return decode(UTF8_DECODER.get(), bytes, 0, bytes.length);
    }

    public static String fromUtf8(final byte[] bytes, final int startIdx, final int length) {
        return decode(UTF8_DECODER.get(), bytes, startIdx, length);
    }

    public static int getmaxNrBytes(final CharsetEncoder ce, final int nrChars) {
        return (int) (nrChars * (double) ce.maxBytesPerChar());
    }

    private static final Logger LOG = LoggerFactory.getLogger(Strings.class);

    private static final Field CHARS_FIELD;

    static {
        CHARS_FIELD = AccessController.doPrivileged(new PrivilegedAction<Field>() {
            @Override
            public Field run() {
                Field charsField;
                try {
                    charsField = String.class.getDeclaredField("value");
                    charsField.setAccessible(true);
                } catch (NoSuchFieldException ex) {
                    LOG.info("char array stealing from String not supported", ex);
                    charsField = null;
                } catch (SecurityException ex) {
                    throw new RuntimeException(ex);
                }
                return charsField;
            }
        });
    }

    /**
     * Steal the underlying character array of a String.
     *
     * @param str
     * @return
     */
    public static char[] steal(final String str) {
        if (CHARS_FIELD == null) {
            return str.toCharArray();
        } else {
            try {
                return (char[]) CHARS_FIELD.get(str);
            } catch (IllegalArgumentException ex) {
                throw new RuntimeException(ex);
            } catch ( IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static byte[] encode(final CharsetEncoder ce, final char[] ca, final int off, final int len) {
        if (len == 0) {
            return Arrays.EMPTY_BYTE_ARRAY;
        }
        byte[] ba = Arrays.getBytesTmp(getmaxNrBytes(ce, len));
        int nrBytes = encode(ce, ca, off, len, ba);
        return java.util.Arrays.copyOf(ba, nrBytes);
    }


    public static int encode(final CharsetEncoder ce, final char[] ca, final int off, final int len,
            final byte[] targetArray) {
        if (len == 0) {
            return 0;
        }
        if (ce instanceof ArrayEncoder) {
            return ((ArrayEncoder) ce).encode(ca, off, len, targetArray);
        } else {
            ce.reset();
            ByteBuffer bb = ByteBuffer.wrap(targetArray);
            CharBuffer cb = CharBuffer.wrap(ca, off, len);
            try {
                CoderResult cr = ce.encode(cb, bb, true);
                if (!cr.isUnderflow()) {
                    cr.throwException();
                }
                cr = ce.flush(bb);
                if (!cr.isUnderflow()) {
                    cr.throwException();
                }
            } catch (CharacterCodingException x) {
                throw new Error(x);
            }
            return bb.position();
        }
    }


}
