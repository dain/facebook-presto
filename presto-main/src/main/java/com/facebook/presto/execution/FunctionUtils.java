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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Signature;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class FunctionUtils
{
    public static final String FUNCTION_SESSION_PREFIX = "$PRESTO_FUNCTION$$";

    private FunctionUtils()
    {
    }

    @NotNull
    public static String getFunctionSessionPropertyName(Signature signature)
    {
        return FUNCTION_SESSION_PREFIX + signature;
    }

    public static String encodeFunctionSessionProperty(String value)
    {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(value.length());
            DeflaterOutputStream deflate = new DeflaterOutputStream(out);
            deflate.write(value.getBytes(UTF_8));
            deflate.close();
            byte[] compressed = out.toByteArray();
            return Base64.getEncoder().encodeToString(compressed);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static String decodeFunctionSessionProperty(String value)
    {
        try {
            byte[] compressed = Base64.getDecoder().decode(value);
            InflaterInputStream deflate = new InflaterInputStream(new ByteArrayInputStream(compressed));
            byte[] uncompressed = ByteStreams.toByteArray(deflate);
            return new String(uncompressed, UTF_8);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static Map<String, String> getSignatureToProperty(Session session)
    {
        ImmutableMap.Builder<String, String> signatureToProperty = ImmutableMap.builder();
        session.getSystemProperties().keySet().stream()
                .filter(name -> name.startsWith(FUNCTION_SESSION_PREFIX))
                .forEach(name -> signatureToProperty.put(name.substring(FUNCTION_SESSION_PREFIX.length()), name));
        return signatureToProperty.build();
    }
}
