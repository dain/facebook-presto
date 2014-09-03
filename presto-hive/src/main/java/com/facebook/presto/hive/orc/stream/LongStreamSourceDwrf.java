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
package com.facebook.presto.hive.orc.stream;

import com.facebook.presto.hive.orc.metadata.Type;
import com.google.common.io.ByteSource;

import java.io.IOException;

public class LongStreamSourceDwrf
        implements StreamSource<LongStream>
{
    private final ByteSource byteSource;
    private final Type.Kind type;
    private final boolean signed;
    private final boolean usesVInt;
    private final int valueSkipSize;

    public LongStreamSourceDwrf(ByteSource byteSource, Type.Kind type, boolean signed, boolean usesVInt, int valueSkipSize)
    {
        this.byteSource = byteSource;
        this.type = type;
        this.signed = signed;
        this.usesVInt = usesVInt;
        this.valueSkipSize = valueSkipSize;
    }

    @Override
    public LongStreamDwrf openStream()
            throws IOException
    {
        LongStreamDwrf longStream = new LongStreamDwrf(byteSource.openStream(), type, signed, usesVInt);
        if (valueSkipSize > 0) {
            longStream.skip(valueSkipSize);
        }
        return longStream;
    }
}
