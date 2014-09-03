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

import com.google.common.io.ByteSource;

import java.io.IOException;

public class LongStreamSourceV2
        implements StreamSource<LongStream>
{
    private final ByteSource byteSource;
    private final boolean signed;
    private final int valueSkipSize;

    public LongStreamSourceV2(ByteSource byteSource, boolean signed, int valueSkipSize)
    {
        this.byteSource = byteSource;
        this.signed = signed;
        this.valueSkipSize = valueSkipSize;
    }

    @Override
    public LongStreamV2 openStream()
            throws IOException
    {
        LongStreamV2 longStream = new LongStreamV2(byteSource.openStream(), signed, false);
        if (valueSkipSize > 0) {
            longStream.skip(valueSkipSize);
        }
        return longStream;
    }
}
