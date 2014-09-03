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
package com.facebook.presto.hive.orc.metadata;

import com.google.common.base.Objects;

import java.util.List;

public class Footer
{
    private final long rowIndexStride;
    private final List<StripeInformation> stripes;
    private final List<Type> types;

    public Footer(long rowIndexStride, List<StripeInformation> stripes, List<Type> types)
    {
        this.rowIndexStride = rowIndexStride;
        this.stripes = stripes;
        this.types = types;
    }

    public long getRowIndexStride()
    {
        return rowIndexStride;
    }

    public List<StripeInformation> getStripes()
    {
        return stripes;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("rowIndexStride", rowIndexStride)
                .add("stripes", stripes)
                .add("types", types)
                .toString();
    }
}
