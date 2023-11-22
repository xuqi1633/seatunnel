/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.api.table.type;

public class JsonType implements SeaTunnelDataType<JsonType> {
    public static final JsonType INSTANCE = new JsonType();

    private JsonType() {}

    @Override
    public Class<JsonType> getTypeClass() {
        return JsonType.class;
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.JSON;
    }

    @Override
    public int hashCode() {
        return JsonType.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        return obj instanceof JsonType;
    }

    @Override
    public String toString() {
        return SqlType.JSON.toString();
    }
}
