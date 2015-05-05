/*
 * Copyright 2015 The Apache Software Foundation.
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
package org.apache.avro;

/**
 *
 * @author zoly
 */
public final class UnresolvedSchema extends Schema {

    private final String typeName;

    public UnresolvedSchema(final String typeName) {
        super(Schema.Type.NULL);
        this.typeName = typeName;
    }

    @Override
    public String getFullName() {
        return typeName;
    }

    @Override
    public String getName() {
        if (typeName.contains(".")) {
            int ldot = typeName.lastIndexOf('.');
            return typeName.substring(ldot + 1);
        } else {
            return typeName;
        }
    }




}
