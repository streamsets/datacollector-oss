/*
 * Copyright 2017 StreamSets Inc.
 *
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

package com.streamsets.pipeline.lib.parser.net.netflow.v9;

public class NetflowV9FieldTemplate {

  private final NetflowV9FieldType type;

  private final int typeId;
  private final int length;

  public NetflowV9FieldTemplate(int typeId, int length) {
    this(NetflowV9FieldType.getTypeForId(typeId), typeId, length);
  }

  public static NetflowV9FieldTemplate getScopeFieldTemplate(int scopeTypeId, int length) {
    return new NetflowV9FieldTemplate(NetflowV9FieldType.getScopeTypeForId(scopeTypeId), scopeTypeId, length);
  }

  public NetflowV9FieldTemplate(NetflowV9FieldType type, int typeId, int length) {
    this.type = type;
    this.typeId = typeId;
    this.length = length;
  }

  public NetflowV9FieldType getType() {
    return type;
  }

  public int getTypeId() {
    return typeId;
  }

  public int getLength() {
    return length;
  }
}
