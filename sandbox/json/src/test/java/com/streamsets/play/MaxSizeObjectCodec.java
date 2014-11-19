/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.play;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.ResolvedType;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.util.Iterator;

public class MaxSizeObjectCodec extends ObjectCodec {
  private final ObjectCodec codec;
  private final int maxSize;

  protected MaxSizeObjectCodec(ObjectCodec codec, int maxSize) {
    this.codec = codec;
    this.maxSize = maxSize;
  }

  @Override
  public Version version() {
    return codec.version();
  }

  @Override
  public <T> T readValue(JsonParser jp, Class<T> valueType) throws IOException, JsonProcessingException {
    return codec.readValue(jp, valueType);
  }

  @Override
  public <T> T readValue(JsonParser jp,
      TypeReference<?> valueTypeRef) throws IOException, JsonProcessingException {
    return codec.readValue(jp, valueTypeRef);
  }

  @Override
  public <T> T readValue(JsonParser jp, ResolvedType valueType) throws IOException, JsonProcessingException {
    return codec.readValue(jp, valueType);
  }

  @Override
  public <T> Iterator<T> readValues(JsonParser jp, Class<T> valueType) throws IOException, JsonProcessingException {
    return codec.readValues(jp, valueType);
  }

  @Override
  public <T> Iterator<T> readValues(JsonParser jp,
      TypeReference<?> valueTypeRef) throws IOException, JsonProcessingException {
    return codec.readValues(jp, valueTypeRef);
  }

  @Override
  public <T> Iterator<T> readValues(JsonParser jp,
      ResolvedType valueType) throws IOException, JsonProcessingException {
    return codec.readValues(jp, valueType);
  }

  @Override
  public void writeValue(JsonGenerator jgen, Object value) throws IOException, JsonProcessingException {
    codec.writeValue(jgen, value);
  }

  @Override
  public <T extends TreeNode> T readTree(JsonParser jp) throws IOException, JsonProcessingException {
    return codec.readTree(jp);
  }

  @Override
  public void writeTree(JsonGenerator jg, TreeNode tree) throws IOException, JsonProcessingException {
    codec.writeTree(jg, tree);
  }

  @Override
  public TreeNode createObjectNode() {
    return codec.createObjectNode();
  }

  @Override
  public TreeNode createArrayNode() {
    return codec.createArrayNode();
  }

  @Override
  public JsonParser treeAsTokens(TreeNode n) {
    return codec.treeAsTokens(n);
  }

  @Override
  public <T> T treeToValue(TreeNode n, Class<T> valueType) throws JsonProcessingException {
    return codec.treeToValue(n, valueType);
  }

  @Override
  @Deprecated
  public JsonFactory getJsonFactory() {
    return codec.getJsonFactory();
  }

  @Override
  public JsonFactory getFactory() {
    return codec.getFactory();
  }
}
