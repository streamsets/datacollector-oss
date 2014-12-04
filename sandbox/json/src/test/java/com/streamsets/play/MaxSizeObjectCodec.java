/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
