/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class NullDeserializer<T> extends JsonDeserializer<T> {

  public static class Object extends NullDeserializer<Object> {
  }

  @Override
  public T deserialize(JsonParser jp, DeserializationContext ctxt) {
    return null;
  }

}
