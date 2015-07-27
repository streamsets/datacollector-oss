/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.streamsets.pipeline.api.impl.ErrorMessage;

import java.io.IOException;
import java.util.Map;

public class ErrorMessageDeserializer extends JsonDeserializer<ErrorMessage> {

  @Override
  public ErrorMessage deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
    ErrorMessage errorMessage = null;
    Map map = jp.readValueAs(Map.class);
    if (map != null) {
      errorMessage = new ErrorMessage((String) map.get("errorCode"), (String) map.get("nonLocalized"),
                                      (long) map.get("timestamp"));
    }
    return errorMessage;
  }
}
