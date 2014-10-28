package com.streamsets.pipeline.serde;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.config.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by harikiran on 10/23/14.
 */
public class StageConfigurationDeserializer {

  public static StageRegistry deserialize(InputStream input) throws IOException {
    com.fasterxml.jackson.core.JsonParser jsonParser = new JsonFactory().createParser(input);

    StageRegistry stageRegistry = new StageRegistry();
    String name = null, version = null, label = null, description = null, type = null;
    List<ConfigDefinition> configDefinitions = null;
    String fieldName;

    while(jsonParser.nextToken() != JsonToken.END_ARRAY) {
      while(jsonParser.nextToken() != JsonToken.END_OBJECT) {
        fieldName = jsonParser.getCurrentName();
        if ("name".equals(fieldName)) {
          jsonParser.nextToken();
          name = jsonParser.getValueAsString();
        } else if ("version".equals(fieldName)) {
          jsonParser.nextToken();
          version = jsonParser.getText();
        } else if ("label".equals(fieldName)) {
          jsonParser.nextToken();
          label = jsonParser.getText();
        } else if ("description".equals(fieldName)) {
          jsonParser.nextToken();
          description = jsonParser.getText();
        } else if ("type".equals(fieldName)) {
          jsonParser.nextToken();
          type = jsonParser.getText();
        } else if ("ConfigOptions".equals(fieldName)) {
          configDefinitions = readConfigOptionArray(jsonParser);
        }
      }
      stageRegistry.getStageDefinitions().add(
        new StageDefinition(name, version, label,
          description, StageType.valueOf(type), configDefinitions)
      );
    }

    return stageRegistry;
  }

  private static List<ConfigDefinition> readConfigOptionArray(JsonParser jsonParser) throws IOException {
    List<ConfigDefinition> configDefinitions = new ArrayList<ConfigDefinition>();
    String fieldName;
    String name = null;
    String description = null;
    String defaultValue = null;
    String shortDescription = null;
    String type = null;
    String mandatory = null;
    String group = null;

    // iterate through the array until token equal to "]"
    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        fieldName = jsonParser.getCurrentName();
        if ("name".equals(fieldName)) {
          jsonParser.nextToken();
          name = jsonParser.getText();
        } else if ("description".equals(fieldName)) {
          jsonParser.nextToken();
          description = jsonParser.getText();
        } else if ("label".equals(fieldName)) {
          jsonParser.nextToken();
          shortDescription = jsonParser.getText();
        } else if ("defaultValue".equals(fieldName)) {
          jsonParser.nextToken();
          defaultValue = jsonParser.getText();
        } else if ("type".equals(fieldName)) {
          jsonParser.nextToken();
          type = jsonParser.getText();
        } else if ("required".equals(fieldName)) {
          jsonParser.nextToken();
          mandatory = jsonParser.getText();
        } else if ("group".equals(fieldName)) {
          jsonParser.nextToken();
          group = jsonParser.getText();
        }
      }
      configDefinitions.add(new ConfigDefinition(name,
        ConfigDef.Type.valueOf(type),
        shortDescription,
        description,
        defaultValue,
        "true".equals(mandatory)? true: false,
        group));
    }
    return configDefinitions;
  }
}
