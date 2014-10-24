package com.streamsets.pipeline.sdk;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by harikiran on 10/23/14.
 */
public class SerializationUtil {

  public static void serialize(StageCollection stageCollection, OutputStream outputStream) throws IOException {

    JsonFactory jsonFactory = new JsonFactory();
    JsonGenerator jsonGenerator = jsonFactory.createGenerator(outputStream);

    //set default Pretty printer
    DefaultPrettyPrinter p = new DefaultPrettyPrinter();
    p.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter());
    jsonGenerator.setPrettyPrinter(p);

    //serialization logic
    jsonGenerator.writeStartArray();

    for(StageConfiguration stageInfo : stageCollection.getStageConfigurations()) {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("name", stageInfo.getStageOptions().get("name"));
      jsonGenerator.writeStringField("version", stageInfo.getStageOptions().get("version"));
      jsonGenerator.writeStringField("label", stageInfo.getStageOptions().get("label"));
      jsonGenerator.writeStringField("description", stageInfo.getStageOptions().get("description"));
      jsonGenerator.writeStringField("type", stageInfo.getStageOptions().get("type"));

      if (!stageInfo.getConfigOptions().isEmpty()) {
        jsonGenerator.writeArrayFieldStart("ConfigOptions"); //start option groups array
        for (Map<String, String> option : stageInfo.getConfigOptions()) {
          jsonGenerator.writeStartObject();
          jsonGenerator.writeStringField("name", option.get("name"));
          jsonGenerator.writeStringField("description", option.get("description"));
          jsonGenerator.writeStringField("label", option.get("label"));
          jsonGenerator.writeStringField("defaultValue", option.get("defaultValue"));
          jsonGenerator.writeStringField("type", option.get("type"));
          jsonGenerator.writeStringField("required", option.get("required"));
          //jsonGenerator.writeStringField("group", option.get("group"));
          jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
      }
      jsonGenerator.writeEndObject();
    }
    jsonGenerator.writeEndArray();

    jsonGenerator.flush();
    jsonGenerator.close();
  }

  public static StageCollection deserialize(InputStream inputStream) throws IOException {

    if(inputStream == null) {
      throw new IllegalArgumentException("Inputstream cannot be null");
    }

    com.fasterxml.jackson.core.JsonParser jsonParser = new JsonFactory().createParser(inputStream);
    StageCollection stageCollection = new StageCollection();
    List<Map<String, String>> configOptions = null;
    String name = null, version = null, label = null, description = null, type = null;
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
          configOptions = readConfigOptionArray(jsonParser);
        }
      }
      StageConfiguration stageConfiguration = new StageConfiguration();
      stageConfiguration.getStageOptions().put("name", name);
      stageConfiguration.getStageOptions().put("version", version);
      stageConfiguration.getStageOptions().put("label", label);
      stageConfiguration.getStageOptions().put("description", description);
      stageConfiguration.getStageOptions().put("type", type);
      if(configOptions != null) {
        stageConfiguration.getConfigOptions().addAll(configOptions);
      }
      stageCollection.getStageConfigurations().add(stageConfiguration);
    }

    return stageCollection;

  }

  private static List<Map<String, String>> readConfigOptionArray(JsonParser jsonParser) throws IOException {
    List<Map<String, String>> configOptions = new ArrayList<Map<String, String>>();
    String fieldName;
    String name = null;
    String description = null;
    String defaultValue = null;
    String label = null;
    String type = null;
    String required = null;
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
          label = jsonParser.getText();
        } else if ("defaultValue".equals(fieldName)) {
          jsonParser.nextToken();
          defaultValue = jsonParser.getText();
        } else if ("type".equals(fieldName)) {
          jsonParser.nextToken();
          type = jsonParser.getText();
        } else if ("required".equals(fieldName)) {
          jsonParser.nextToken();
          required = jsonParser.getText();
        } else if ("group".equals(fieldName)) {
          jsonParser.nextToken();
          group = jsonParser.getText();
        }
      }
      Map<String, String> configMap = new HashMap<String, String>();
      configMap.put("name", name);
      configMap.put("description", description);
      configMap.put("label", label);
      configMap.put("defaultValue", defaultValue);
      configMap.put("type", type);
      configMap.put("required", required);
      configMap.put("group", group);
      configOptions.add(configMap);
    }
    return configOptions;
  }
}
