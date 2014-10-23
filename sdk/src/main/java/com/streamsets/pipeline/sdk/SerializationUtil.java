package com.streamsets.pipeline.sdk;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

import java.io.IOException;
import java.io.OutputStream;
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

    for(StageConfiguration moduleInfo : stageCollection.getStageConfigurations()) {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("name", moduleInfo.getStageOptions().get("name"));
      jsonGenerator.writeStringField("version", moduleInfo.getStageOptions().get("version"));
      jsonGenerator.writeStringField("label", moduleInfo.getStageOptions().get("label"));
      jsonGenerator.writeStringField("description", moduleInfo.getStageOptions().get("description"));
      jsonGenerator.writeStringField("type", moduleInfo.getStageOptions().get("type"));

      jsonGenerator.writeArrayFieldStart("ConfigOptions"); //start option groups array

      for (Map<String, String> option : moduleInfo.getConfigOptions()) {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("name", option.get("name"));
        jsonGenerator.writeStringField("description", option.get("description"));
        jsonGenerator.writeStringField("shortDescription", option.get("label"));
        jsonGenerator.writeStringField("defaultValue", option.get("defaultValue"));
        jsonGenerator.writeStringField("type", option.get("type"));
        jsonGenerator.writeStringField("required", option.get("required"));
        //jsonGenerator.writeStringField("group", option.get("group"));
        jsonGenerator.writeEndObject();

      }
      jsonGenerator.writeEndArray();
      jsonGenerator.writeEndObject();
    }
    jsonGenerator.writeEndArray();

    jsonGenerator.flush();
    jsonGenerator.close();
  }
}
