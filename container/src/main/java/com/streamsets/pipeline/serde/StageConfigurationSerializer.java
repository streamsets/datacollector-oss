package com.streamsets.pipeline.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.config.StageRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Created by harikiran on 10/20/14.
 */
@Produces(MediaType.APPLICATION_JSON)
public class StageConfigurationSerializer extends JsonSerializer<StageRegistry> {

  private static Logger LOG = LoggerFactory.getLogger(StageConfigurationSerializer.class);

  @Override
  public void serialize(StageRegistry moduleConfiguration, JsonGenerator jsonGenerator
    , SerializerProvider serializerProvider) throws IOException {
    LOG.debug("Using serializer " + getClass().getName());

    //set default Pretty printer
    DefaultPrettyPrinter p = new DefaultPrettyPrinter();
    p.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter());
    jsonGenerator.setPrettyPrinter(p);

    jsonGenerator.writeStartArray();

    for(StageDefinition moduleInfo : moduleConfiguration.getStageDefinitions()) {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("name", moduleInfo.getName());
      jsonGenerator.writeStringField("version", moduleInfo.getVersion());
      jsonGenerator.writeStringField("label", moduleInfo.getShortDescription());
      jsonGenerator.writeStringField("description", moduleInfo.getDescription());
      jsonGenerator.writeStringField("type", moduleInfo.getModuleType().name());
      jsonGenerator.writeArrayFieldStart("ConfigOptions"); //start option groups array

      for (ConfigDefinition option : moduleInfo.getConfigDefinitionList()) {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("name", option.getName());
        jsonGenerator.writeStringField("description", option.getDescription());
        jsonGenerator.writeStringField("label", option.getLabel());
        jsonGenerator.writeStringField("defaultValue", option.getDefaultValue());
        jsonGenerator.writeStringField("type", option.getType().name());
        jsonGenerator.writeStringField("required", option.isRequired() ? "true" : "false");
        jsonGenerator.writeStringField("group", option.getGroup());
        jsonGenerator.writeEndObject();

      }
      jsonGenerator.writeEndArray();
      jsonGenerator.writeEndObject();
    }
    jsonGenerator.writeEndArray();
  }
}