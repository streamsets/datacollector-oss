package com.streamsets.pipeline.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.streamsets.config.api.ConfigOption;
import com.streamsets.config.api.ConfigOptionGroup;
import com.streamsets.pipeline.api.Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Created by harikiran on 10/18/14.
 */
@Produces(MediaType.APPLICATION_JSON)
public class ModuleInfoSerializer extends JsonSerializer<Module.Info>{

  private static Logger LOG = LoggerFactory.getLogger(ModuleInfoSerializer.class);

  @Override
  public void serialize(Module.Info moduleInfo, JsonGenerator jsonGenerator
    , SerializerProvider serializerProvider) throws IOException {

    LOG.debug("Using serializer " + getClass().getName());

    //set default Pretty printer
    jsonGenerator.setPrettyPrinter(new DefaultPrettyPrinter());

    jsonGenerator.writeStartObject();
    jsonGenerator.writeStringField("name", moduleInfo.getName());
    jsonGenerator.writeStringField("instanceName", moduleInfo.getInstanceName());
    jsonGenerator.writeStringField("version", moduleInfo.getVersion());
    jsonGenerator.writeStringField("description", moduleInfo.getDescription());

    jsonGenerator.writeArrayFieldStart("ConfigOptionGroups"); //start option groups array
    for(ConfigOptionGroup optionGroup : moduleInfo.getConfiguration()) {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("name", optionGroup.getName());

      jsonGenerator.writeArrayFieldStart("ConfigOptions"); //start options array
      for(ConfigOption option : optionGroup.getConfigOptions()) {

        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("name",option.getName());
        jsonGenerator.writeStringField("description", option.getDescription());
        jsonGenerator.writeStringField("shortDescription", option.getShortDescription());
        jsonGenerator.writeStringField("defaultValue", option.getDefaultValue());
        jsonGenerator.writeStringField("type", option.getType().name());
        jsonGenerator.writeStringField("mandatory", option.isMandatory()? "true" : "false");

        jsonGenerator.writeEndObject();
      }
      jsonGenerator.writeEndArray();
      jsonGenerator.writeEndObject();
    }
    jsonGenerator.writeEndArray();
    jsonGenerator.writeEndObject();

  }
}
