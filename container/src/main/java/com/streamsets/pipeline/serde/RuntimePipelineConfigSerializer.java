package com.streamsets.pipeline.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.streamsets.pipeline.config.ConfigOption;
import com.streamsets.pipeline.config.RuntimeModuleConfiguration;
import com.streamsets.pipeline.config.RuntimePipelineConfiguration;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Created by harikiran on 10/20/14.
 */
@Produces(MediaType.APPLICATION_JSON)
public class RuntimePipelineConfigSerializer extends JsonSerializer<RuntimePipelineConfiguration> {

  @Override
  public void serialize(RuntimePipelineConfiguration runtimePipelineConfiguration
    , JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {

    System.out.println("Using serializer " + getClass().getName());

    //set default Pretty printer
    DefaultPrettyPrinter p = new DefaultPrettyPrinter();
    p.indentArraysWith(new DefaultPrettyPrinter.Lf2SpacesIndenter());
    jsonGenerator.setPrettyPrinter(p);

    jsonGenerator.writeStartObject();
    jsonGenerator.writeArrayFieldStart("runtimeModuleConfigurations");

    for(RuntimeModuleConfiguration moduleInfo : runtimePipelineConfiguration.getRuntimeModuleConfigurations()) {
      jsonGenerator.writeStartObject();

      //general information
      jsonGenerator.writeObjectFieldStart("generalInformation");
      jsonGenerator.writeStringField("instanceName", moduleInfo.getInstanceName());
      jsonGenerator.writeStringField("moduleName", moduleInfo.getModuleName());
      jsonGenerator.writeStringField("moduleVersion", moduleInfo.getModuleVersion());
      jsonGenerator.writeStringField("moduleDescription", moduleInfo.getModuleDescription());
      jsonGenerator.writeEndObject();

      jsonGenerator.writeObjectFieldStart("configInformation");
      jsonGenerator.writeArrayFieldStart("options"); //start option groups array
      for (ConfigOption option : moduleInfo.getConfigOptions()) {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("name", option.getName());
        jsonGenerator.writeStringField("description", option.getDescription());
        jsonGenerator.writeStringField("shortDescription", option.getShortDescription());
        jsonGenerator.writeStringField("defaultValue", option.getDefaultValue());
        jsonGenerator.writeStringField("type", option.getType().name());
        jsonGenerator.writeStringField("mandatory", option.isMandatory() ? "true" : "false");
        jsonGenerator.writeStringField("group", option.getGroup());
        jsonGenerator.writeEndObject();
      }
      jsonGenerator.writeEndArray();

      //input lanes and output lanes
      jsonGenerator.writeArrayFieldStart("inputLanes"); //input lanes
      for(String inputLane : moduleInfo.getInputLanes()) {
        jsonGenerator.writeString(inputLane);
      }
      jsonGenerator.writeEndArray(); //input lanes

      jsonGenerator.writeArrayFieldStart("outputLanes"); //output lanes
      for(String outputLane : moduleInfo.getOutputLanes()) {
        jsonGenerator.writeString(outputLane);
      }
      jsonGenerator.writeEndArray(); //output lanes
      jsonGenerator.writeEndObject(); //end configuraton

      //UI related
      jsonGenerator.writeObjectFieldStart("uiInformation");
      jsonGenerator.writeStringField("xPos", String.valueOf(moduleInfo.getxPos()));
      jsonGenerator.writeStringField("yPos", String.valueOf(moduleInfo.getyPos()));
      jsonGenerator.writeEndObject();

      jsonGenerator.writeEndObject();
    }
    jsonGenerator.writeEndArray();
    jsonGenerator.writeEndObject();
  }
}

