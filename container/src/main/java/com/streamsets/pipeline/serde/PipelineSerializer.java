package com.streamsets.pipeline.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.streamsets.pipeline.container.*;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Created by harikiran on 10/19/14.
 */
@Produces(MediaType.APPLICATION_JSON)
public class PipelineSerializer extends JsonSerializer<Pipeline> {
  @Override
  public void serialize(Pipeline pipeline, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
    System.out.println("Using serializer : " + getClass().getName());
    jsonGenerator.setPrettyPrinter(new DefaultPrettyPrinter());

    ModuleInfoSerializer moduleInfoSerializer = new ModuleInfoSerializer();

    jsonGenerator.writeStartObject(); //start writing pipeline

    Pipe[] pipes = pipeline.getPipes();
    jsonGenerator.writeArrayFieldStart("pipes"); //start option groups array
    for(Pipe p : pipes) {
      jsonGenerator.writeStartObject();
      jsonGenerator.writeStringField("name", p.getModuleInfo().getInstanceName());
      writePipeTypeInformation(jsonGenerator, p);
      jsonGenerator.writeFieldName("moduleInfo");
      moduleInfoSerializer.serialize(p.getModuleInfo(), jsonGenerator, serializerProvider);

      jsonGenerator.writeArrayFieldStart("inputLanes"); //output lanes
      for(String inputLane : p.getInputLanes()) {
        jsonGenerator.writeString(inputLane);
      }
      jsonGenerator.writeEndArray(); //output lanes

      jsonGenerator.writeArrayFieldStart("outputLanes"); //output lanes
      for(String outputLane : p.getOutputLanes()) {
        jsonGenerator.writeString(outputLane);
      }
      jsonGenerator.writeEndArray(); //output lanes

      jsonGenerator.writeArrayFieldStart("producedLanes"); //produced lanes
      for(String producedLane : p.getProducedLanes()) {
        jsonGenerator.writeString(producedLane);
      }
      jsonGenerator.writeEndArray(); //produced lanes

      jsonGenerator.writeArrayFieldStart("consumedLanes"); //consumed lanes
      for(String consumedLane : p.getConsumedLanes()) {
        jsonGenerator.writeString(consumedLane);
      }
      jsonGenerator.writeEndArray(); //consumed lanes

      jsonGenerator.writeEndObject();
    }
    jsonGenerator.writeEndArray();
    jsonGenerator.writeEndObject(); //end writing pipeline
  }

  private void writePipeTypeInformation(JsonGenerator jsonGenerator, Pipe p) throws IOException {
    if(p instanceof SourcePipe) {
      jsonGenerator.writeStringField("type", "source");
      jsonGenerator.writeStringField("typeName", ((SourcePipe)p).getSource().getClass().getName());
    } else if(p instanceof ProcessorPipe) {
      jsonGenerator.writeStringField("type", "processor");
      jsonGenerator.writeStringField("typeName", ((ProcessorPipe)p).getProcessor().getClass().getName());
    } else if(p instanceof TargetPipe) {
      jsonGenerator.writeStringField("type", "target");
      jsonGenerator.writeStringField("typeName", ((TargetPipe)p).getTarget().getClass().getName());
    }
  }
}
