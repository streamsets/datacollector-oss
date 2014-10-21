package com.streamsets.pipeline.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.streamsets.pipeline.api.Module;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.container.Pipe;
import com.streamsets.pipeline.container.Pipeline;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by harikiran on 10/19/14.
 */
@Consumes(MediaType.APPLICATION_JSON)
public class PipelineDeserializer extends JsonDeserializer<Pipeline> {

  @Override
  public Pipeline deserialize(JsonParser jsonParser,
                              DeserializationContext deserializationContext)
    throws IOException {
    String name = null;

    Pipeline.Builder builder = new Pipeline.Builder();
    while(jsonParser.nextToken() != JsonToken.END_OBJECT) {

      String fieldName = jsonParser.getCurrentName();
      if ("pipes".equals(fieldName)) {
        builder = readPipes(jsonParser, deserializationContext);
      } else {
        //Should start with pipes
      }
    }

    //build pipeline form pipes
    return builder.build();
  }

  private Pipeline.Builder readPipes(JsonParser jsonParser,
                               DeserializationContext deserializationContext) throws IOException {
    ModuleInfoDeserializer moduleInfoDeserializer = new ModuleInfoDeserializer();

    Pipeline.Builder pipelineBuilder = new Pipeline.Builder();
    List<Module.Info> pipelineInfo = new ArrayList<Module.Info>();
    String fieldName = null;
    String name = null;
    String type = null;
    String typeName = null;
    Module.Info moduleInfo = null;
    Set<String> inputLanes = null;
    Set<String> outputLanes = null;
    Set<String> producedLanes = null;
    Set<String> consumedLanes = null;

    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      while(jsonParser.nextToken() != JsonToken.END_OBJECT) {
        fieldName = jsonParser.getCurrentName();
        if ("name".equals(fieldName)) {
          name = jsonParser.getText();
        } else if ("type".equals(fieldName)) {
          type = jsonParser.getText();
        } else if ("typeName".equals(fieldName)) {
          typeName = jsonParser.getText();
        } else if ("moduleInfo".equals(fieldName)) {
          //read config option array
          moduleInfo = moduleInfoDeserializer.deserialize(jsonParser, deserializationContext);
          pipelineInfo.add(moduleInfo);
        } else if ("inputLanes".equals(fieldName)) {
          inputLanes = readStringArray(jsonParser);
        } else if ("outputLanes".equals(fieldName)) {
          outputLanes = readStringArray(jsonParser);
        } else if ("producedLanes".equals(fieldName)) {
          producedLanes = readStringArray(jsonParser);
        } else if ("consumedLanes".equals(fieldName)) {
          consumedLanes = readStringArray(jsonParser);
        }
      }
      addPipeToBuilder(pipelineBuilder, name, pipelineInfo, type, typeName
        , moduleInfo, inputLanes, outputLanes,
        producedLanes, consumedLanes);
    }
    return pipelineBuilder;
  }

  private Pipe addPipeToBuilder(Pipeline.Builder builder, String name, List<Module.Info> pipelineInfo,
                                String type, String typeName, Module.Info moduleInfo,
                                Set<String> inputLanes, Set<String> outputLanes,
                                Set<String> producedLanes, Set<String> consumedLanes) {
    try {
      if(type.equals("source")) {
        Object source = Class.forName(typeName).newInstance();
        assert source instanceof Source;
        builder.add(moduleInfo, (Source)source, outputLanes);
      } else if(type.equals("processor")) {
        Object processor = Class.forName(typeName).newInstance();
        assert processor instanceof Processor;
        builder.add(moduleInfo, (Processor)processor, inputLanes, outputLanes);
      } else if(type.equals("target")) {
        Object target = Class.forName(typeName).newInstance();
        assert target instanceof Target;
        builder.add(moduleInfo, (Target)target, inputLanes);
      }

    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }

  private Set<String> readStringArray(JsonParser jsonParser) throws IOException {
    Set<String> stringSet = new HashSet<String>();
    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      stringSet.add(jsonParser.getValueAsString());
    }
    return stringSet;
  }

}
