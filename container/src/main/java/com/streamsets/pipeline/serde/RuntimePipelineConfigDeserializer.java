package com.streamsets.pipeline.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.streamsets.pipeline.config.ConfigOption;
import com.streamsets.pipeline.config.ConfigType;
import com.streamsets.pipeline.config.RuntimeModuleConfiguration;
import com.streamsets.pipeline.config.RuntimePipelineConfiguration;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by harikiran on 10/20/14.
 */
@Consumes(MediaType.APPLICATION_JSON)
public class RuntimePipelineConfigDeserializer extends JsonDeserializer<RuntimePipelineConfiguration> {

  @Override
  public RuntimePipelineConfiguration deserialize(
    JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
    RuntimePipelineConfiguration runtimePipelineConfiguration = new RuntimePipelineConfiguration();

    System.out.println("Using deserializer : " + getClass().getName());
    String fieldName;
    String uuid = null;
    String instanceName = null;
    String moduleName = null;
    String moduleVersion = null;
    String moduleDescription = null;
    List<ConfigOption> configOptionList = null;
    int xPos = 0;
    int yPos = 0;
    List<String> inputLanes = null;
    List<String> outputLanes = null;
    Map<String, List<String>> errorsMap = null;

    while(jsonParser.nextToken() != JsonToken.END_OBJECT) {
      fieldName = jsonParser.getCurrentName();
      if("uuid".equals(fieldName)) {
        jsonParser.nextToken();
        uuid = jsonParser.getValueAsString();
      } else if ("runtimeModuleConfigurations".equals(fieldName)) {
        while(jsonParser.nextToken() != JsonToken.END_ARRAY) {

          //read each module config
          while (jsonParser.nextToken() != JsonToken.END_OBJECT) {

            fieldName = jsonParser.getCurrentName();
            if ("generalInformation".equals(fieldName)) {
              //read general information
              while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                fieldName = jsonParser.getCurrentName();
                if ("instanceName".equals(fieldName)) {
                  jsonParser.nextToken();
                  instanceName = jsonParser.getValueAsString();
                } else if ("moduleName".equals(fieldName)) {
                  jsonParser.nextToken();
                  moduleName = jsonParser.getText();
                } else if ("moduleVersion".equals(fieldName)) {
                  jsonParser.nextToken();
                  moduleVersion = jsonParser.getText();
                } else if ("moduleDescription".equals(fieldName)) {
                  jsonParser.nextToken();
                  moduleDescription = jsonParser.getText();
                }
              }
            } else if ("configInformation".equals(fieldName)) {
              while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                //read config option array
                fieldName = jsonParser.getCurrentName();
                if ("options".equals(fieldName)) {
                  configOptionList = readConfigOptionArray(jsonParser);
                } else if ("inputLanes".equals(fieldName)) {
                  jsonParser.nextToken();
                  //read config option array
                  inputLanes = readStringArray(jsonParser);
                } else if ("outputLanes".equals(fieldName)) {
                  jsonParser.nextToken();
                  //read config option array
                  outputLanes = readStringArray(jsonParser);
                }
              }
            } else if ("uiInformation".equals(fieldName)) {
              while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                fieldName = jsonParser.getCurrentName();
                if ("xPos".equals(fieldName)) {
                  xPos = jsonParser.getValueAsInt();
                } else if ("yPos".equals(fieldName)) {
                  yPos = jsonParser.getValueAsInt();
                }
              }
            }
          }
          runtimePipelineConfiguration.getRuntimeModuleConfigurations().add(
            new RuntimeModuleConfiguration(
              instanceName, moduleName, moduleVersion, moduleDescription, configOptionList, xPos, yPos, inputLanes,
              outputLanes));
        }
      } else if ("errors".equals(fieldName)) {
        errorsMap = readErrorsMap(jsonParser);
      }
    }

    runtimePipelineConfiguration.setUuid(uuid);
    runtimePipelineConfiguration.setErrorsMap(errorsMap);
    return runtimePipelineConfiguration;
  }

  private Map<String, List<String>> readErrorsMap(JsonParser jsonParser) throws IOException {
    Map<String, List<String>> errorsMap = new LinkedHashMap<String, List<String>>();
    String fieldName = null;
    String key = null;
    List<String> value = null;
    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
        fieldName = jsonParser.getCurrentName();
        if("module".equals(fieldName)) {
          jsonParser.nextToken();
          key = jsonParser.getValueAsString();
        } else if ("errorMessages".equals(fieldName)) {
          jsonParser.nextToken();
          value = readStringArray(jsonParser);
        }
      }
      if(key != null && value !=null) {
        errorsMap.put(key, value);
      }
    }
    return errorsMap;
  }

  private List<String> readStringArray(JsonParser jsonParser) throws IOException {
    List<String> stringSet = new ArrayList<String>();
    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      stringSet.add(jsonParser.getText());
    }
    return stringSet;
  }

  private List<ConfigOption> readConfigOptionArray(JsonParser jsonParser) throws IOException {
    List<ConfigOption> configOptions = new ArrayList<ConfigOption>();
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
        } else if ("shortDescription".equals(fieldName)) {
          jsonParser.nextToken();
          shortDescription = jsonParser.getText();
        } else if ("defaultValue".equals(fieldName)) {
          jsonParser.nextToken();
          defaultValue = jsonParser.getText();
        } else if ("type".equals(fieldName)) {
          jsonParser.nextToken();
          type = jsonParser.getText();
        } else if ("mandatory".equals(fieldName)) {
          jsonParser.nextToken();
          mandatory = jsonParser.getText();
        } else if ("group".equals(fieldName)) {
          jsonParser.nextToken();
          group = jsonParser.getText();
        }
      }
      configOptions.add(new ConfigOption(name,
        ConfigType.valueOf(type),
        shortDescription,
        description,
        defaultValue,
        "true".equals(mandatory)? true: false,
        group));
    }
    return configOptions;
  }
}
