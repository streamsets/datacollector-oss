package com.streamsets.pipeline.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.streamsets.config.api.ConfigOption;
import com.streamsets.config.api.ConfigOptionGroup;
import com.streamsets.config.api.ConfigType;
import com.streamsets.pipeline.api.Module;
import com.streamsets.pipeline.container.ModuleInfo;

import javax.ws.rs.Consumes;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by harikiran on 10/19/14.
 */
@Consumes(MediaType.APPLICATION_JSON)
public class ModuleInfoDeserializer extends JsonDeserializer<Module.Info> {

  @Override
  public Module.Info deserialize(JsonParser jsonParser
    , DeserializationContext deserializationContext) throws IOException {

    String name = null;
    String instanceName = null;
    String version = null;
    String description = null;
    List<ConfigOptionGroup> configOptionGroupList = null;

    while(jsonParser.nextToken() != JsonToken.END_OBJECT) {
      String fieldName = jsonParser.getCurrentName();
      if("name".equals(fieldName)) {
        name = jsonParser.getText();
      } else if("instanceName".equals(fieldName)) {
        instanceName = jsonParser.getText();
      } else if("version".equals(fieldName)) {
        version = jsonParser.getText();
      } else if("description".equals(fieldName)) {
        description = jsonParser.getText();
      } else if("ConfigOptionGroups".equals(fieldName)) {
        configOptionGroupList = readConfigOptionsArray(jsonParser);
      }
    }

    ModuleInfo moduleInfo = new ModuleInfo(name, version, description, instanceName);
    moduleInfo.getConfiguration().addAll(configOptionGroupList);
    return moduleInfo;
  }

  private List<ConfigOptionGroup> readConfigOptionsArray(JsonParser jsonParser) throws IOException {
    String fieldName;
    List<ConfigOptionGroup> configOptionGroups = new ArrayList<ConfigOptionGroup>();
    List<ConfigOption> configOptions = null;
    String name = null;
    //read the config option group array
    // starts with '['
    jsonParser.nextToken();
    // iterate through the array until token equal to "]"
    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      fieldName = jsonParser.getCurrentName();
      if("name".equals(fieldName)) {
        name = jsonParser.getText();
      } else if ("configOptions".equals(fieldName)) {
        //read config option array
        configOptions = readConfigOptionArray(jsonParser);
      }
      ConfigOptionGroup opg = new ConfigOptionGroup(name, configOptions);
      configOptionGroups.add(opg);
    }

    return configOptionGroups;
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
    jsonParser.nextToken();
    // iterate through the array until token equal to "]"
    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      fieldName = jsonParser.getCurrentName();
      if("name".equals(fieldName)) {
        name = jsonParser.getText();
      } else if("description".equals(fieldName)) {
        description = jsonParser.getText();
      } else if("shortDescription".equals(fieldName)) {
        shortDescription = jsonParser.getText();
      } else if("defaultValue".equals(fieldName)) {
        defaultValue = jsonParser.getText();
      } else if ("type".equals(fieldName)) {
        type = jsonParser.getText();
      } else if ("mandatory".equals(fieldName)) {
        mandatory = jsonParser.getText();
      }
      configOptions.add(new ConfigOption(name,
        ConfigType.valueOf(type),
        shortDescription,
        description,
        defaultValue,
        "true".equals(mandatory)? true: false));
    }

    return configOptions;
  }
}
