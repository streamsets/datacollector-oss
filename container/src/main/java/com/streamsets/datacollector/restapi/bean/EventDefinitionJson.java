/*
 * Copyright 2019 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.restapi.bean;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EventDefinitionJson {
  private String type;
  private String description;
  private int version;
  private List<EventFieldDefinitionJson> fields;

  public EventDefinitionJson(
      String type,
      String description,
      int version,
      List<EventFieldDefinitionJson> fields
  ) {
    this.type = type;
    this.description = description;
    this.fields = fields;
  }

  public String getType() {
    return type;
  }

  public String getDescription() {
    return description;
  }

  public int getVersion() {
    return version;
  }

  public List<EventFieldDefinitionJson> getFields() {
    return fields;
  }
}
