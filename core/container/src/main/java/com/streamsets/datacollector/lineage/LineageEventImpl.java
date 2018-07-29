/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.datacollector.lineage;

import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LineageEventImpl implements LineageEvent {
  public static final String PARTIAL_URL = "/collector/pipeline/";
  private Map<LineageGeneralAttribute, String> generalAttributes;
  private Map<LineageSpecificAttribute, String> specificAttributes;

  private List<String> tags;
  private Map<String, String> properties;

  public LineageEventImpl(
      LineageEventType type,
      String name,
      String user,
      long startTime,
      String id,
      String dcId,
      String permalink,
      String stageName
  ) {
    generalAttributes = new HashMap<>();
    specificAttributes = new HashMap<>();
    tags = new ArrayList<>();
    properties = new HashMap<>();

    generalAttributes.put(LineageGeneralAttribute.EVENT_TYPE, type.toString());
    generalAttributes.put(LineageGeneralAttribute.PIPELINE_TITLE, name);
    generalAttributes.put(LineageGeneralAttribute.PIPELINE_USER, user);
    generalAttributes.put(LineageGeneralAttribute.PIPELINE_START_TIME, Long.toString(startTime));
    generalAttributes.put(LineageGeneralAttribute.PIPELINE_ID, id);
    generalAttributes.put(LineageGeneralAttribute.SDC_ID, dcId);
    generalAttributes.put(LineageGeneralAttribute.PERMALINK, permalink);
    generalAttributes.put(LineageGeneralAttribute.STAGE_NAME, stageName);
    generalAttributes.put(LineageGeneralAttribute.TIME_STAMP, Long.toString(System.currentTimeMillis()));
  }

  @Override
  public LineageEventType getEventType() {
    return LineageEventType.valueOf(generalAttributes.get(LineageGeneralAttribute.EVENT_TYPE));
  }

  @Override
  public String getPipelineId() {
    return generalAttributes.get(LineageGeneralAttribute.PIPELINE_ID);

  }

  @Override
  public String getPipelineUser() {
    return generalAttributes.get(LineageGeneralAttribute.PIPELINE_USER);

  }

  @Override
  public long getPipelineStartTime() {
    return Long.parseLong(generalAttributes.get(LineageGeneralAttribute.PIPELINE_START_TIME));

  }

  @Override
  public String getStageName() {
    return generalAttributes.get(LineageGeneralAttribute.STAGE_NAME);

  }

  @Override
  public long getTimeStamp() {
    return Long.parseLong(generalAttributes.get(LineageGeneralAttribute.TIME_STAMP));

  }

  @Override
  public String getPipelineTitle() {
    return generalAttributes.get(LineageGeneralAttribute.PIPELINE_TITLE);

  }

  @Override
  public String getPipelineDataCollectorId() {
    return generalAttributes.get(LineageGeneralAttribute.SDC_ID);

  }

  @Override
  public String getPermalink() {
    return generalAttributes.get(LineageGeneralAttribute.PERMALINK);

  }

  @Override
  public void setSpecificAttribute(LineageSpecificAttribute name, String value) {
    specificAttributes.put(name, value);
  }

  @Override
  public String getSpecificAttribute(LineageSpecificAttribute name) {
    return specificAttributes.get(name);
  }

  @Override
  public List<String> getTags() {
    return tags;
  }

  @Override
  public void setTags(List<String> tags) {
    this.tags = tags;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public List<LineageSpecificAttribute> missingSpecificAttributes() {
    LineageEventType type;
    try {
      type = getEventType();
    } catch (IllegalArgumentException | NullPointerException ex) {
      throw new IllegalArgumentException(Utils.format(ContainerError.CONTAINER_01404.getMessage(), getEventType()));
    }

    Set<LineageSpecificAttribute> badFields = new HashSet<>(type.getSpecificAttributes());

    // Remove attributes we have - from the group of attributes we expect...
    badFields.removeAll(specificAttributes.keySet());

    // at this point, badFields is a list of attributes we
    // expected to be there, but are missing.

    // now we add attributes which are there, but may have null or empty values.
    for(LineageSpecificAttribute att : type.getSpecificAttributes()) {
      if(StringUtils.isEmpty(specificAttributes.get(att))) {
        badFields.add(att);
      }
    }

    return new ArrayList<>(badFields);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());

    sb.append(" general: ");
    for (Map.Entry<LineageGeneralAttribute, String> entry : generalAttributes.entrySet()) {
      sb.append(entry.getKey());
      sb.append(": ");
      sb.append(entry.getValue());
      sb.append(" ");
    }

    sb.append(" specific: ");
    for (Map.Entry<LineageSpecificAttribute, String> entry : specificAttributes.entrySet()) {
      sb.append(entry.getKey());
      sb.append(": ");
      sb.append(entry.getValue());
      sb.append(" ");
    }

    sb.append(" properties: ");
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      sb.append(entry.getKey());
      sb.append(": ");
      sb.append(entry.getValue());
      sb.append(" ");
    }

    sb.append(" tags: ");
    for (String s : tags) {
      sb.append(s);
      sb.append(" ");
    }

    return sb.toString();
  }

}
