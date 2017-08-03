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
package com.streamsets.pipeline.stage.plugin.navigator;

import com.cloudera.nav.sdk.model.SourceType;
import com.cloudera.nav.sdk.model.annotations.MClass;
import com.cloudera.nav.sdk.model.annotations.MProperty;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import org.apache.commons.lang.StringUtils;

import java.nio.file.Path;
import java.nio.file.Paths;

@MClass(model = "sdc_dataset")
public class NavigatorDataset extends Entity {

  @MProperty(attribute = "firstClassParentId")
  private String parentId;

  private String pipelineId;
  private long pipelineStartTime;
  private String entityName;

  NavigatorDataset() {
  }

  NavigatorDataset(String namespace, LineageEvent event, String parentId) {
    fillIt(namespace, event, parentId);

  }

  void fillIt(String namespace, LineageEvent event, String parentId) {

    this.pipelineId = event.getPipelineId();
    this.pipelineStartTime = event.getPipelineStartTime();
    this.entityName = event.getSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME);


    setNamespace(namespace);
    setOwner(event.getPipelineUser());
    if (!StringUtils.isEmpty(event.getSpecificAttribute(LineageSpecificAttribute.DESCRIPTION))) {
      setDescription(event.getSpecificAttribute(LineageSpecificAttribute.DESCRIPTION));
    }
    setIdentity(generateId());

    // TODO: this is a Navigator bug.  this should be set, but not to a valid node.
    // apparently use of this field is being "redefined" between CDH 5.9 and 5.10 and up.
    // if set to a valid node, in 5.9, the parent replaces the child in the UI.
    // also this shows up in the UI, highlighted as a link, and does
    // nothing, since it points to the same location.
    //    this.parentId = parentId;
    this.parentId = getIdentity();

    NavigatorParameters parms = new NavigatorParameters(EndPointType.valueOf(event.getSpecificAttribute(
        LineageSpecificAttribute.ENDPOINT_TYPE)));

    setSourceType(parms.getSourceType());
    setSourceId(Integer.toString(SourceType.HDFS.ordinal()));
    setEntityType(parms.getEntityType());

    if (!StringUtils.isEmpty(event.getSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME))) {
      Path path = Paths.get(event.getSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME));
      if(path.getParent() != null) {
        setParentPath(path.getParent().toString());
      }
      setName(NavigatorHelper.nameChopper(event.getSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME)));

    } else {
      // otherwise, default to stage name.
      setName(NavigatorHelper.nameChopper(event.getStageName()));

    }

    setTags(event.getTags());
    setProperties(event.getProperties());

  }

  @Override
  public String generateId() {
    return NavigatorHelper.makeDatasetIdentity(pipelineId, entityName, pipelineStartTime);
  }

  public String getParentId() {
    return parentId;
  }

}
