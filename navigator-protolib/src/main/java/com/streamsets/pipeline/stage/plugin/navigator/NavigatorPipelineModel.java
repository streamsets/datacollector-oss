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
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MClass(model = "sdc_pipeline_model")
public class NavigatorPipelineModel extends Entity {
  private static final Logger LOG = LoggerFactory.getLogger(NavigatorPipelineModel.class);

  // this class is the parent of other classes which have a SOURCE or TARGET (or both).
  // the Navigator SDK pulls an NPE if it is passed
  // a RelationRole.SOURCE or RelationRole.TARGET which is either not initialized,
  // or is initialized, but is empty.

  @MProperty(attribute = "firstClassParentId")
  private String parentId;

  @MProperty
  private Instant started;

  @MProperty
  private String link;

  @MProperty
  private Instant ended;

  @MProperty
  private String pipelineId;

  private long pipelineStartTime;

  NavigatorPipelineModel(){

  }

  public NavigatorPipelineModel(String namespace, LineageEvent event, String parentId) {
    fillIn(namespace, event, parentId);
    LOG.info("bobp: NavigatorPipelineModel() got here eventType {}.", event.getEventType());
  }

  void fillIn(String namespace, LineageEvent event, String parentId) {

    Preconditions.checkArgument(StringUtils.isNotEmpty(namespace));
    Preconditions.checkArgument(StringUtils.isNotEmpty(event.getPipelineId()));
    Preconditions.checkArgument(StringUtils.isNotEmpty(event.getPipelineTitle()));

    pipelineId = event.getPipelineId();
    pipelineStartTime = event.getPipelineStartTime();

    setNamespace(NavigatorHelper.nameChopper(namespace));
    setName(NavigatorHelper.nameChopper(event.getPipelineTitle()));
    setIdentity(
        NavigatorHelper.makePipelineIdentity(
            event.getPipelineId(),
            event.getPipelineStartTime())
    );
    // TODO: this is a Navigator bug.  this should be set - but when setting
    // it to the parent (in CDH 5.9), the parent replaces the child in the UI.
    //    this.parentId = parentId;
    this.parentId = getIdentity();

    setOwner(event.getPipelineUser());
    setStarted(new Instant(pipelineStartTime));
    setCreated(new Instant(event.getPipelineStartTime()));

    // this seems to have a bug - if they are not the same across all entities
    // it does not correctly render the Lineage graph.
    setSourceId(Integer.toString(SourceType.HDFS.ordinal()));

    setSourceType(SourceType.SDK);
    setEntityType(EntityType.OPERATION);
    setDescription(event.getSpecificAttribute(LineageSpecificAttribute.DESCRIPTION));
    setLink(event.getPermalink());
    setTags(event.getTags());
    setProperties(event.getProperties());

  }

  @Override
  public String generateId() {
    return NavigatorHelper.makePipelineIdentity(pipelineId, pipelineStartTime);
  }

  public String getLink() {
    return this.link;
  }

  public void setLink(String link) {
    this.link = link;
  }

  public Instant getStarted() {
    return started;
  }

  public Instant getEnded() {
    return ended;
  }

  public void setStarted(Instant started) {
    this.started = started;
  }

  public void setEnded(Instant ended) {
    this.ended = ended;
  }

  public String getPipelineId() {
    return pipelineId;
  }

  public String getParentId() {
    return parentId;
  }


}
