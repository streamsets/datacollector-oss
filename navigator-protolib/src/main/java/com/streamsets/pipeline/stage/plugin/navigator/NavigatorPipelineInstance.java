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
import com.cloudera.nav.sdk.model.annotations.MRelation;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.cloudera.nav.sdk.model.entities.EntityType;
import com.cloudera.nav.sdk.model.relations.RelationRole;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import org.joda.time.Instant;

@MClass(model = "sdc_pipeline_instance")
public class NavigatorPipelineInstance extends NavigatorPipelineModel {

  @MProperty(attribute = "firstClassParentId")
  private String parentId;

  @MRelation(role = RelationRole.TEMPLATE)
  private Entity template;

  @MProperty
  private Instant started;

  @MProperty
  private String link;

  @MProperty
  private Instant ended;

  @MProperty
  private String steward;

  @MProperty
  private String script;

  private String pipelineId;
  private long pipelineStartTime;

  public NavigatorPipelineInstance(
      String namespace, LineageEvent event, Entity model, String parentId
  ) {
    fillIn(namespace, event, parentId);
    setTemplate(model);

    setName(NavigatorHelper.nameChopper(event.getPipelineTitle() + " Instance"));
    setSourceType(SourceType.SDK);
    setEntityType(EntityType.OPERATION_EXECUTION);
    setLink(event.getPermalink());

    setEnded(new Instant(event.getTimeStamp()));

    // need to have unique identity - specifically NOT the
    // identity which would match the model: NavigatorPipelineModel
    setIdentity(generateId());
    // TODO: this is a Navigator bug.  this should be set, but not to a valid node.
    // if set to a valid node, in 5.9, the parent replaces the child in the UI.
    //    this.parentId = parentId;
    this.parentId = getIdentity();

  }

  @Override
  public String generateId() {
    return NavigatorHelper.makePipelineInstanceIdentity(pipelineId, pipelineStartTime);
  }

  @Override
  public SourceType getSourceType() {
    return super.getSourceType();
  }

  @Override
  public EntityType getEntityType() {
    return super.getEntityType();
  }

  public void setTemplate(Entity template) {
    this.template = template;
  }

  public Entity getTemplate() {
    return template;
  }

  public String getLink() {
    return this.link;
  }

  public Instant getStarted() {
    return started;
  }

  public Instant getEnded() {
    return ended;
  }

  public void setScript(String script) {
    this.script = script;
  }

  public String getScript() {
    return script;
  }

  public void setStarted(Instant started) {
    this.started = started;
  }

  public void setEnded(Instant ended) {
    this.ended = ended;
  }

  public void setLink(String link) {
    this.link = link;
  }

  public String getSteward() {
    return this.steward;
  }

  public void setSteward(String steward) {
    this.steward = steward;
  }

  public String getParentId() {
    return parentId;
  }


}
