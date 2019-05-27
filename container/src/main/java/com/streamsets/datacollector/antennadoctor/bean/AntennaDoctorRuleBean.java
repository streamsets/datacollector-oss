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
package com.streamsets.datacollector.antennadoctor.bean;

import java.util.Collections;
import java.util.List;

/**
 * Bean describing individual rule for Antenna Doctor.
 */
public class AntennaDoctorRuleBean {

  /**
   * Entity to which the rule is associated with (e.g. stage, pipeline, sdc, ...).
   */
  public enum Entity {
    REST, // Error that is propagated all the way up to REST interface
    STAGE, // Stage assumes that it's always running in SDC and thus with it's context
    VALIDATION, // Entity validation (stage, lineage publisher, ...)
    ;

    public boolean isOneOf(Entity ...entities) {
      if(entities == null) {
        return false;
      }

      for(Entity t : entities) {
        if(this == t) return true;
      }
      return false;
    }
  }

  /**
   * UUID uniquely identifying this rule.
   *
   * We are using the uuid for metric collection as well as CRUD for incremental update.
   */
  private String uuid;

  /**
   * List of conditions that must be true on SDC-level in order for this rule to be applicable.
   *
   * The conditions here are evaluated once during SDC start up. If the rule passes them, the rule is kept in memory
   * and used to troubleshoot issues. Alternatively the rule is discarded and not used during runtime. It will
   * however still be kept in the storage (for case that the SDC is upgraded, ...).
   */
  private List<String> preconditions;

  /**
   * Entity that this rule is associated with.
   *
   * The runtime will ensure that pipeline rule is not applied at stage error and similar checks.
   */
  private Entity entity;

  /**
   * List of conditions.
   *
   * The conditions here are evaluated each time this rule is matched (e.g. for the right entity and with the right
   * preconditions satisfied).
   */
  private List<String> conditions;

  /**
   * List of conditions primarily mutating the context variable.
   *
   * Similarly like precoditions, the expressions here will be executed only once. However the resulting 'context' variable
   * will be save and will be then available to each and every condition execution.
   */
  private List<String> startingContext;

  /**
   * Message that should be displayed if the rule matches.
   */
  private AntennaDoctorMessageBean message;

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public List<String> getPreconditions() {
    return preconditions;
  }

  public void setPreconditions(List<String> preconditions) {
    this.preconditions = preconditions;
  }

  public Entity getEntity() {
    return entity;
  }

  public void setEntity(Entity entity) {
    this.entity = entity;
  }

  public List<String> getConditions() {
    return conditions;
  }

  public void setConditions(List<String> conditions) {
    this.conditions = conditions;
  }

  public AntennaDoctorMessageBean getMessage() {
    return message;
  }

  public void setMessage(AntennaDoctorMessageBean message) {
    this.message = message;
  }

  public List<String> getStartingContext() {
    return startingContext == null ? Collections.emptyList() : startingContext;
  }

  public void setStartingContext(List<String> startingContext) {
    this.startingContext = startingContext;
  }
}
