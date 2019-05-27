/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.antennadoctor.engine;

import com.streamsets.datacollector.antennadoctor.bean.AntennaDoctorRuleBean;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Stripped down variant of AntennaDoctorRuleBean that doesn't contain information that is no longer relevant
 * to the runtime engine (to save memory consumption).
 */
public class RuntimeRule {
  private final String uuid;
  private final AntennaDoctorRuleBean.Entity entity;
  private final List<String> conditions;
  private final String summary;
  private final String description;
  private final Map<String, Object> startingContext;

  public RuntimeRule(AntennaDoctorRuleBean ruleBean, Map<String, Object> startingContext) {
    this.uuid = ruleBean.getUuid();
    this.entity = ruleBean.getEntity();
    this.conditions = Collections.unmodifiableList(ruleBean.getConditions());
    this.summary = ruleBean.getMessage().getSummary();
    this.description = String.join("\n", ruleBean.getMessage().getDescription());
    this.startingContext = Collections.unmodifiableMap(startingContext);
  }

  public String getUuid() {
    return uuid;
  }

  public AntennaDoctorRuleBean.Entity getEntity() {
    return entity;
  }

  public List<String> getConditions() {
    return conditions;
  }

  public String getSummary() {
    return summary;
  }

  public String getDescription() {
    return description;
  }

  public Map<String, Object> getStartingContext() {
    return startingContext;
  }
}
