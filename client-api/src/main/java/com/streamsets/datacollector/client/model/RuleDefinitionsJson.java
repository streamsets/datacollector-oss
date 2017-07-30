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
package com.streamsets.datacollector.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.client.StringUtil;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.ArrayList;
import java.util.List;


@ApiModel(description = "")
public class RuleDefinitionsJson   {

  private List<MetricsRuleDefinitionJson> metricsRuleDefinitions = new ArrayList<MetricsRuleDefinitionJson>();
  private List<DataRuleDefinitionJson> dataRuleDefinitions = new ArrayList<DataRuleDefinitionJson>();
  private List<String> emailIds = new ArrayList<String>();
  private String uuid = null;
  private List<RuleIssueJson> ruleIssues = new ArrayList<RuleIssueJson>();
  List<ConfigConfigurationJson> configuration = new ArrayList<>();
  private List<IssueJson> configIssues = new ArrayList<IssueJson>();

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("metricsRuleDefinitions")
  public List<MetricsRuleDefinitionJson> getMetricsRuleDefinitions() {
    return metricsRuleDefinitions;
  }
  public void setMetricsRuleDefinitions(List<MetricsRuleDefinitionJson> metricsRuleDefinitions) {
    this.metricsRuleDefinitions = metricsRuleDefinitions;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("dataRuleDefinitions")
  public List<DataRuleDefinitionJson> getDataRuleDefinitions() {
    return dataRuleDefinitions;
  }
  public void setDataRuleDefinitions(List<DataRuleDefinitionJson> dataRuleDefinitions) {
    this.dataRuleDefinitions = dataRuleDefinitions;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("emailIds")
  public List<String> getEmailIds() {
    return emailIds;
  }
  public void setEmailIds(List<String> emailIds) {
    this.emailIds = emailIds;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("uuid")
  public String getUuid() {
    return uuid;
  }
  public void setUuid(String uuid) {
    this.uuid = uuid;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("ruleIssues")
  public List<RuleIssueJson> getRuleIssues() {
    return ruleIssues;
  }
  public void setRuleIssues(List<RuleIssueJson> ruleIssues) {
    this.ruleIssues = ruleIssues;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("configuration")
  public List<ConfigConfigurationJson> getConfiguration() {
    return configuration;
  }
  public void setConfiguration(List<ConfigConfigurationJson> configuration) {
    this.configuration = configuration;
  }

  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("configIssues")
  public List<IssueJson> getConfigIssues() {
    return configIssues;
  }
  public void setConfigIssues(List<IssueJson> configIssues) {
    this.configIssues = configIssues;
  }

  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class RuleDefinitionsJson {\n");

    sb.append("    metricsRuleDefinitions: ").append(StringUtil.toIndentedString(metricsRuleDefinitions)).append("\n");
    sb.append("    dataRuleDefinitions: ").append(StringUtil.toIndentedString(dataRuleDefinitions)).append("\n");
    sb.append("    emailIds: ").append(StringUtil.toIndentedString(emailIds)).append("\n");
    sb.append("    uuid: ").append(StringUtil.toIndentedString(uuid)).append("\n");
    sb.append("    ruleIssues: ").append(StringUtil.toIndentedString(ruleIssues)).append("\n");
    sb.append("    configuration: ").append(StringUtil.toIndentedString(configuration)).append("\n");
    sb.append("    configIssues: ").append(StringUtil.toIndentedString(configIssues)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
