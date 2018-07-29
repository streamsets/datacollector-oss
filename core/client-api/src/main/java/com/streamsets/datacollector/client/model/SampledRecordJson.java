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


@ApiModel(description = "")
public class SampledRecordJson   {

  private RecordJson record = null;
  private Boolean matchedCondition = null;


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("record")
  public RecordJson getRecord() {
    return record;
  }
  public void setRecord(RecordJson record) {
    this.record = record;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("matchedCondition")
  public Boolean getMatchedCondition() {
    return matchedCondition;
  }
  public void setMatchedCondition(Boolean matchedCondition) {
    this.matchedCondition = matchedCondition;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class SampledRecordJson {\n");

    sb.append("    record: ").append(StringUtil.toIndentedString(record)).append("\n");
    sb.append("    matchedCondition: ").append(StringUtil.toIndentedString(matchedCondition)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
