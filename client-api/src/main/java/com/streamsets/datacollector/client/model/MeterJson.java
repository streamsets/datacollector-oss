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

import com.streamsets.datacollector.client.StringUtil;



import io.swagger.annotations.*;
import com.fasterxml.jackson.annotation.JsonProperty;


@ApiModel(description = "")
public class MeterJson   {

  private Long count = null;
  private Long m1Rate = null;
  private Long m5Rate = null;
  private Long m15Rate = null;
  private Long m30Rate = null;
  private Long h1Rate = null;
  private Long h6Rate = null;
  private Long h12Rate = null;
  private Long h24Rate = null;
  private Long meanRate = null;
  private String units = null;


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("count")
  public Long getCount() {
    return count;
  }
  public void setCount(Long count) {
    this.count = count;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("m1_rate")
  public Long getM1Rate() {
    return m1Rate;
  }
  public void setM1Rate(Long m1Rate) {
    this.m1Rate = m1Rate;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("m5_rate")
  public Long getM5Rate() {
    return m5Rate;
  }
  public void setM5Rate(Long m5Rate) {
    this.m5Rate = m5Rate;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("m15_rate")
  public Long getM15Rate() {
    return m15Rate;
  }
  public void setM15Rate(Long m15Rate) {
    this.m15Rate = m15Rate;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("m30_rate")
  public Long getM30Rate() {
    return m30Rate;
  }
  public void setM30Rate(Long m30Rate) {
    this.m30Rate = m30Rate;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("h1_rate")
  public Long getH1Rate() {
    return h1Rate;
  }
  public void setH1Rate(Long h1Rate) {
    this.h1Rate = h1Rate;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("h6_rate")
  public Long getH6Rate() {
    return h6Rate;
  }
  public void setH6Rate(Long h6Rate) {
    this.h6Rate = h6Rate;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("h12_rate")
  public Long getH12Rate() {
    return h12Rate;
  }
  public void setH12Rate(Long h12Rate) {
    this.h12Rate = h12Rate;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("h24_rate")
  public Long getH24Rate() {
    return h24Rate;
  }
  public void setH24Rate(Long h24Rate) {
    this.h24Rate = h24Rate;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("mean_rate")
  public Long getMeanRate() {
    return meanRate;
  }
  public void setMeanRate(Long meanRate) {
    this.meanRate = meanRate;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("units")
  public String getUnits() {
    return units;
  }
  public void setUnits(String units) {
    this.units = units;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class MeterJson {\n");

    sb.append("    count: ").append(StringUtil.toIndentedString(count)).append("\n");
    sb.append("    m1Rate: ").append(StringUtil.toIndentedString(m1Rate)).append("\n");
    sb.append("    m5Rate: ").append(StringUtil.toIndentedString(m5Rate)).append("\n");
    sb.append("    m15Rate: ").append(StringUtil.toIndentedString(m15Rate)).append("\n");
    sb.append("    m30Rate: ").append(StringUtil.toIndentedString(m30Rate)).append("\n");
    sb.append("    h1Rate: ").append(StringUtil.toIndentedString(h1Rate)).append("\n");
    sb.append("    h6Rate: ").append(StringUtil.toIndentedString(h6Rate)).append("\n");
    sb.append("    h12Rate: ").append(StringUtil.toIndentedString(h12Rate)).append("\n");
    sb.append("    h24Rate: ").append(StringUtil.toIndentedString(h24Rate)).append("\n");
    sb.append("    meanRate: ").append(StringUtil.toIndentedString(meanRate)).append("\n");
    sb.append("    units: ").append(StringUtil.toIndentedString(units)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
