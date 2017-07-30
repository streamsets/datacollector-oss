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
public class TimerJson   {

  private Long count = null;
  private Long max = null;
  private Long mean = null;
  private Long min = null;
  private Long p50 = null;
  private Long p75 = null;
  private Long p95 = null;
  private Long p98 = null;
  private Long p99 = null;
  private Long p999 = null;
  private Long stddev = null;
  private Long m15Rate = null;
  private Long m1Rate = null;
  private Long m5Rate = null;
  private Long meanRate = null;
  private String durationUnits = null;
  private String rateUnits = null;


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
  @JsonProperty("max")
  public Long getMax() {
    return max;
  }
  public void setMax(Long max) {
    this.max = max;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("mean")
  public Long getMean() {
    return mean;
  }
  public void setMean(Long mean) {
    this.mean = mean;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("min")
  public Long getMin() {
    return min;
  }
  public void setMin(Long min) {
    this.min = min;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("p50")
  public Long getP50() {
    return p50;
  }
  public void setP50(Long p50) {
    this.p50 = p50;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("p75")
  public Long getP75() {
    return p75;
  }
  public void setP75(Long p75) {
    this.p75 = p75;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("p95")
  public Long getP95() {
    return p95;
  }
  public void setP95(Long p95) {
    this.p95 = p95;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("p98")
  public Long getP98() {
    return p98;
  }
  public void setP98(Long p98) {
    this.p98 = p98;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("p99")
  public Long getP99() {
    return p99;
  }
  public void setP99(Long p99) {
    this.p99 = p99;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("p999")
  public Long getP999() {
    return p999;
  }
  public void setP999(Long p999) {
    this.p999 = p999;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("stddev")
  public Long getStddev() {
    return stddev;
  }
  public void setStddev(Long stddev) {
    this.stddev = stddev;
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
  @JsonProperty("duration_units")
  public String getDurationUnits() {
    return durationUnits;
  }
  public void setDurationUnits(String durationUnits) {
    this.durationUnits = durationUnits;
  }


  /**
   **/
  @ApiModelProperty(value = "")
  @JsonProperty("rate_units")
  public String getRateUnits() {
    return rateUnits;
  }
  public void setRateUnits(String rateUnits) {
    this.rateUnits = rateUnits;
  }



  @Override
  public String toString()  {
    StringBuilder sb = new StringBuilder();
    sb.append("class TimerJson {\n");

    sb.append("    count: ").append(StringUtil.toIndentedString(count)).append("\n");
    sb.append("    max: ").append(StringUtil.toIndentedString(max)).append("\n");
    sb.append("    mean: ").append(StringUtil.toIndentedString(mean)).append("\n");
    sb.append("    min: ").append(StringUtil.toIndentedString(min)).append("\n");
    sb.append("    p50: ").append(StringUtil.toIndentedString(p50)).append("\n");
    sb.append("    p75: ").append(StringUtil.toIndentedString(p75)).append("\n");
    sb.append("    p95: ").append(StringUtil.toIndentedString(p95)).append("\n");
    sb.append("    p98: ").append(StringUtil.toIndentedString(p98)).append("\n");
    sb.append("    p99: ").append(StringUtil.toIndentedString(p99)).append("\n");
    sb.append("    p999: ").append(StringUtil.toIndentedString(p999)).append("\n");
    sb.append("    stddev: ").append(StringUtil.toIndentedString(stddev)).append("\n");
    sb.append("    m15Rate: ").append(StringUtil.toIndentedString(m15Rate)).append("\n");
    sb.append("    m1Rate: ").append(StringUtil.toIndentedString(m1Rate)).append("\n");
    sb.append("    m5Rate: ").append(StringUtil.toIndentedString(m5Rate)).append("\n");
    sb.append("    meanRate: ").append(StringUtil.toIndentedString(meanRate)).append("\n");
    sb.append("    durationUnits: ").append(StringUtil.toIndentedString(durationUnits)).append("\n");
    sb.append("    rateUnits: ").append(StringUtil.toIndentedString(rateUnits)).append("\n");
    sb.append("}");
    return sb.toString();
  }
}
