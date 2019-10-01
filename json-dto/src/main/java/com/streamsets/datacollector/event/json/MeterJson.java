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
package com.streamsets.datacollector.event.json;

public class MeterJson {
  private long count;
  private double m1_rate;
  private double m5_rate;
  private double m15_rate;
  private double m30_rate;
  private double h1_rate;
  private double h6_rate;
  private double h12_rate;
  private double h24_rate;
  private double mean_rate;
  private String units;

  public MeterJson() {

  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public double getM1_rate() {
    return m1_rate;
  }

  public void setM1_rate(double m1_rate) {
    this.m1_rate = m1_rate;
  }

  public double getM5_rate() {
    return m5_rate;
  }

  public void setM5_rate(double m5_rate) {
    this.m5_rate = m5_rate;
  }

  public double getM15_rate() {
    return m15_rate;
  }

  public void setM15_rate(double m15_rate) {
    this.m15_rate = m15_rate;
  }

  public double getM30_rate() {
    return m30_rate;
  }

  public void setM30_rate(double m30_rate) {
    this.m30_rate = m30_rate;
  }

  public double getH1_rate() {
    return h1_rate;
  }

  public void setH1_rate(double h1_rate) {
    this.h1_rate = h1_rate;
  }

  public double getH6_rate() {
    return h6_rate;
  }

  public void setH6_rate(double h6_rate) {
    this.h6_rate = h6_rate;
  }

  public double getH12_rate() {
    return h12_rate;
  }

  public void setH12_rate(double h12_rate) {
    this.h12_rate = h12_rate;
  }

  public double getH24_rate() {
    return h24_rate;
  }

  public void setH24_rate(double h24_rate) {
    this.h24_rate = h24_rate;
  }

  public double getMean_rate() {
    return mean_rate;
  }

  public void setMean_rate(double mean_rate) {
    this.mean_rate = mean_rate;
  }

  public String getUnits() {
    return units;
  }

  public void setUnits(String units) {
    this.units = units;
  }
}
