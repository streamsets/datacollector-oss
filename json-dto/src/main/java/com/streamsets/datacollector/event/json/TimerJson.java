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

public class TimerJson {
  long count;
  double max;
  double mean;
  double min;
  double p50;
  double p75;
  double p95;
  double p98;
  double p99;
  double p999;
  double stddev;
  double m15_rate;
  double m1_rate;
  double m5_rate;
  double mean_rate;
  String duration_units;
  String rate_units;

  public TimerJson() {

  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public double getMax() {
    return max;
  }

  public void setMax(double max) {
    this.max = max;
  }

  public double getMean() {
    return mean;
  }

  public void setMean(double mean) {
    this.mean = mean;
  }

  public double getMin() {
    return min;
  }

  public void setMin(double min) {
    this.min = min;
  }

  public double getP50() {
    return p50;
  }

  public void setP50(double p50) {
    this.p50 = p50;
  }

  public double getP75() {
    return p75;
  }

  public void setP75(double p75) {
    this.p75 = p75;
  }

  public double getP95() {
    return p95;
  }

  public void setP95(double p95) {
    this.p95 = p95;
  }

  public double getP98() {
    return p98;
  }

  public void setP98(double p98) {
    this.p98 = p98;
  }

  public double getP99() {
    return p99;
  }

  public void setP99(double p99) {
    this.p99 = p99;
  }

  public double getP999() {
    return p999;
  }

  public void setP999(double p999) {
    this.p999 = p999;
  }

  public double getStddev() {
    return stddev;
  }

  public void setStddev(double stddev) {
    this.stddev = stddev;
  }

  public double getM15_rate() {
    return m15_rate;
  }

  public void setM15_rate(double m15_rate) {
    this.m15_rate = m15_rate;
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

  public double getMean_rate() {
    return mean_rate;
  }

  public void setMean_rate(double mean_rate) {
    this.mean_rate = mean_rate;
  }

  public String getDuration_units() {
    return duration_units;
  }

  public void setDuration_units(String duration_units) {
    this.duration_units = duration_units;
  }

  public String getRate_units() {
    return rate_units;
  }

  public void setRate_units(String rate_units) {
    this.rate_units = rate_units;
  }
}
