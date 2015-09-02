/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.restapi.bean;

public class TimerJson {
  long count;
  long max;
  long mean;
  long min;
  long p50;
  long p75;
  long p95;
  long p98;
  long p99;
  long p999;
  long stddev;
  long m15_rate;
  long m1_rate;
  long m5_rate;
  long mean_rate;
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

  public long getMax() {
    return max;
  }

  public void setMax(long max) {
    this.max = max;
  }

  public long getMean() {
    return mean;
  }

  public void setMean(long mean) {
    this.mean = mean;
  }

  public long getMin() {
    return min;
  }

  public void setMin(long min) {
    this.min = min;
  }

  public long getP50() {
    return p50;
  }

  public void setP50(long p50) {
    this.p50 = p50;
  }

  public long getP75() {
    return p75;
  }

  public void setP75(long p75) {
    this.p75 = p75;
  }

  public long getP95() {
    return p95;
  }

  public void setP95(long p95) {
    this.p95 = p95;
  }

  public long getP98() {
    return p98;
  }

  public void setP98(long p98) {
    this.p98 = p98;
  }

  public long getP99() {
    return p99;
  }

  public void setP99(long p99) {
    this.p99 = p99;
  }

  public long getP999() {
    return p999;
  }

  public void setP999(long p999) {
    this.p999 = p999;
  }

  public long getStddev() {
    return stddev;
  }

  public void setStddev(long stddev) {
    this.stddev = stddev;
  }

  public long getM15_rate() {
    return m15_rate;
  }

  public void setM15_rate(long m15_rate) {
    this.m15_rate = m15_rate;
  }

  public long getM1_rate() {
    return m1_rate;
  }

  public void setM1_rate(long m1_rate) {
    this.m1_rate = m1_rate;
  }

  public long getM5_rate() {
    return m5_rate;
  }

  public void setM5_rate(long m5_rate) {
    this.m5_rate = m5_rate;
  }

  public long getMean_rate() {
    return mean_rate;
  }

  public void setMean_rate(long mean_rate) {
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
