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

public class HistogramJson {
  private long count;
  private long max;
  private double mean;
  private long min;
  private double p50;
  private double p75;
  private double p95;
  private double p98;
  private double p99;
  private double p999;
  private double stddev;

  public HistogramJson() {

  }

  public long getCount() {
    return count;
  }

  public long getMax() {
    return max;
  }

  public double getMean() {
    return mean;
  }

  public long getMin() {
    return min;
  }

  public double getP50() {
    return p50;
  }

  public double getP75() {
    return p75;
  }

  public double getP95() {
    return p95;
  }

  public double getP98() {
    return p98;
  }

  public double getP99() {
    return p99;
  }

  public double getP999() {
    return p999;
  }

  public double getStddev() {
    return stddev;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public void setMax(long max) {
    this.max = max;
  }

  public void setMean(double mean) {
    this.mean = mean;
  }

  public void setMin(long min) {
    this.min = min;
  }

  public void setP50(double p50) {
    this.p50 = p50;
  }

  public void setP75(double p75) {
    this.p75 = p75;
  }

  public void setP95(double p95) {
    this.p95 = p95;
  }

  public void setP98(double p98) {
    this.p98 = p98;
  }

  public void setP99(double p99) {
    this.p99 = p99;
  }

  public void setP999(double p999) {
    this.p999 = p999;
  }

  public void setStddev(double stddev) {
    this.stddev = stddev;
  }
}
