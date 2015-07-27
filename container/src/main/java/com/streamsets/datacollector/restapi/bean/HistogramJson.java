/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.datacollector.restapi.bean;

public class HistogramJson {
  private long count;
  private long max;
  private long mean;
  private long min;
  private long p50;
  private long p75;
  private long p95;
  private long p98;
  private long p99;
  private long p999;
  private long stddev;

  public HistogramJson() {

  }

  public long getCount() {
    return count;
  }

  public long getMax() {
    return max;
  }

  public long getMean() {
    return mean;
  }

  public long getMin() {
    return min;
  }

  public long getP50() {
    return p50;
  }

  public long getP75() {
    return p75;
  }

  public long getP95() {
    return p95;
  }

  public long getP98() {
    return p98;
  }

  public long getP99() {
    return p99;
  }

  public long getP999() {
    return p999;
  }

  public long getStddev() {
    return stddev;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public void setMax(long max) {
    this.max = max;
  }

  public void setMean(long mean) {
    this.mean = mean;
  }

  public void setMin(long min) {
    this.min = min;
  }

  public void setP50(long p50) {
    this.p50 = p50;
  }

  public void setP75(long p75) {
    this.p75 = p75;
  }

  public void setP95(long p95) {
    this.p95 = p95;
  }

  public void setP98(long p98) {
    this.p98 = p98;
  }

  public void setP99(long p99) {
    this.p99 = p99;
  }

  public void setP999(long p999) {
    this.p999 = p999;
  }

  public void setStddev(long stddev) {
    this.stddev = stddev;
  }
}
