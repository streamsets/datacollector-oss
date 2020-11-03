/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.inspector.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.streamsets.datacollector.util.ProcessUtil;
import com.streamsets.pipeline.api.impl.Utils;

public class HealthCheck {

  /**
   * Severity of the result entry.
   */
  public enum Severity {
    /**
     * Green means all good.
     */
    GREEN,
    /**
     * Yellow is somehow worrisome, but not necessary concerning.
     */
    YELLOW,
    /**
     * Red items are generally concerning and are implying some sort of misconfiguration or possible issue.
     */
    RED,

    ;

    /**
     * Evaluate long value against an interval.
     */
    public static Severity smallerIsBetter(long value, long greenToYellow, long yellowToRed) {
      if(value <= greenToYellow) {
        return Severity.GREEN;
      } else if (value <= yellowToRed) {
        return Severity.YELLOW;
      } else {
        return Severity.RED;
      }
    }

    /**
     * Evaluate long value against an interval.
     */
    public static Severity higherIsBetter(long value, long greenToYellow, long yellowToRed) {
      if(value >= greenToYellow) {
        return Severity.GREEN;
      } else if (value >= yellowToRed) {
        return Severity.YELLOW;
      } else {
        return Severity.RED;
      }
    }
  }

  /**
   * Human readable name of the entry.
   */
  private final String name;
  public String getName() {
    return name;
  }

  /**
   * If the performed validation have a distinct value (like configuration value or number of threads).
   *
   * This might be null if the validation doesn't have an inherent value (like traceroute).
   */
  private final Object value;
  public Object getValue() {
    return value;
  }


  /**
   * Severity of the result.
   */
  private final Severity severity;
  public Severity getSeverity() {
    return severity;
  }

  /**
   * Human readable description that can (optionally) provide more color then the name of the check.
   */
  private final String description;
  public String getDescription() {
    return description;
  }

  /**
   * Optional technical output with more details. Like log of the operation made, ... .
   */
  private final String details;
  public String getDetails() {
    return details;
  }

  @JsonCreator
  public HealthCheck(
      @JsonProperty("name") String name,
      @JsonProperty("value") Object value,
      @JsonProperty("severity") Severity severity,
      @JsonProperty("description") String description,
      @JsonProperty("details") String details
  ) {
    this.name = name;
    this.value = value;
    this.severity = severity;
    this.description = description;
    this.details = details;
  }

  public static class Builder {
    // Required fields
    private final String name;
    private final Severity severity;

    // Optional ones
    private Object value;
    private String description;
    private String details;

    public Builder(String name, Severity severity) {
      this.name = name;
      this.severity = severity;
    }

    public Builder withValue(Object value) {
      this.value = value;
      return this;
    }

    public Builder withDescription(String description, Object ...args) {
      if(args == null || args.length == 0) {
        this.description = description;
      } else {
        this.description = Utils.format(description, args);
      }
      return this;
    }

    public Builder withDetails(String details) {
      this.details = details;
      return this;
    }

    public Builder withDetails(ProcessUtil.Output output) {
      this.details =  Utils.format("success: {}\nstderr: \n{}\nstderr: \n{}\n", output.success, output.stdout, output.stderr);
      return this;
    }

    public HealthCheck build() {
      return new HealthCheck(
          name,
          value,
          severity,
          description,
          details
      );
    }
  }

}
