/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.lib.util;

import org.apache.avro.Schema;

/**
 * This class provides a way to check whether Avro Schema class supports the LogicalType class. For example: Avro 1.8
 * library does support it while Avro 1.7 does not support it. If LogicalType class is supported, it can be used when
 * calling methods provided by Avro library. Additionally, if LogicalType is supported Schema class provides the
 * getLogicalType() method, otherwise it does not provide that method.
 */
public class AvroLogicalTypeSupport {

  private final boolean logicalTypeExists;

  private AvroLogicalTypeSupport(boolean logicalTypeExists) {
    this.logicalTypeExists = logicalTypeExists;
  }

  public boolean isLogicalTypeSupported() {
    return logicalTypeExists;
  }

  /**
   * Checks if Avro Schema class supports LogicalType class or not by checking if it implements the getLogicalType()
   * method and creates an instance of this class holding this information.
   *
   * @return the AvroLogicalTypeSupport instance which knows if LogicalType is supported or not by the current Avro
   * Schema class.
   */
  public static AvroLogicalTypeSupport getAvroLogicalTypeSupport() {
    AvroLogicalTypeSupport avroLogicalTypeSupport;
    Class schemaClass = Schema.class;
    try {
      schemaClass.getDeclaredMethod("getLogicalType", null); // If exception is not thrown we know method exists
      avroLogicalTypeSupport = new AvroLogicalTypeSupport(true);
    } catch (NoSuchMethodException e) {
      avroLogicalTypeSupport = new AvroLogicalTypeSupport(false);
    }

    return avroLogicalTypeSupport;
  }

}
