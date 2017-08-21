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
package com.streamsets.pipeline.lib.generator;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.data.DataFormat;
import com.streamsets.pipeline.lib.generator.avro.AvroDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.binary.BinaryDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.delimited.DelimitedDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.json.JsonDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.protobuf.ProtobufDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.sdcrecord.SdcRecordDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.text.TextDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.wholefile.WholeFileDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.xml.XmlDataGeneratorFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;

public enum DataGeneratorFormat implements DataFormat<DataGeneratorFactory> {
  TEXT(TextDataGeneratorFactory.class, TextDataGeneratorFactory.MODES, TextDataGeneratorFactory.CONFIGS),
  JSON(JsonDataGeneratorFactory.class, JsonDataGeneratorFactory.MODES, JsonDataGeneratorFactory.CONFIGS),
  DELIMITED(DelimitedDataGeneratorFactory.class, DelimitedDataGeneratorFactory.MODES,
    DelimitedDataGeneratorFactory.CONFIGS),
  XML(XmlDataGeneratorFactory.class, XmlDataGeneratorFactory.MODES,
      XmlDataGeneratorFactory.CONFIGS),
  SDC_RECORD(SdcRecordDataGeneratorFactory.class, SdcRecordDataGeneratorFactory.MODES,
    SdcRecordDataGeneratorFactory.CONFIGS),
  AVRO(AvroDataGeneratorFactory.class, AvroDataGeneratorFactory.MODES, AvroDataGeneratorFactory.CONFIGS),
  BINARY(BinaryDataGeneratorFactory.class, BinaryDataGeneratorFactory.MODES, BinaryDataGeneratorFactory.CONFIGS),
  PROTOBUF(ProtobufDataGeneratorFactory.class, ProtobufDataGeneratorFactory.MODES, ProtobufDataGeneratorFactory.CONFIGS),
  WHOLE_FILE(WholeFileDataGeneratorFactory.class, WholeFileDataGeneratorFactory.MODES, WholeFileDataGeneratorFactory.CONFIGS),
  ;

  private final Class<? extends DataGeneratorFactory> klass;
  private final Constructor<? extends DataGeneratorFactory> constructor;
  private final Set<Class<? extends Enum>> modes;
  private Map<String, Object> configs;

  DataGeneratorFormat(Class<? extends DataGeneratorFactory> klass, Set<Class<? extends Enum>> modes,
                   Map<String, Object> configs) {
    this.klass = klass;
    try {
      constructor = klass.getConstructor(DataFactory.Settings.class);
      Utils.checkState((constructor.getModifiers() & Modifier.PUBLIC) != 0,
        Utils.formatL("Constructor for DataFactory '{}' must be public",
          klass.getName()));
    } catch (Exception ex) {
      throw new RuntimeException(Utils.format("Could not obtain constructor '<init>({})' for DataFactory '{}': {}",
        DataFactory.Settings.class, klass.getName(), ex.toString()), ex);
    }
    this.modes = modes;
    this.configs = configs;

  }

  @Override
  public Set<Class<? extends Enum>> getModes() {
    return modes;
  }

  @Override
  public Map<String, Object> getConfigs() {
    return configs;
  }

  @Override
  public DataGeneratorFactory create(DataFactory.Settings settings) {
    try {
      return constructor.newInstance(settings);
    } catch (Exception ex) {
      Throwable cause = ex;
      if (ex.getCause() != null) {
        cause = ex.getCause();
      }
      throw new RuntimeException(Utils.format("Could not create DataFactory instance for '{}': {}",
        klass.getName(), cause.toString()), cause);
    }
  }
}
