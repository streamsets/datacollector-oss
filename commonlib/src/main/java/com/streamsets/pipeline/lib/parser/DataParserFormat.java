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
package com.streamsets.pipeline.lib.parser;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.Compression;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.data.DataFormat;
import com.streamsets.pipeline.lib.parser.avro.AvroDataParserFactory;
import com.streamsets.pipeline.lib.parser.binary.BinaryDataParserFactory;
import com.streamsets.pipeline.lib.parser.delimited.DelimitedDataParserFactory;
import com.streamsets.pipeline.lib.parser.excel.WorkbookParserFactory;
import com.streamsets.pipeline.lib.parser.flowfile.FlowFileParserFactory;
import com.streamsets.pipeline.lib.parser.json.JsonDataParserFactory;
import com.streamsets.pipeline.lib.parser.log.LogDataParserFactory;
import com.streamsets.pipeline.lib.parser.net.netflow.NetflowDataParserFactory;
import com.streamsets.pipeline.lib.parser.protobuf.ProtobufDataParserFactory;
import com.streamsets.pipeline.lib.parser.sdcrecord.SdcRecordDataParserFactory;
import com.streamsets.pipeline.lib.parser.text.TextDataParserFactory;
import com.streamsets.pipeline.lib.parser.udp.DatagramParserFactory;
import com.streamsets.pipeline.lib.parser.net.syslog.SyslogDataParserFactory;
import com.streamsets.pipeline.lib.parser.wholefile.WholeFileDataParserFactory;
import com.streamsets.pipeline.lib.parser.xml.XmlDataParserFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;

public enum DataParserFormat implements DataFormat<DataParserFactory> {
  TEXT(TextDataParserFactory.class, TextDataParserFactory.MODES, TextDataParserFactory.CONFIGS),
  JSON(JsonDataParserFactory.class, JsonDataParserFactory.MODES, JsonDataParserFactory.CONFIGS),
  XML(XmlDataParserFactory.class, XmlDataParserFactory.MODES, XmlDataParserFactory.CONFIGS),
  DELIMITED(DelimitedDataParserFactory.class, DelimitedDataParserFactory.MODES, DelimitedDataParserFactory.CONFIGS),
  SDC_RECORD(SdcRecordDataParserFactory.class, SdcRecordDataParserFactory.MODES, SdcRecordDataParserFactory.CONFIGS),
  LOG(LogDataParserFactory.class, LogDataParserFactory.MODES, LogDataParserFactory.CONFIGS),
  AVRO(AvroDataParserFactory.class, AvroDataParserFactory.MODES, AvroDataParserFactory.CONFIGS),
  BINARY(BinaryDataParserFactory.class, BinaryDataParserFactory.MODES, BinaryDataParserFactory.CONFIGS),
  PROTOBUF(ProtobufDataParserFactory.class, ProtobufDataParserFactory.MODES, ProtobufDataParserFactory.CONFIGS),
  DATAGRAM(DatagramParserFactory.class, DatagramParserFactory.MODES, DatagramParserFactory.CONFIGS),
  WHOLE_FILE(WholeFileDataParserFactory.class, WholeFileDataParserFactory.MODES, WholeFileDataParserFactory.CONFIGS),
  SYSLOG(SyslogDataParserFactory.class, SyslogDataParserFactory.MODES, SyslogDataParserFactory.CONFIGS),
  NETFLOW(NetflowDataParserFactory.class, NetflowDataParserFactory.MODES, NetflowDataParserFactory.CONFIGS),
  EXCEL(WorkbookParserFactory.class, WorkbookParserFactory.MODES, WorkbookParserFactory.CONFIGS),
  FLOWFILE(FlowFileParserFactory.class, FlowFileParserFactory.MODES, FlowFileParserFactory.CONFIGS),
  ;

  private final Class<? extends DataParserFactory> klass;
  private final Constructor<? extends DataParserFactory> constructor;
  private final Set<Class<? extends Enum>> modes;
  private Map<String, Object> configs;

  DataParserFormat(Class<? extends DataParserFactory> klass, Set<Class<? extends Enum>> modes,
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
  public DataParserFactory create(DataFactory.Settings settings) {
    try {
      DataParserFactory dataParserFactory = constructor.newInstance(settings);
      if(settings.getCompression() != Compression.NONE) {
        dataParserFactory = new CompressionDataParserFactory(settings, dataParserFactory);
      }
      return dataParserFactory;
    } catch (Exception ex) {
      Throwable cause = ex;
      if (ex.getCause() != null) {
        cause = ex.getCause();
      }
      throw new RuntimeException(
        Utils.format("Could not create DataFactory instance for '{}': {}", klass.getName(), cause.toString()),
        cause
      );
    }
  }


}
