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
package com.streamsets.pipeline.lib.generator.wholefile;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.config.ChecksumAlgorithm;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.StreamCloseEventHandler;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WholeFileDataGeneratorFactory extends DataGeneratorFactory {
  public static final Map<String, Object> CONFIGS = new HashMap<>();
  public static final Set<Class<? extends Enum>> MODES = ImmutableSet.of();


  public static final String INCLUDE_CHECKSUM_IN_THE_EVENTS_KEY = "includeChecksumInTheEvents";
  public static final String CHECKSUM_ALGO_KEY = "checksumAlgorithm";

  static {
    CONFIGS.put(INCLUDE_CHECKSUM_IN_THE_EVENTS_KEY, false);
    CONFIGS.put(CHECKSUM_ALGO_KEY, ChecksumAlgorithm.MD5);
  }


  public WholeFileDataGeneratorFactory(Settings settings) {
    super(settings);
  }

  @Override
  public DataGenerator getGenerator(OutputStream os, StreamCloseEventHandler<?> streamCloseEventHandler) throws IOException {
    FileRefUtil.initMetricsIfNeeded(getSettings().getContext());
    return new WholeFileDataGenerator(
        getSettings().getContext(),
        os,
        (boolean) getSettings().getConfig(INCLUDE_CHECKSUM_IN_THE_EVENTS_KEY),
        (ChecksumAlgorithm) getSettings().getConfig(CHECKSUM_ALGO_KEY),
        streamCloseEventHandler
    );
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    return getGenerator(os, null);
  }
}
