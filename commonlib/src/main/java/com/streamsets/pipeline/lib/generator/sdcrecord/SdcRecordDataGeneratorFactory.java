/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.sdcrecord;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SdcRecordDataGeneratorFactory extends DataGeneratorFactory {

  public static final Map<String, Object> CONFIGS = new HashMap<>();

  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of();

  private final ContextExtensions context;

  public SdcRecordDataGeneratorFactory(Settings settings) {
    super(settings);
    this.context = (ContextExtensions) settings.getContext();
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException, DataGeneratorException {
    return new SdcRecordDataGenerator(context.createRecordWriter(os));
  }

}
