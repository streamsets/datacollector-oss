/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.data.DataFactoryBuilder;

public class DataGeneratorFactoryBuilder extends
  DataFactoryBuilder<DataGeneratorFactoryBuilder, DataGeneratorFactory, DataGeneratorFormat> {

  public DataGeneratorFactoryBuilder(Stage.Context context, DataGeneratorFormat format) {
    super(context, format);
  }

}
