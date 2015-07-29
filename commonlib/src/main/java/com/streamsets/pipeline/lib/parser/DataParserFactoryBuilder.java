/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.data.DataFactoryBuilder;

public class DataParserFactoryBuilder extends
    DataFactoryBuilder<DataParserFactoryBuilder, DataParserFactory, DataParserFormat> {

  public DataParserFactoryBuilder(Stage.Context context, DataParserFormat format) {
    super(context, format);
  }

  @Override
  public DataParserFactory build() {
    return new WrapperDataParserFactory(super.build());
  }

}
