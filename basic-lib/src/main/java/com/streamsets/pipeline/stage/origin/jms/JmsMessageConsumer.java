/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.jms;


import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;

import java.util.List;

public interface JmsMessageConsumer {

  List<Stage.ConfigIssue> init(Source.Context context);

  int take(BatchMaker batchMaker, Source.Context context, int batchSize, String messageId) throws StageException;

  void commit() throws StageException;

  void rollback() throws StageException;

  void close();
}
