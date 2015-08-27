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

import javax.jms.Message;
import java.util.List;

public interface JmsMessageConverter {

  List<Stage.ConfigIssue> init(Source.Context context);

  int convert(BatchMaker batchMaker, Source.Context context, String messsageId, Message message) throws StageException;
}
