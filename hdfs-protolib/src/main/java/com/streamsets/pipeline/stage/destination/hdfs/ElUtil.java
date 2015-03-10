/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.lib.el.MiscEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;

public class ElUtil {

  public static ELEval createDirPathTemplateEval(Stage.ElEvalProvider elEvalProvider) {
    return elEvalProvider.createELEval("dirPathTemplate", RecordEL.class, TimeEL.class);
  }

  public static ELEval createTimeDriverEval(Stage.ElEvalProvider elEvalProvider) {
    return elEvalProvider.createELEval("timeDriver", RecordEL.class, TimeEL.class);
  }

  public static ELEval createKeyElEval(Stage.ElEvalProvider elEvalProvider) {
    return elEvalProvider.createELEval("keyEl", RecordEL.class, MiscEL.class);
  }

  public static ELEval createLateRecordsLimitEval(Stage.ElEvalProvider elEvalProvider) {
    return elEvalProvider.createELEval("lateRecordsLimit", TimeEL.class);
  }

  public static ELEval createLateRecordsDirPathTemplateEval (Stage.ElEvalProvider elEvalProvider) {
    return elEvalProvider.createELEval("lateRecordsDirPathTemplate", RecordEL.class, TimeEL.class);
  }

}
