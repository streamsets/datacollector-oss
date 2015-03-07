/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.el.MiscEl;
import com.streamsets.pipeline.el.RecordEl;
import com.streamsets.pipeline.el.TimeEl;

public class ElUtil {

  public static ELEval createDirPathTemplateEval(Stage.ElEvalProvider elEvalProvider) {
    return elEvalProvider.createELEval("dirPathTemplate", RecordEl.class, TimeEl.class);
  }

  public static ELEval createTimeDriverEval(Stage.ElEvalProvider elEvalProvider) {
    return elEvalProvider.createELEval("timeDriver", RecordEl.class, TimeEl.class);
  }

  public static ELEval createKeyElEval(Stage.ElEvalProvider elEvalProvider) {
    return elEvalProvider.createELEval("keyEl", RecordEl.class, MiscEl.class);
  }

  public static ELEval createLateRecordsLimitEval(Stage.ElEvalProvider elEvalProvider) {
    return elEvalProvider.createELEval("lateRecordsLimit", TimeEl.class);
  }

  public static ELEval createLateRecordsDirPathTemplateEval (Stage.ElEvalProvider elEvalProvider) {
    return elEvalProvider.createELEval("lateRecordsDirPathTemplate", RecordEl.class, TimeEl.class);
  }

}
