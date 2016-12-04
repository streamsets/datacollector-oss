/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.stage.destination.datalake.writer;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.el.StringEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.stage.destination.datalake.DataLakeTarget;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class TestRecordWriter {
  @Test
  public void validateFilePath() throws StageException {
    Target.Context context = ContextInfoCreator.createTargetContext(
        DataLakeTarget.class,
        "n",
        false,
        OnRecordError.DISCARD,
        null
    );

    ELEval dirPathTemplateEval = context.createELEval("dirPathTemplate2", StringEL.class, TimeEL.class, TimeNowEL.class);
    ELVars dirPathTemplateVars = context.createELVars();

    final String dirPath = "/tmp/output/";
    final String dirPathTemplate = dirPath + "${YYYY()}-${MM()}-${DD()}";
    final String TEST_STRING = "test";
    final String MIME = "text/plain";
    int i = 1;

    Record r = RecordCreator.create("text", "s:" + i, (TEST_STRING + i).getBytes(), MIME);
    r.set(Field.create((TEST_STRING+ i)));
    final String uniquePrefix = "sdc";
    final String fileNameEL = "";
    final String timeZoneID = "UTC";

    Date date = Calendar.getInstance(TimeZone.getTimeZone(timeZoneID)).getTime();
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    TimeEL.setCalendarInContext(dirPathTemplateVars, calendar);
    TimeNowEL.setTimeNowInContext(dirPathTemplateVars, date);

    final String year = String.valueOf(calendar.get(Calendar.YEAR));
    final String month = String.valueOf(Utils.intToPaddedString(calendar.get(Calendar.MONTH) + 1, 2));
    final String day =  String.valueOf(Utils.intToPaddedString(calendar.get(Calendar.DAY_OF_MONTH), 2));

    final String targetFilePathPrefix = dirPath + year + "-" + month + "-" + day + "/" + uniquePrefix + "-";

    RecordWriter recordWriter = new RecordWriter(
        null,
        DataFormat.TEXT,
        new DataGeneratorFormatConfig(),
        uniquePrefix,
        fileNameEL,
        dirPathTemplateEval,
        dirPathTemplateVars,
        timeZoneID,
        null
    );
    String filePath = recordWriter.getFilePath(dirPathTemplate, r, date);
    Assert.assertTrue(filePath.startsWith(targetFilePathPrefix));
  }
}
