/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.testing.ParametrizedUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Collections;

@RunWith(Parameterized.class)
public class TestFieldTypeConverterProcessorNulls {

  @Parameterized.Parameters(name = "{0} -> {1}")
  public static Collection<Object[]> offsets() {
    return ParametrizedUtils.crossProduct(
      Field.Type.values(),
      Field.Type.values()
    );
  }

  @Parameterized.Parameter(0)
  public Field.Type inputType;

  @Parameterized.Parameter(1)
  public Field.Type targetType;

  @Test
  public void runTest() throws StageException {
    FieldTypeConverterConfig fieldTypeConverterConfig = new FieldTypeConverterConfig();
    fieldTypeConverterConfig.fields = Collections.singletonList("/path");
    fieldTypeConverterConfig.targetType = targetType;
    fieldTypeConverterConfig.dataLocale = "en";
    fieldTypeConverterConfig.encoding = "UTF8";

    ProcessorRunner runner = new ProcessorRunner.Builder(FieldTypeConverterDProcessor.class)
      .setOnRecordError(OnRecordError.TO_ERROR)
      .addConfiguration("convertBy", ConvertBy.BY_FIELD)
      .addConfiguration("fieldTypeConverterConfigs", Collections.singletonList(fieldTypeConverterConfig))
      .addOutputLane("a").build();
    runner.runInit();

    try {
     Record record = RecordCreator.create("s", "s:1");
     record.set(Field.create(Collections.singletonMap("path", Field.create(inputType, null))));

      StageRunner.Output output = runner.runProcess(Collections.singletonList(record));

      // ZONED_DATETIME have explicit check to limit the allowed conversions
      if(inputType == Field.Type.ZONED_DATETIME && !targetType.isOneOf(Field.Type.STRING, Field.Type.ZONED_DATETIME)) {
        Assert.assertEquals(1, runner.getErrorRecords().size());
        return;
      }

      Assert.assertEquals(1, output.getRecords().get("a").size());
      Record outputRecord = output.getRecords().get("a").get(0);

      Assert.assertTrue(outputRecord.has("/path"));
      Assert.assertEquals(targetType, outputRecord.get("/path").getType());
      Assert.assertNull(outputRecord.get("/path").getValue());

    } finally {
      runner.runDestroy();
    }
  }
}
