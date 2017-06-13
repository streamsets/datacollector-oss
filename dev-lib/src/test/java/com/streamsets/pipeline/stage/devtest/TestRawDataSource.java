/**
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
package com.streamsets.pipeline.stage.devtest;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.devtest.rawdata.RawDataSource;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestRawDataSource {

  String utf8 = "UTF8: € greek όλα τα ελληνικά σε μένα japanese 天気の良い日 thai วันที่ดี data.";
  String gbk = "GBK: japanese 天気の良い日 trad chinese 傳統 simplified chinese 中国是美丽的 data.";

  @Test
  public void testRawDataSourceUtf8OK() throws StageException {


    DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();
    dataFormatConfig.charset = "UTF-8";

    RawDataSource origin = new RawDataSource(DataFormat.TEXT, dataFormatConfig, utf8);

    SourceRunner runner = new SourceRunner.Builder(RawDataSource.class, origin)
        .addOutputLane("a")
        .build();

    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1);
      List<Record> records = output.getRecords().get("a");
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(Field.Type.STRING, records.get(0).get("/text").getType());
      Assert.assertEquals(utf8, records.get(0).get("/text").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRawDataSourceGBK5OK() throws StageException {


    DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();
    dataFormatConfig.charset = "GBK";

    RawDataSource origin = new RawDataSource(DataFormat.TEXT, dataFormatConfig, gbk);

    SourceRunner runner = new SourceRunner.Builder(RawDataSource.class, origin)
        .addOutputLane("a")
        .build();

    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1);
      List<Record> records = output.getRecords().get("a");
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(Field.Type.STRING, records.get(0).get("/text").getType());
      Assert.assertEquals(gbk, records.get(0).get("/text").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testRawDataSourceBig5NotOK() throws StageException {


    DataParserFormatConfig dataFormatConfig = new DataParserFormatConfig();
    dataFormatConfig.charset = "GBK";

    RawDataSource origin = new RawDataSource(DataFormat.TEXT, dataFormatConfig, utf8);

    SourceRunner runner = new SourceRunner.Builder(RawDataSource.class, origin)
        .addOutputLane("a")
        .build();

    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1);
      List<Record> records = output.getRecords().get("a");
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(Field.Type.STRING, records.get(0).get("/text").getType());
      Assert.assertNotEquals(utf8, records.get(0).get("/text").getValueAsString());
    } finally {
      runner.runDestroy();
    }
  }

}
