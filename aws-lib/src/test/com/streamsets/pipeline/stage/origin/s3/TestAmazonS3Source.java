/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.regions.Regions;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import com.streamsets.pipeline.stage.origin.lib.DataFormatConfig;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

@Ignore
public class TestAmazonS3Source {

  private AmazonS3Source createSource() {

    S3ConfigBean s3ConfigBean = new S3ConfigBean();
    s3ConfigBean.basicConfig = new BasicConfig();
    s3ConfigBean.basicConfig.maxWaitTime = 1000;
    s3ConfigBean.basicConfig.maxBatchSize = 60000;

    s3ConfigBean.dataFormatConfig = new DataFormatConfig();
    s3ConfigBean.dataFormatConfig.dataFormat = DataFormat.LOG;
    s3ConfigBean.dataFormatConfig.charset = "UTF-8";
    s3ConfigBean.dataFormatConfig.logMode = LogMode.COMMON_LOG_FORMAT;
    s3ConfigBean.dataFormatConfig.logMaxObjectLen = 1024;

    s3ConfigBean.errorConfig = new S3ErrorConfig();
    s3ConfigBean.errorConfig.errorFolder = "test-error-folder";
    s3ConfigBean.errorConfig.errorBucket = "test-error-bucket";

    s3ConfigBean.postProcessingConfig = new S3PostProcessingConfig();
    s3ConfigBean.postProcessingConfig.archivingOption = S3ArchivingOption.MOVE_TO_BUCKET;
    s3ConfigBean.postProcessingConfig.postProcessing = PostProcessingOptions.ARCHIVE;
    s3ConfigBean.postProcessingConfig.postProcessBucket = "test-postprocessing-bucket";
    s3ConfigBean.postProcessingConfig.postProcessFolder = "folder/";

    s3ConfigBean.s3FileConfig = new S3FileConfig();
    s3ConfigBean.s3FileConfig.maxSpoolObjects = 20;
    s3ConfigBean.s3FileConfig.overrunLimit = 65*1000;

    s3ConfigBean.s3Config = new S3Config();
    s3ConfigBean.s3Config.region = Regions.US_WEST_1;
    s3ConfigBean.s3Config.bucket = "test-error-bucket";
    s3ConfigBean.s3Config.accessKeyId = "AKIAJ6S5Q43F4BT6ZJLQ";
    s3ConfigBean.s3Config.secretAccessKey = "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";
    s3ConfigBean.s3Config.folder = "test-error-folder/";
    s3ConfigBean.s3Config.prefix = "common";
    s3ConfigBean.s3Config.delimiter = "/";

    return new AmazonS3Source(s3ConfigBean);
  }

  @Test
  public void testProduceFullFile() throws Exception {
    AmazonS3Source source = createSource();
    SourceRunner runner = new SourceRunner.Builder(AmazonS3DSource.class, source).addOutputLane("lane").build();
    runner.runInit();
    try {
      List<Record> allRecords = new ArrayList<>();
      String offset = null;
      for(int j = 0; j < 12; j++){
        BatchMaker batchMaker = SourceRunner.createTestBatchMaker("lane");
        offset = source.produce(offset, 60000, batchMaker);
        Assert.assertNotNull(offset);
        StageRunner.Output output = SourceRunner.getOutput(batchMaker);
        List<Record> records = output.getRecords().get("lane");
        System.out.println("Produced " + records.size() + " records");
        System.out.println("Current offset : " + offset);
        allRecords.addAll(records);
        Assert.assertNotNull(records);
      }

      source.produce(offset, 60000, SourceRunner.createTestBatchMaker("lane"));

      System.out.println("Total records produced : " + allRecords.size());
    } finally {
      runner.runDestroy();
    }
  }
}
