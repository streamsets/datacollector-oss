/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.restapi.bean;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.record.RecordImpl;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestRecordBean {

  @Test
  public void testWrapRecord() throws IOException {
    List<Record> records = new ArrayList<>();
    RecordImpl r = new RecordImpl("stage", "source", null, null);
    com.streamsets.pipeline.api.Field f = com.streamsets.pipeline.api.Field.create(true);
    r.set(f);
    records.add(r);

    List<RecordJson> recordJsonList = BeanHelper.wrapRecords(records);
    System.out.println(ObjectMapperFactory.get().writeValueAsString(recordJsonList));
  }

  @Test
  public void testUnwrapRecord() throws IOException {
    String recordString = "{\n" +
      "  \"header\": {\n" +
      "    \"stageCreator\": \"com_streamsets_pipeline_lib_stage_devtest_RandomDataGenerator1423946816761\",\n" +
      "    \"sourceId\": \"random:2\",\n" +
      "    \"stagesPath\": \"com_streamsets_pipeline_lib_stage_devtest_RandomDataGenerator1423946816761\",\n" +
      "    \"trackingId\": \"random:2::com_streamsets_pipeline_lib_stage_devtest_RandomDataGenerator1423946816761\",\n" +
      "    \"previousTrackingId\": null,\n" +
      "    \"raw\": null,\n" +
      "    \"rawMimeType\": null,\n" +
      "    \"errorDataCollectorId\": null,\n" +
      "    \"errorPipelineName\": null,\n" +
      "    \"errorStage\": null,\n" +
      "    \"errorCode\": null,\n" +
      "    \"errorMessage\": null,\n" +
      "    \"errorTimestamp\": 0,\n" +
      "    \"values\": {}\n" +
      "  },\n" +
      "  \"value\": {\n" +
      "    \"path\": \"\",\n" +
      "    \"type\": \"MAP\",\n" +
      "    \"value\": {\n" +
      "      \"X\": {\n" +
      "        \"path\": \"/X\",\n" +
      "        \"type\": \"BYTE_ARRAY\",\n" +
      "        \"value\": \"U3RyZWFtU2V0cyBJbmMsIFNhbiBGcmFuY2lzY28=\"\n" +
      "      }\n" +
      "    }\n" +
      "  }\n" +
      "}";
    RecordJson recordJson =
      ObjectMapperFactory.get().readValue(recordString, RecordJson.class);

  }


}
