/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.play;

import com.codahale.metrics.Meter;
import com.streamsets.pipeline.api.v1.AbstractProcessor;
import com.streamsets.pipeline.api.v1.Batch;
import com.streamsets.pipeline.api.v1.BatchMaker;
import com.streamsets.pipeline.api.v1.Configuration;
import com.streamsets.pipeline.api.v1.ModuleDef;
import com.streamsets.pipeline.api.v1.Record;

import java.util.HashSet;
import java.util.Set;

@ModuleDef(name = "IdCounter", version = "0.1.0",
           description = "Counts how many different IDs go through per batch")
public class IdCounterProcessor extends AbstractProcessor {

  @Configuration(name = "id-field", type = Configuration.Type.STRING, defaultValue = "id")
  private String idField;

  private Meter meter;

  @Override
  protected void init() {
    meter = new Meter();
    getContext().getMetric().register(getInfo().getInstance() + ":idCounter.meter", meter);
  }

  @Override
  public void process(Batch batch, BatchMaker batchMaker) {
    Set<String> ids = new HashSet<String>();
    for (Record record : batch.getAllRecords()) {
      String id = record.getValue(idField, String.class);
      ids.add(id);
      batchMaker.addRecord(record);
    }
    meter.mark(ids.size());
  }

}


