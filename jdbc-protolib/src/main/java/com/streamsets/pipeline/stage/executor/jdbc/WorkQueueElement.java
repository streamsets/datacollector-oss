package com.streamsets.pipeline.stage.executor.jdbc;

import com.streamsets.pipeline.api.Record;

class WorkQueueElement {
  final String query;
  final Record record;

  WorkQueueElement(String query, Record record) {
    this.query = query;
    this.record = record;
  }
}
