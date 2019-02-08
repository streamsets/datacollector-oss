package com.streamsets.pipeline.lib.jdbc;
/*
 * Copyright 2019 StreamSets Inc.
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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;


/**
 * The SchemaTableClassifier allows to split batches of records into sub-batches grouped by tuples of schema and table
 */
public class SchemaTableClassifier {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaAndTable.class);

  private boolean dynamicSchemaName;
  private ELEval schemaNameEval;
  private ELVars schemaNameVars;
  private String schemaNameExpr;

  private boolean dynamicTableName;
  private ELEval tableNameEval;
  private ELVars tableNameVars;
  private String tableNameExpr;

  /**
   * @param schemaNameExpr The schema name. It can be a literal or an EL expression to be evaluated
   * @param tableNameExpr The table name. It can be a literal or an EL expression to be evaluated
   * @param context The stage context
   */
  public SchemaTableClassifier(String schemaNameExpr, String tableNameExpr, Stage.Context context) {
    this.schemaNameExpr = schemaNameExpr;
    this.tableNameExpr = tableNameExpr;

    JdbcUtil jdbcUtil = UtilsProvider.getJdbcUtil();
    dynamicSchemaName = jdbcUtil.isElString(schemaNameExpr);
    dynamicTableName = jdbcUtil.isElString(tableNameExpr);

    if (dynamicSchemaName) {
      schemaNameVars = context.createELVars();
      schemaNameEval = context.createELEval(JdbcUtil.SCHEMA_NAME);
    }

    if (dynamicTableName) {
      tableNameVars = context.createELVars();
      tableNameEval = context.createELEval(JdbcUtil.TABLE_NAME);
    }
  }

  /**
   * Split records according to tuples (schemaName, tableName)
   *
   * @param batch batch of SDC records
   * @return batch partitions grouped by the tuples (schemaName, tableName)
   * @throws OnRecordErrorException
   */
  public Multimap<SchemaAndTable, Record> classify(Batch batch) throws OnRecordErrorException {
    Multimap<SchemaAndTable, Record> partitions = ArrayListMultimap.create();
    Iterator<Record> batchIterator = batch.getRecords();

    while (batchIterator.hasNext()) {
      Record record = batchIterator.next();
      String schemaName = schemaNameExpr;
      String tableName = tableNameExpr;

      if (dynamicSchemaName) {
        try {
          RecordEL.setRecordInContext(schemaNameVars, record);
          schemaName = schemaNameEval.eval(schemaNameVars, schemaNameExpr, String.class);
          LOG.debug("Expression '{}' is evaluated to '{}' : ", schemaNameExpr, schemaName);
        } catch (ELEvalException e) {
          LOG.error("Failed to evaluate expression '{}' : ", schemaNameExpr, e.toString(), e);
          throw new OnRecordErrorException(record, e.getErrorCode(), e.getParams());
        }
      }

      if (dynamicTableName) {
        try {
          RecordEL.setRecordInContext(tableNameVars, record);
          tableName = tableNameEval.eval(tableNameVars, tableNameExpr, String.class);
          LOG.debug("Expression '{}' is evaluated to '{}' : ", tableNameExpr, tableName);
        } catch (ELEvalException e) {
          LOG.error("Failed to evaluate expression '{}' : ", tableNameExpr, e.toString(), e);
          throw new OnRecordErrorException(record, e.getErrorCode(), e.getParams());
        }
      }

      SchemaAndTable partitionKey = new SchemaAndTable(schemaName, tableName);
      partitions.put(partitionKey, record);
    }

    return partitions;
  }
}
