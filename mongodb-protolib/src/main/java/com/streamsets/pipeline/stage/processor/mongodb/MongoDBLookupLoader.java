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
package com.streamsets.pipeline.stage.processor.mongodb;

import com.google.common.cache.CacheLoader;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.stage.common.mongodb.Errors;
import com.streamsets.pipeline.stage.common.mongodb.MongoDBUtil;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MongoDBLookupLoader extends CacheLoader<Document, Optional<List<Map<String, Field>>>> {

  private MongoCollection<Document> mongoCollection;
  private static final Logger LOG = LoggerFactory.getLogger(MongoDBLookupLoader.class);


  public MongoDBLookupLoader(MongoCollection<Document> mongoCollection) {
    this.mongoCollection = mongoCollection;
  }

  @Override
  public Optional<List<Map<String, Field>>> load(Document key) throws Exception {
    return lookupValuesForRecord(key);
  }

  private Optional<List<Map<String, Field>>> lookupValuesForRecord(Document doc) throws StageException {
    List<Map<String, Field>> lookupItems = new ArrayList<>();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Going to lookup with:" + doc.toJson());
    }

    FindIterable<Document> it = mongoCollection.find(doc);
    if (it.first() != null) {
      MongoCursor<Document> ite = it.iterator();
      while (ite.hasNext()) {
        Document result = ite.next();
        if (LOG.isTraceEnabled()) {
          LOG.trace("Found document:" + result.toJson());
        }
        try {
          Map<String, Field> fields = MongoDBUtil.createFieldFromDocument(result);
          lookupItems.add(fields);
        } catch (IOException io) {
          LOG.error(Errors.MONGODB_06.getMessage(), mongoCollection, result.toJson());
          throw new OnRecordErrorException(Errors.MONGODB_10, result.toJson());
        }
      }
    } else {
      // no lookup result.
      return Optional.empty();
    }
    return Optional.of(lookupItems);
  }
}
