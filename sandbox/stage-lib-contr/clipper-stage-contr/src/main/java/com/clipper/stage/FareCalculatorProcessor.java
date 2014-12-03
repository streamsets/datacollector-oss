/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.clipper.stage;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BaseProcessor;

import java.util.*;

@StageDef(description = "FareCalculatorProcessor inspects the transaction log, extracts the fares and adds new fields to the record - one for the fare and cone for cumulative sum",
  label = "Clipper Fare Calculator",
  version = "1.0")
public class FareCalculatorProcessor extends BaseProcessor{

  private double caltrainClaim = 0.0;
  private double bartClaim = 0.0;
  private double muniClaim = 0.0;
  private double vtaClaim = 0.0;
  private Set<String> caltrainDates = new HashSet<String>();
  private Set<String> bartDates = new HashSet<String>();
  private Set<String> muniDates = new HashSet<String>();
  private Set<String> vtaDates = new HashSet<String>();

  private String[] lanes;

  @Override
  protected void init() throws StageException {
    lanes = getContext().getOutputLanes().toArray(
      new String[getContext().getOutputLanes().size()]);
  }

  @Override
  public void process(Batch batch, BatchMaker batchMaker) throws StageException {
    Iterator<Record> it = batch.getRecords();
    while (it.hasNext()) {
      Record record = it.next();

      Map<String, Field> fieldMap = record.get().getValueAsMap();

      Field f = fieldMap.get("transactionLog");
      String transactionLog = (String)f.getValue();
      double transactionFare = 0.0;

      if(transactionLog.contains("Dual-tag entry transaction, maximum fare deducted")
        || transactionLog.contains("Single-tag fare payment")
        || transactionLog.contains("Dual-tag exit transaction, fare payment")) {
        transactionFare = extractFareFromLine(transactionLog);
        if(transactionLog.contains("BART")) {
          bartClaim += transactionFare;
          String date = extractDateFromLine(transactionLog);
          bartDates.add(date);
          fieldMap.put("Travel Date, Mode and Fare", Field.create(Field.Type.STRING, date + ", " + "BART" + ", " + String.valueOf(transactionFare)));
        } else if (transactionLog.contains("Muni")) {
          muniClaim += transactionFare;
          String date = extractDateFromLine(transactionLog);
          muniDates.add(date);
          fieldMap.put("Travel Date, Mode and Fare", Field.create(Field.Type.STRING, date + ", " + "MUNI" + ", " + String.valueOf(transactionFare)));
        } else if (transactionLog.contains("VTA")) {
          vtaClaim += transactionFare;
          String date = extractDateFromLine(transactionLog);
          vtaDates.add(date);
          fieldMap.put("Travel Date, Mode and Fare", Field.create(Field.Type.STRING, date + ", " + "LIGHT RAIL" + ", " + String.valueOf(transactionFare)));
        } else {
          caltrainClaim += transactionFare;
          String date = extractDateFromLine(transactionLog);
          caltrainDates.add(date);
          fieldMap.put("Travel Date, Mode and Fare", Field.create(Field.Type.STRING, date + " ," + "CALTRAIN" + ", " + String.valueOf(transactionFare)));
        }
      } else if (transactionLog.contains("Dual-tag exit transaction, fare adjustment")) {
        transactionFare = extractFareFromLine(transactionLog);
        caltrainClaim -= transactionFare;
      }

      fieldMap.put("All Bart travel dates", Field.create(Field.Type.STRING, getTravelDates(bartDates)));
      fieldMap.put("Total Bart travel fare", Field.create(Field.Type.DOUBLE, bartClaim));
      fieldMap.put("All Caltrain travel dates", Field.create(Field.Type.STRING, getTravelDates(caltrainDates)));
      fieldMap.put("Total Caltrain travel fare", Field.create(Field.Type.DOUBLE, caltrainClaim));
      fieldMap.put("All Light rail travel dates", Field.create(Field.Type.STRING, getTravelDates(vtaDates)));
      fieldMap.put("Total Light Rail travel fare", Field.create(Field.Type.DOUBLE, vtaClaim));
      fieldMap.put("All Muni travel dates", Field.create(Field.Type.STRING, getTravelDates(muniDates)));
      fieldMap.put("Total Muni travel fare", Field.create(Field.Type.DOUBLE, muniClaim));

      fieldMap.put("Total Travel Expenditure As of this travel", Field.create(Field.Type.DOUBLE,
        caltrainClaim + bartClaim + muniClaim + vtaClaim));

      batchMaker.addRecord(record, lanes[0]);
    }
  }

  private String getTravelDates(Set<String> bartDates) {
    StringBuilder sb = new StringBuilder();
    for(String d : bartDates) {
      sb.append(d).append(", ");
    }
    if(sb.length() > 1) {
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  private String extractDateFromLine(String line) {
    String[] tokens = line.split(" ");
    return new String(tokens[0]);
  }

  private Double extractFareFromLine(String line) {
    //Fare is the last but one word in the line which is empty space separated
    String[] tokens = line.split(" ");
    int length = tokens.length;
    String fare = tokens[length -2];
    try {
      return Double.parseDouble(fare);
    } catch (NumberFormatException e) {
      //Double tagging may result in this. There is an entry but no deduction
      return 0.0;
    }
  }
}
