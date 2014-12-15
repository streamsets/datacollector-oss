package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of BatchMaker interface for SDK
 */
class BatchMakerImpl implements BatchMaker {

  private final Set<String> outputLanes;
  private final Map<String, List<Record>> laneToRecordsMap;
  private final String singleLaneOutput;

  public BatchMakerImpl(Set<String> outputLanes) {
    this.outputLanes = outputLanes;
    if(outputLanes.size() == 1) {
      singleLaneOutput = outputLanes.iterator().next();
    } else {
      singleLaneOutput = null;
    }
    laneToRecordsMap = new HashMap<>();
    //The output map should always have a key for all the defined output lanes, if the stage did not produce any record
    // for a lane, the value in the map should be an empty record list.
    for(String lane : outputLanes) {
      laneToRecordsMap.put(lane, new ArrayList<Record>());
    }
  }

  @Override
  public Set<String> getLanes() {
    return outputLanes;
  }

  @Override
  public void addRecord(Record record, String... lanes) {
    if(lanes == null || lanes.length == 0) {
      List<Record> records = laneToRecordsMap.get(singleLaneOutput);
      if(records == null) {
        records = new ArrayList<>();
        laneToRecordsMap.put(singleLaneOutput, records);
      }
      records.add(record);
      return;
    }
    for(String lane : lanes) {
      List<Record> records = laneToRecordsMap.get(lane);
      if(records == null) {
        records = new ArrayList<>();
        laneToRecordsMap.put(lane, records);
      }
      records.add(record);
    }
  }

  public Map<String, List<Record>> getOutput() {
    return laneToRecordsMap;
  }
}