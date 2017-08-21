/*
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
package com.streamsets.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public final class SdcClusterOffsetHelper {
  private static final Logger LOG = LoggerFactory.getLogger(SdcClusterOffsetHelper.class);
  private static final String SDC_STREAMING_OFFSET_FILE = "offset.json";
  private static final String SDC_STREAMING_BACKUP_OFFSET_FILE = "backup_offset.json";
  private static final String SDC_STREAMING_OFFSET_MARKER_FILE = "offset_marker";


  private static final String SDC_STREAMING_OFFSET_VERSION = "1";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String CANNOT_RENAME_FROM_TO_TEMPLATE = "Cannot rename from {} to {}";

  private final Path checkPointFilePath;
  private final Path backupCheckPointFilePath;
  private final Path checkPointMarkerFilePath;
  private final FileSystem fs;
  private final long duration;

  private long lastOffsetStoredTime;

  public SdcClusterOffsetHelper(Path checkPointPath, FileSystem fs, long duration) {
    this.fs = fs;
    this.duration = duration;
    this.lastOffsetStoredTime = -1;
    this.checkPointFilePath = new Path(checkPointPath, SDC_STREAMING_OFFSET_FILE);
    this.backupCheckPointFilePath = new Path(checkPointPath, SDC_STREAMING_BACKUP_OFFSET_FILE);
    this.checkPointMarkerFilePath = new Path(checkPointPath, SDC_STREAMING_OFFSET_MARKER_FILE);
    LOG.info("SDC Checkpoint File Path : {}", checkPointPath.toString());
  }

  //This tell us SDC is check pointing
  public boolean isSDCCheckPointing()  {
    try {
      return fs.exists(checkPointFilePath) || fs.exists(backupCheckPointFilePath);
    } catch (IOException ex) {
      LOG.error("Error doing isSDCCheckPointing", ex);
      throw new RuntimeException(Utils.format("Error checking exists on hdfs path: {}. Reason: {}", checkPointFilePath.toString(), ex.toString()), ex);
    }
  }

  //Tells us whether checkpoint can be done. Only checkpoint at the right time interval
  //Using a separate thread for this may be an overkill at this point
  //Meaning we will checkpoint after the current batch is read
  //and the time interval from last checkpoint time to current time is greater than or equal to duration configured.
  private boolean canCheckPoint() {
    long currentTime = System.currentTimeMillis();
    return lastOffsetStoredTime < 0 || duration < 0 || ((currentTime - lastOffsetStoredTime) >= duration);
  }

  //Write the offsets in the main offset file
  //This should be called after backing up the existing contents if the main offset file is not corrupted
  //Or if the file is corrupted, we want to update the right offsets to the main offset file.
  private void writeOffsetsToMainOffsetFile(Map<Integer, Long> partitionToOffsetMap) throws IOException {
    LOG.info("Saving the following offset {} to {}", partitionToOffsetMap, checkPointFilePath);

    //Creating a marker file (overwriting if it already exists) to mark that we are going to write offsets out the offsets to the main offset file.
    try(OutputStream os = fs.create(checkPointMarkerFilePath, true)) {
      //NOOP
    }
    //If the both above passes and writing fails or leaves corrupted file we will have the back file
    try (OutputStream os = fs.create(checkPointFilePath, true)) {
      OBJECT_MAPPER.writeValue(os, new ClusterSourceOffsetJson(serializeKafkaPartitionOffset(partitionToOffsetMap), SDC_STREAMING_OFFSET_VERSION));
    }

    //If this fails we are still good, as we will start from the backup offset file. (Not optimal, but deterministic)
    boolean deleted = fs.delete(checkPointMarkerFilePath, false);
    LOG.warn("Status {} for Deleting Marker File {}", deleted, checkPointMarkerFilePath);

    //If the write fails we don't want to touch the timestamp and will error out so not doing this in finally
    lastOffsetStoredTime = System.currentTimeMillis();
  }

  public void saveOffsets(Map<Integer, Long> partitionToOffsetMap) {
    if (canCheckPoint()) {
      try {
        //Only if marker file does not exist meaning the current offset file is not corrupted or this is the first run
        //where there are no offset files, we should try the below steps
        if (!fs.exists(checkPointMarkerFilePath)) {
          //Delete the backup file only if it exists
          if (fs.exists(backupCheckPointFilePath)) {
            LOG.info(
                "Deleting the Backup Offset file {} before renaming Main Offset File {} to Backup Offset File {}",
                backupCheckPointFilePath,
                checkPointFilePath,
                backupCheckPointFilePath
            );
            //If this fails we will still have the main offset file.
            boolean deleted = fs.delete(backupCheckPointFilePath, false);
            LOG.warn("Status {} for Deleting Backup Offset File {}", deleted, backupCheckPointFilePath);
          }
          //If the main offset file does not exist can't backup (The first offset save will not contain both offset files)
          if (fs.exists(checkPointFilePath)) {
            LOG.info("Renaming Main Offset File {} to Backup Offset File {}", checkPointFilePath, backupCheckPointFilePath);
            //If this fails we will still have the main offset file
            if (!fs.rename(checkPointFilePath, backupCheckPointFilePath)) {
              throw new IOException(Utils.format(CANNOT_RENAME_FROM_TO_TEMPLATE, checkPointFilePath, backupCheckPointFilePath));
            }
          }
        }
        writeOffsetsToMainOffsetFile(partitionToOffsetMap);
      } catch (IOException ex) {
        LOG.error("Error when serializing partition offset", ex);
        throw new RuntimeException(Utils.format("Error writing offset To to hdfs path: {}. Reason: {}", checkPointFilePath.toString(), ex.toString()), ex);
      }
    }
  }

  //Most Wrong information in offset file will throw IOException. (Illegal State Exception if less number of kafka partitions for topic than in offset)
  //Ex: Partition Offset empty, empty map in the offset, cannot deserialize
  private Map<Integer, Long> readClusterOffsetFile(Path checkPointFilePath, int numberOfPartitions) throws IOException{
    if (!fs.exists(checkPointFilePath)) {
      throw new IOException(Utils.format("Checkpoint file path {} does not exist", checkPointFilePath));
    }
    ClusterSourceOffsetJson clusterSourceOffsetJson = OBJECT_MAPPER.readValue(
        (InputStream) fs.open(checkPointFilePath),
        ClusterSourceOffsetJson.class
    );
    String lastSourceOffset = clusterSourceOffsetJson.getOffset();
    if (!StringUtils.isEmpty(lastSourceOffset)) {
      return deserializeKafkaPartitionOffset(lastSourceOffset, numberOfPartitions);
    } else {
      throw new IOException("Partition Offset Cannot be empty");
    }
  }

  @SuppressWarnings("unchecked")
  public Map<Integer, Long> readOffsets(int numberOfPartitions)  {
    Map<Integer, Long> offsets;
    Path currentCheckPointFilePath = this.checkPointFilePath;
    try {
      if (fs.exists(checkPointMarkerFilePath)) {
        //Try the backup offset file as marker file exists meaning the main offset file is probably corrupted
        //Set the currentCheckPointPath to backup file
        currentCheckPointFilePath = this.backupCheckPointFilePath;

        //Force to read the backup file.
        LOG.info(Utils.format("Checkpoint marker file present {}, which means the main offset file {} is corrupted", checkPointMarkerFilePath, checkPointFilePath));
        LOG.info("Trying the backup offset file {}", backupCheckPointFilePath);

        offsets = readClusterOffsetFile(currentCheckPointFilePath, numberOfPartitions);

        LOG.info("Updating the probably corrupted Main Offset File {} to the offsets from backup file {}", checkPointFilePath, backupCheckPointFilePath);
        //Both main and backup offset file will have the same content
        writeOffsetsToMainOffsetFile(offsets);
      } else {
        //Try the main offset file
        offsets = readClusterOffsetFile(currentCheckPointFilePath, numberOfPartitions);
      }
      return offsets;
    } catch (IOException ex) {
      LOG.error("Error when deserializing partition offset from Check Point Path : {}. Reason : {}", currentCheckPointFilePath, ex);
      throw new RuntimeException(Utils.format("Error reading offset from hdfs path: {}. Reason: {}", currentCheckPointFilePath.toString(), ex.toString()), ex);
    }
  }

  private String serializeKafkaPartitionOffset(Map<Integer, Long> partitionsToOffset) throws IOException {
    return OBJECT_MAPPER.writeValueAsString(partitionsToOffset);
  }

  @SuppressWarnings("unchecked")
  private Map<Integer, Long> deserializeKafkaPartitionOffset(String partitionOffset, int numberOfPartitions) throws IOException {
    Map<Integer, Long> partitionToOffsetMap = new HashMap<>();
    int greatestPartitionFromOffset = -1;

    if (!StringUtils.isEmpty(partitionOffset)) {
      Map<String, Object> deserializedPartitionOffset = OBJECT_MAPPER.readValue(partitionOffset, Map.class);
      if (deserializedPartitionOffset.isEmpty()) {
        throw new IOException("Partition Offset cannot be empty");
      }
      //Basically could happen when topic is deleted and recreated with less number of partitions
      //Users should either delete the sdc checkpoint folder
      //or use a new consumer group.
      Utils.checkState(
          deserializedPartitionOffset.size() <= numberOfPartitions,
          "More number of partitions found in the offset than the number of partitions for the topic." +
              " The topic may have been deleted and recreated with less partitions," +
              " please use a new consumer group or delete the checkpoint directory"
      );
      for (Map.Entry<String, Object> partitionOffsetEntry : deserializedPartitionOffset.entrySet()) {
        int partition = Integer.parseInt(partitionOffsetEntry.getKey());
        Long offset = Long.parseLong(partitionOffsetEntry.getValue().toString());
        partitionToOffsetMap.put(partition, offset);
        greatestPartitionFromOffset = (partition > greatestPartitionFromOffset) ? partition : greatestPartitionFromOffset;
      }
    }

    //Basically add new partitions with offset 0.
    for (int partition = greatestPartitionFromOffset + 1; partition < numberOfPartitions; partition++) {
      partitionToOffsetMap.put(partition, 0L);
    }

    LOG.info("Starting offsets: {}", partitionToOffsetMap);
    return partitionToOffsetMap;
  }
}
