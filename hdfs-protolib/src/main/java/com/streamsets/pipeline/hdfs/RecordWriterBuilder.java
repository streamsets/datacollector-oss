/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.recordserialization.CsvRecordToString;
import com.streamsets.pipeline.lib.recordserialization.JsonRecordToString;
import com.streamsets.pipeline.lib.recordserialization.RecordToString;
import com.streamsets.pipeline.lib.recordserialization.TsvRecordToString;
import com.streamsets.pipeline.lib.recordserialization.XmlRecordToString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class RecordWriterBuilder {
  private URI fsUri;
  private Configuration hdfsConfiguration;
  private HdfsFileType fileType;
  private CompressionCodec compressionCodec;
  private SequenceFile.CompressionType compressionType;
  private String keyEL;
  private RecordToString recordToString;

  public RecordWriterBuilder(URI fsUri, Configuration hdfsConfiguration) {
    Utils.checkNotNull(fsUri, "fsUri");
    Utils.checkNotNull(hdfsConfiguration, "hdfsConfiguration");
    this.fsUri = fsUri;
    this.hdfsConfiguration = hdfsConfiguration;
  }

  public RecordWriterBuilder setFileType(HdfsFileType fileType) {
    this.fileType = fileType;
    return this;
  }

  public RecordWriterBuilder setCompressionCodec(CompressionCodec codec) {
    this.compressionCodec = codec;
    return this;
  }

  public RecordWriterBuilder setCompressionType(SequenceFile.CompressionType type) {
    this.compressionType = type;
    return this;
  }

  public RecordWriterBuilder setKeyEL(String keyEL) {
    this.keyEL = keyEL;
    return this;
  }

  public RecordWriterBuilder setRecordDataFormat(HdfsDataFormat dataFormat) {
    this.recordToString = getRecordToString(dataFormat);
    return this;
  }

  private static Path getFinalPath(Path path, CompressionCodec codec) {
    if (codec != null) {
      path = new Path(path.toString() + codec.getDefaultExtension()); //getDefaultExtension() includes the dot
    }
    return path;
  }

  private RecordToString getRecordToString(HdfsDataFormat dataFormat) {
    switch (dataFormat) {
      case CSV:
        return new CsvRecordToString();
      case JSON:
        return new JsonRecordToString();
      case TSV:
        return new TsvRecordToString();
      case XML:
        return new XmlRecordToString();
      default:
        throw new IllegalArgumentException(Utils.format("Unsupported data format '{}'", dataFormat));
    }
  }

  // must be used within a UGI.doAs() call
  public RecordWriter build(Path path) throws IOException {
    Utils.checkNotNull(fileType, "fileType");
    Utils.checkNotNull(path, "path");
    Utils.checkNotNull(recordToString, "recordToString");
    FileSystem fs = FileSystem.get(fsUri, hdfsConfiguration);
    switch (fileType) {
      case TEXT:
        path = getFinalPath(path, compressionCodec);
        OutputStream os = fs.create(path, false);
        if (compressionCodec != null) {
          os = compressionCodec.createOutputStream(os);
        }
        return new RecordWriter(path, os, recordToString);
      case SEQUENCE_FILE:
        Utils.checkNotNull(compressionType, "compressionType");
        Utils.checkNotNull(keyEL, "keyEL");
        Utils.checkArgument(compressionType != SequenceFile.CompressionType.NONE && compressionCodec != null,
                            "if compressionType is different than NONE, compressionCodec cannot be NULL");
        path = getFinalPath(path, compressionCodec);
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, hdfsConfiguration, path, String.class, String.class,
                                                               compressionType, compressionCodec);
        return new RecordWriter(path, writer, keyEL, recordToString);
      default:
        throw new UnsupportedOperationException(Utils.format("Unsupported file Type '{}'", fileType));
    }
  }

}
