/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.lib.recordserialization.RecordToString;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import javax.servlet.jsp.el.ELException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class RecordWriter {

    private final Path path;
    private final RecordToString recordToString;
    private long recordCount;

    private CountingOutputStream textOutputStream;
    private Writer writer;

    private SequenceFile.Writer seqWriter;
    private String keyEL;
    private ELEvaluator elEval;
    private ELEvaluator.Variables elVars;

    private RecordWriter(Path path, RecordToString recordToString) {
        this.path = path;
        this.recordToString = recordToString;
    }

    public RecordWriter(Path path, OutputStream textOutputStream, RecordToString recordToString) {
        this(path, recordToString);
        this.textOutputStream = new CountingOutputStream(textOutputStream);
        writer = new OutputStreamWriter(this.textOutputStream);
    }

    public RecordWriter(Path path, SequenceFile.Writer seqWriter, String keyEL, RecordToString recordToString) {
        this(path, recordToString);
        this.seqWriter = seqWriter;
        this.keyEL = keyEL;
        elEval = new ELEvaluator();
        ELRecordSupport.registerRecordFunctions(elEval);
        elVars = new ELEvaluator.Variables();
    }

    public void write(Record record) throws IOException, ELException {
        if (writer != null) {
            writer.write(recordToString.toString(record));
        } else if (seqWriter != null) {
            ELRecordSupport.setRecordInContext(elVars, record);
            String key = (String) elEval.eval(elVars, keyEL);
            seqWriter.append(key, recordToString.toString(record));
        }
        recordCount++;
    }

    public void flush() throws IOException {
        if (writer != null) {
            writer.flush();
        } else if (seqWriter != null) {
            seqWriter.hflush();
        }
    }

    // due to buffering of underlying streams, the reported length may be less than the actual one up to the
    // buffer size.
    public long getLength() throws IOException {
        long length = -1;
        if (writer != null) {
            length = textOutputStream.getCount();
        } else if (seqWriter != null) {
            length = seqWriter.getLength();
        }
        return length;
    }

    public long getRecords() {
        return recordCount;
    }


    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        } else if (seqWriter != null) {
            seqWriter.close();
        }
    }

    public String toString() {
        return Utils.format("DataWriter[{}]", path);
    }

}
