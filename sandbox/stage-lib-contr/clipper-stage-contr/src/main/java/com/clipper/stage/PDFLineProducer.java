/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.clipper.stage;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

@GenerateResourceBundle
@RawSource(rawSourcePreviewer = ClipperSourcePreviewer.class)
@StageDef(version = "1.0", label = "Clipper Travel Log Producer", description = "Produces lines from a PDF file")
public class PDFLineProducer extends BaseSource {

  private static final String MODULE = "PdfLineProducer";

  @ConfigDef(defaultValue = "", label = "PDF Location", description = "Absolute file name of the PDF",
    required = true, type = ConfigDef.Type.STRING)
  public String pdfLocation;

  private String[] lanes;
  private String[] transactions = null;
  private int linesPendingRead = 0;
  //Skip first 5 lines of clipper pdf
  private int index = 5;


  public PDFLineProducer(String pdfLocation) {
    this.pdfLocation = pdfLocation;
  }

  public PDFLineProducer() {

  }

  @Override
  protected void init() throws StageException {
    lanes = getContext().getOutputLanes().toArray(
      new String[getContext().getOutputLanes().size()]);
    transactions = pdftoText(pdfLocation).split(System.lineSeparator());
    linesPendingRead = transactions.length;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    maxBatchSize = (maxBatchSize > -1) ? maxBatchSize : 10;
    int batchSize = Math.min(maxBatchSize, linesPendingRead);

    //calculate the line number in pdf based on the source offset
    try {
      index = Integer.parseInt(lastSourceOffset);
    } catch (NumberFormatException e) {
      index = 0;
    }

    for(int i = 0; i < batchSize; i++) {
      Record r = getContext().createRecord("LineNumber[" + i + "]");
      r.set(Field.create(Field.Type.STRING, transactions[i + index]));
      batchMaker.addRecord(r, lanes[0]);
    }
    //update the line number after reading
    index += batchSize;
    //update pending lines
    linesPendingRead -= batchSize;

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    if(linesPendingRead > 0) {
      return String.valueOf(index);
    } else {
      linesPendingRead = transactions.length;
      return String.valueOf(0);
    }
  }

  private String pdftoText(String inputFileName) {
    PDFParser parser;
    String parsedText = null;;
    PDFTextStripper pdfStripper = null;
    PDDocument pdDoc = null;
    COSDocument cosDoc = null;
    File file = new File(inputFileName);
    if (!file.isFile()) {
      System.err.println("File " + inputFileName + " does not exist.");
      return null;
    }
    try {
      parser = new PDFParser(new FileInputStream(file));
    } catch (IOException e) {
      System.err.println("Unable to open PDF Parser. " + e.getMessage());
      return null;
    }
    try {
      parser.parse();
      cosDoc = parser.getDocument();
      pdfStripper = new PDFTextStripper();
      pdDoc = new PDDocument(cosDoc);
      pdfStripper.setStartPage(1);
      pdfStripper.setEndPage(5);
      parsedText = pdfStripper.getText(pdDoc);
    } catch (Exception e) {
      System.err
        .println("An exception occured in parsing the PDF Document."
          + e.getMessage());
    } finally {
      try {
        if (cosDoc != null)
          cosDoc.close();
        if (pdDoc != null)
          pdDoc.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return parsedText;
  }
}
