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
import com.streamsets.pipeline.api.base.BaseSource;
import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

@StageDef(name = "PdfLineProducer", version = "1.0", label = "pdf_line_producer", description = "Produces lines from a PDF file")
public class PDFLineProducer extends BaseSource {

  private static final String MODULE = "PdfLineProducer";

  @ConfigDef(defaultValue = "", label = "pdf_location", description = "Absolute file name of the PDF",
    name = "pdfLocation", required = true, type = ConfigDef.Type.STRING)
  public String pdfLocation;

  public PDFLineProducer(String pdfLocation) {
    this.pdfLocation = pdfLocation;
  }

  public PDFLineProducer() {

  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String rideHistory = pdftoText(pdfLocation);
    String[] transactions = rideHistory.split(System.lineSeparator());

    for(int i = 0; i < transactions.length; i++) {
      Record r = getContext().createRecord("LineNumber[" + i + "]");
      r.setField("transactionLog", Field.create(Field.Type.STRING, transactions[i]));
      batchMaker.addRecord(r, "lane");
    }
    //In the first cut each line is a record and one batch has all records.
    //No more records
    return null;
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
