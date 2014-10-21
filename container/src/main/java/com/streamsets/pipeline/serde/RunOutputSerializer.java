package com.streamsets.pipeline.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.container.PipelineBatchRunOutput;
import com.streamsets.pipeline.container.RunOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by harikiran on 10/17/14.
 *
 * Serializes the RunOutput object into Json.
 * As of now the only type of RunOutput is PipeLineBatchRunOutput.
 *
 * This is a mock implementation to enable interaction with the UI.
 */
@Produces(MediaType.APPLICATION_JSON)
public class RunOutputSerializer extends JsonSerializer<RunOutput> {

  private static Logger LOG = LoggerFactory.getLogger(RunOutputSerializer.class);

  @Override
  public void serialize(RunOutput runOutput
    , JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
    throws IOException {

    LOG.debug("Using Serializer : " + getClass().getName());
    jsonGenerator.setPrettyPrinter(new DefaultPrettyPrinter());

    if(runOutput instanceof PipelineBatchRunOutput) {
      jsonGenerator.writeStartObject(); //start RunOutput object
      jsonGenerator.writeStringField("batchId", runOutput.getBatchId());
      jsonGenerator.writeArrayFieldStart("modules"); //start modules array

      //write each module in the pipeline and the records in each lane
      for(RunOutput.ModuleOutput m : runOutput.getOutput()) {
        jsonGenerator.writeStartObject(); //start module
        //write moduleInfo
        jsonGenerator.writeStringField("name", m.getModuleInfo().getName());
        jsonGenerator.writeStringField("description", m.getModuleInfo().getDescription());
        jsonGenerator.writeStringField("instanceName", m.getModuleInfo().getInstanceName());
        jsonGenerator.writeStringField("version", m.getModuleInfo().getVersion());

        //write map of lanes vs records for t
        Map<String, List<Record>> recordMap = m.getOutput();

        jsonGenerator.writeArrayFieldStart("output"); //start array output lanes
        for(Map.Entry<String, List<Record>> entry :recordMap.entrySet()) {
          jsonGenerator.writeStartObject();
          jsonGenerator.writeStringField("lane", entry.getKey());
          jsonGenerator.writeArrayFieldStart("records"); //start array of records

          for(Record r : entry.getValue()) {
            jsonGenerator.writeStartObject(); //start record
            jsonGenerator.writeStringField("value", r.toString());
            jsonGenerator.writeEndObject(); // end record
          }
          jsonGenerator.writeEndArray(); //end array records
          jsonGenerator.writeEndObject();
        }
        jsonGenerator.writeEndArray();
        jsonGenerator.writeEndObject(); //end module
      }
      jsonGenerator.writeEndArray(); //end modules array
      jsonGenerator.writeEndObject(); //end RunOutput object
    }
  }
}
