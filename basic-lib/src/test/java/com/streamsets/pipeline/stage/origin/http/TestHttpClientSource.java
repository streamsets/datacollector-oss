package com.streamsets.pipeline.stage.origin.http;

import com.google.common.collect.Sets;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.glassfish.jersey.test.DeploymentContext;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.ServletDeploymentContext;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.glassfish.jersey.test.spi.TestContainerException;
import org.glassfish.jersey.test.spi.TestContainerFactory;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;


public class TestHttpClientSource extends JerseyTest {

  @Path("/stream")
  @Produces("application/json")
  public static class StreamResource {
    @GET
    public Response getStream() {
      return Response.ok(
          "{\"name\": \"adam\"}\r\n" +
          "{\"name\": \"joe\"}\r\n" +
          "{\"name\": \"sally\"}"
      ).build();
    }
  }

  @Path("/nlstream")
  @Produces("application/json")
  public static class NewlineStreamResource {
    @GET
    public Response getStream() {
      return Response.ok(
          "{\"name\": \"adam\"}\n" +
          "{\"name\": \"joe\"}\n" +
          "{\"name\": \"sally\"}"
      ).build();
    }
  }

  @Override
  protected Application configure() {
    return new ResourceConfig(
        Sets.newHashSet(
            StreamResource.class,
            NewlineStreamResource.class
        )
    );
  }

  @Override
  protected TestContainerFactory getTestContainerFactory() throws TestContainerException {
    return new GrizzlyWebTestContainerFactory();
  }

  @Override
  protected DeploymentContext configureDeployment() {
    return ServletDeploymentContext.forServlet(
        new ServletContainer(
            new ResourceConfig(
                Sets.newHashSet(
                    StreamResource.class,
                    NewlineStreamResource.class
                )
            )
        )
    ).build();
  }

  @Test
  public void testStreamingHttp() throws Exception {
    HttpClientSource origin = new HttpClientSource(
        HttpClientMode.STREAMING,
        "http://localhost:9998/stream",
        1000,
        "\r\n",
        100,
        1000,
        1000
    );
    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      Assert.assertEquals(3, parsedRecords.size());

      String[] names = { "adam", "joe", "sally" };

      for (int i = 0; i < parsedRecords.size(); i++) {
        Assert.assertTrue(checkPersonRecord(parsedRecords.get(i), names[i]));
      }
    } finally {
      runner.runDestroy();
    }

  }

  @Test
  public void testStreamingHttpWithNewlineOnly() throws Exception {
    HttpClientSource origin = new HttpClientSource(
        HttpClientMode.STREAMING,
        "http://localhost:9998/nlstream",
        1000,
        "\n",
        100,
        1000,
        1000
    );
    SourceRunner runner = new SourceRunner.Builder(HttpClientSource.class, origin)
        .addOutputLane("lane")
        .build();
    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      Assert.assertEquals(3, parsedRecords.size());

      String[] names = { "adam", "joe", "sally" };

      for (int i = 0; i < parsedRecords.size(); i++) {
        Assert.assertTrue(checkPersonRecord(parsedRecords.get(i), names[i]));
      }
    } finally {
      runner.runDestroy();
    }

  }

  private boolean checkPersonRecord(Record record, String name) {
    return record.has("/name") &&
        record.get("/name").getValueAsString().equals(name);
  }
}
