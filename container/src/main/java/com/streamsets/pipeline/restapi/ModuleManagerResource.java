package com.streamsets.pipeline.restapi;

import com.streamsets.config.api.ConfigOption;
import com.streamsets.config.api.ConfigOptionGroup;
import com.streamsets.config.api.ConfigType;
import com.streamsets.pipeline.api.Module;
import com.streamsets.pipeline.container.ModuleInfo;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by harikiran on 10/18/14.
 */
@Path("/v1/modules")
public class ModuleManagerResource {

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllRegisteredModules() {

    //Mock implementation returns an instance of Source info and an instance of Target Info
    List<Module.Info> moduleInfo = new ArrayList<Module.Info>();

    ModuleInfo moduleInfo1 = new ModuleInfo("s", "1", "S", "si");
    ConfigOption option = new ConfigOption("location", ConfigType.TEXT, "file location", "file location", "/etc/data", true);
    List<ConfigOption> options = new ArrayList<ConfigOption>();
    options.add(option);
    ConfigOptionGroup opg = new ConfigOptionGroup("FileLocation", options);
    moduleInfo1.getConfiguration().add(opg);
    moduleInfo.add(moduleInfo1);

    moduleInfo.add(new ModuleInfo("p", "1", "P", "pi"));
    moduleInfo.add(new ModuleInfo("t", "1", "T", "ti"));

    return Response.ok().type(MediaType.APPLICATION_JSON).entity(moduleInfo).build();
  }
}
