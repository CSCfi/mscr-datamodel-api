package fi.vm.yti.datamodel.api.endpoint.genericapi;

import java.util.logging.Logger;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import fi.vm.yti.datamodel.api.config.EndpointServices;
import fi.vm.yti.datamodel.api.utils.GraphManager;
import fi.vm.yti.datamodel.api.utils.IDManager;
import fi.vm.yti.datamodel.api.utils.JerseyResponseManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.ws.rs.QueryParam;
 
@Path("freeID")
@Api(tags = {"Resource"}, description = "Test if ID is valid and not in use")
public class FreeID {

  @Context ServletContext context;
  EndpointServices services = new EndpointServices();
  private static final Logger logger = Logger.getLogger(FreeID.class.getName());
  
  @GET
  @Produces("application/json")
  @ApiOperation(value = "Returns true if ID is valid and not in use", notes = "ID must be valid IRI")
  @ApiResponses(value = {
      @ApiResponse(code = 200, message = "False or True response")
  })
  public Response json(@ApiParam(value = "ID", required = true) @QueryParam("id") String id) {
     
    if(IDManager.isInvalid(id)) {
        return JerseyResponseManager.sendBoolean(false);
    }
            
    return JerseyResponseManager.sendBoolean(!GraphManager.isExistingGraph(id));

}
  
}
