/*
 * Licensed under the European Union Public Licence (EUPL) V.1.1 
 */
package fi.vm.yti.datamodel.api.endpoint.usermanagement;

import fi.vm.yti.datamodel.api.config.EndpointServices;
import fi.vm.yti.datamodel.api.utils.JerseyJsonLDClient;

import java.util.logging.Logger;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

/**
 * REST Web Service
 *
 * @author malonen
 */
@Path("groups")
@Api(tags = {"Groups"}, description = "Edit groups")
public class Group {

	@Context ServletContext context;
        EndpointServices services = new EndpointServices();
        private static final Logger logger = Logger.getLogger(Group.class.getName());

	@GET
	@ApiOperation(value = "Get groups", notes = "")
	@ApiResponses(value = { @ApiResponse(code = 204, message = "Graph is saved"),
			@ApiResponse(code = 400, message = "Invalid graph supplied"),
			@ApiResponse(code = 404, message = "Service not found") })
	@Produces("application/ld+json")
	public Response getGroup() {
                /* TODO: Join group graphs on allow editing */
                logger.info("Getting groups!?");
		return JerseyJsonLDClient.getGraphResponseFromService("urn:csc:groups",services.getCoreReadAddress());
	}

        /*

	@PUT
	@ApiOperation(value = "Add new group", notes = "PUT Body should be json-ld")
	@ApiResponses(value = { @ApiResponse(code = 204, message = "New group is created"),
			@ApiResponse(code = 400, message = "Invalid graph supplied"),
			@ApiResponse(code = 404, message = "Service not found") })
	public Response addNewGroup(
           @ApiParam(value = "Group", required = true) String groupID,
           @ApiParam(value = "New group in application/ld+json", required = true) String body,
           @Context HttpServletRequest request) {
           
            IRI groupIRI;
       
            try {
                groupIRI = IDManager.constructIRI(groupID);
            } catch (IRIException e) {
                logger.log(Level.WARNING, "GROUP ID is invalid IRI!");
                return JerseyResponseManager.invalidIRI();
            } 
            
            if(GraphManager.isExistingGraph(groupIRI)) {
                return Response.status(401).entity(ErrorMessage.USEDIRI).build();
            }
            
            HttpSession session = request.getSession();

            if(session==null) return JerseyResponseManager.unauthorized();

            LoginSession login = new LoginSession(session);

            if(!login.isLoggedIn() || !login.isAdminOfGroup(groupID)) {
                return JerseyResponseManager.unauthorized();
            }

		try {

                    ClientResponse response = JerseyFusekiClient.putGraphToTheService(groupID, body, services.getCoreReadWriteAddress());

                    if (response.getStatusInfo().getFamily() != Family.SUCCESSFUL) {
                        Logger.getLogger(Group.class.getName()).log(Level.WARNING,
                                        "Group was not updated! Status " + response.getStatus());
                        return Response.status(response.getStatus()).build();
                    }

                    logger.log(Level.INFO, "Group added sucessfully!");

                    return JerseyResponseManager.okNoContent();

		} catch (UniformInterfaceException | ClientHandlerException ex) {
			Logger.getLogger(Group.class.getName()).log(Level.WARNING, "Expect the unexpected!", ex);
			return Response.status(400).build();
		}


	}
        
        @POST
        @ApiOperation(value = "Add new group", notes = "PUT Body should be json-ld")
        @ApiResponses(value = { @ApiResponse(code = 204, message = "New group is created"),
                        @ApiResponse(code = 400, message = "Invalid graph supplied"),
                        @ApiResponse(code = 404, message = "Service not found") })
        public Response updateGroup(
           @ApiParam(value = "Group", required = true) String groupID,
           @ApiParam(value = "New group metadata in application/ld+json", required = true) String body,
           @Context HttpServletRequest request) {

            IRI groupIRI;

            try {
                groupIRI = IDManager.constructIRI(groupID);
            } catch (IRIException e) {
                logger.log(Level.WARNING, "GROUP ID is invalid IRI!");
                return JerseyResponseManager.invalidIRI();
            } 

            if(!GraphManager.isExistingGraph(groupIRI)) {
                return Response.status(401).entity(ErrorMessage.NOTFOUND).build();
            }

            HttpSession session = request.getSession();

            if(session==null) return JerseyResponseManager.unauthorized();

            LoginSession login = new LoginSession(session);

            if(!login.isLoggedIn() || !login.isAdminOfGroup(groupID)) {
                return JerseyResponseManager.unauthorized();
            }

            try {

                    ClientResponse response = JerseyFusekiClient.putGraphToTheService(groupID, body, services.getCoreReadWriteAddress());

                    if (response.getStatusInfo().getFamily() != Family.SUCCESSFUL) {
                        Logger.getLogger(Group.class.getName()).log(Level.WARNING,
                                        "Group was not updated! Status " + response.getStatus());
                        return Response.status(response.getStatus()).build();
                    }

                    logger.log(Level.INFO, "Group added sucessfully!");

                    return JerseyResponseManager.okNoContent();

                } catch (UniformInterfaceException | ClientHandlerException ex) {
                        Logger.getLogger(Group.class.getName()).log(Level.WARNING, "Expect the unexpected!", ex);
                        return Response.status(400).build();
                }


        }
        
        */
}
