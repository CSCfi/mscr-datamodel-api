/*
 * Licensed under the European Union Public Licence (EUPL) V.1.1 
 */
package fi.vm.yti.datamodel.api.endpoint.model;

import java.util.logging.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import fi.vm.yti.datamodel.api.config.EndpointServices;
import fi.vm.yti.datamodel.api.utils.JerseyResponseManager;
import fi.vm.yti.datamodel.api.utils.GraphManager;
import fi.vm.yti.datamodel.api.utils.IDManager;
import fi.vm.yti.datamodel.api.utils.JerseyClient;
import fi.vm.yti.datamodel.api.utils.LDHelper;
import fi.vm.yti.datamodel.api.utils.NamespaceResolver;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.rdf.model.ResourceFactory;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.jena.iri.IRI;
import org.apache.jena.iri.IRIException;

/**
 * Root resource (exposed at "modelCreator" path)
 */
@Path("modelRequirementCreator")
@Api(tags = {"Model"}, description = "Construct new requirement")
public class ModeRequirementlCreator {

    EndpointServices services = new EndpointServices();
    private static final Logger logger = Logger.getLogger(ModeRequirementlCreator.class.getName());
    
    @GET
    @Produces("application/ld+json")
    @ApiOperation(value = "Create new model", notes = "Create namespace object. Namespace must be valid URI and end with # or /.")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "New class is created"),
                    @ApiResponse(code = 400, message = "Invalid ID supplied"),
                    @ApiResponse(code = 403, message = "Invalid IRI in parameter"),
                    @ApiResponse(code = 404, message = "Service not found"),
                    @ApiResponse(code = 401, message = "No right to create new")})
    public Response newRequiredModel(
            @ApiParam(value = "Model namespace", required = true) @QueryParam("namespace") String namespace,
            @ApiParam(value = "Model prefix", required = true) @QueryParam("prefix") String prefix,
            @ApiParam(value = "Model label", required = true) @QueryParam("label") String label,
            @ApiParam(value = "Initial language", required = true, allowableValues="fi,en") @QueryParam("lang") String lang) {

            if(namespace==null || (!namespace.endsWith("#") && !namespace.endsWith("/"))) return JerseyResponseManager.invalidIRI();

            IRI namespaceIRI;

            try {
                    namespaceIRI = IDManager.constructIRI(namespace);
            } catch (IRIException e) {
                    return JerseyResponseManager.invalidIRI();
            }
            
            boolean isResolvedNamespace = true;
            boolean isLocalNamespace = true;
            
            if(!GraphManager.isExistingServiceGraph(namespace)) {
               isResolvedNamespace = NamespaceResolver.resolveNamespace(namespace,null,false);
               isLocalNamespace = false;
            }

            String queryString;
            ParameterizedSparqlString pss = new ParameterizedSparqlString();
            pss.setNsPrefixes(LDHelper.PREFIX_MAP);
            queryString = "CONSTRUCT  { "
                    + "?g a rdfs:Resource . "
                    + "?g rdfs:label ?label . "
                    + "?g dcap:preferredXMLNamespaceName ?namespace . "
                    + "?g dcap:preferredXMLNamespacePrefix ?prefix . "
                    + (isLocalNamespace?"":"?g iow:isResolved ?resolved . ")
                    + "} WHERE { }";

            pss.setCommandText(queryString);
            pss.setIri("g", namespaceIRI);
            pss.setLiteral("label", ResourceFactory.createLangLiteral(label, lang));
            pss.setLiteral("namespace",namespace);
            pss.setLiteral("prefix", prefix);
            if(!isLocalNamespace) pss.setLiteral("resolved", isResolvedNamespace);

            return JerseyClient.constructNonEmptyGraphFromService(pss.toString(), services.getTempConceptReadSparqlAddress());

        }
}
