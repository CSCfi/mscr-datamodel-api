package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import org.apache.jena.iri.IRI;
import org.apache.jena.iri.IRIFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import org.topbraid.shacl.vocabulary.SH;

@Service
public class UriResolveService {

    private final Logger logger = LoggerFactory.getLogger(UriResolveService.class);
    private static final IRIFactory iriFactory = IRIFactory.iriImplementation();
    private final CoreRepository coreRepository;

    public UriResolveService(CoreRepository coreRepository) {
        this.coreRepository = coreRepository;
    }

    public ResponseEntity<String> resolve(String iri, String accept) {
        logger.info("Resolve resource {}, accept: {}", iri, accept);

        if (!checkIRI(iriFactory.create(iri))) {
            return ResponseEntity.badRequest().build();
        }
        var parts = iri.split(":");

        if (parts.length == 0 || parts.length != 3) {
            return ResponseEntity.badRequest().build();
        }

        var contentType = parts[1];
        var currentUrl = ServletUriComponentsBuilder.fromCurrentRequestUri().build().toUri();
        var redirectURL = new StringBuilder();
        redirectURL.append(currentUrl.getScheme())
                .append("://")
                .append(currentUrl.getHost())
                .append(currentUrl.getHost().equals("localhost") ? ":3000" : "");

        if (accept == null || (accept != null && accept.contains(MimeTypeUtils.TEXT_HTML_VALUE))) {
            // redirect to the site
            redirectURL
                    .append("/")
                    .append(contentType)
                    .append("/")
                    .append(iri);
        } else {
            // redirect to serialized resource
        	if(iri.indexOf("@mapping") > 0) {
                redirectURL
                .append("/datamodel-api/v2/")
                .append(contentType)
                .append("/")
                .append(iri.substring(0,iri.indexOf("@")))
                .append("/")
                .append("mapping");
        		
        	}
        	else {
                redirectURL
                .append("/datamodel-api/v2/")
                .append(contentType)
                .append("/")
                .append(iri);
        		
        	}
        }
        return ResponseEntity
                .status(HttpStatus.SEE_OTHER)
                .header(HttpHeaders.LOCATION, redirectURL.toString())
                .build();
    }

    private static boolean checkIRI(IRI iri) {
        return iri.toString().startsWith("mscr:");
    }
}
