package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.service.UriResolveService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("v2/resolve")
@Tag(name = "Resolve" )
public class ResolveController {

    private final UriResolveService uriResolveService;
    public ResolveController(UriResolveService uriResolveService) {
        this.uriResolveService = uriResolveService;

    }

    @Operation(summary = "Resolve content by its IRI")
    @ApiResponse(responseCode = "303", description = "Resolves given uri and redirects the request per accept header")
    @ApiResponse(responseCode = "404", description = "Resource not found")
    @Parameter(name = "iri", description = "Resource IRI")
    @GetMapping
    public ResponseEntity<String> resolve(@RequestParam String iri, @RequestHeader(value = HttpHeaders.ACCEPT) String accept) {
        return uriResolveService.resolve(iri, accept);
    }

}
