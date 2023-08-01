package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.service.NamespaceResolver;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("v2/namespace")
@Tag(name = "Namespace" )
public class NamespaceResolveController {

    private final NamespaceResolver namespaceResolver;

    public NamespaceResolveController(NamespaceResolver namespaceResolver) {
        this.namespaceResolver = namespaceResolver;
    }

    @PutMapping
    public Boolean resolveNamespace(@RequestParam String namespace, @RequestParam(required = false, defaultValue = "false") boolean force){
        return namespaceResolver.resolve(namespace, force);
    }

    @GetMapping("/resolved")
    public boolean isNamespaceAlreadyResolved(@RequestParam String namespace){
        return namespaceResolver.namespaceAlreadyResolved(namespace);
    }
}
