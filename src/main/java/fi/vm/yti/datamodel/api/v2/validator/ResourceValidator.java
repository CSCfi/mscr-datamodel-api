package fi.vm.yti.datamodel.api.v2.validator;

import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.ResourceDTO;
import fi.vm.yti.datamodel.api.v2.dto.ResourceType;
import fi.vm.yti.datamodel.api.v2.repository.ImportsRepository;
import fi.vm.yti.datamodel.api.v2.service.ResourceService;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDFS;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;

public class ResourceValidator extends BaseValidator implements ConstraintValidator<ValidResource, ResourceDTO> {

    private boolean updateProperty;

    private ResourceType resourceType;

    @Autowired
    private ResourceService resourceService;
    @Autowired
    private ImportsRepository importsRepository;

    
    @Value("${defaultNamespace}")
    private String defaultNamespace;
    
    @Override
    public void initialize(ValidResource constraintAnnotation) {
        this.updateProperty = constraintAnnotation.updateProperty();
        this.resourceType = constraintAnnotation.resourceType();
    }

    @Override
    public boolean isValid(ResourceDTO value, ConstraintValidatorContext context) {
        setConstraintViolationAdded(false);

        checkLabel(context, value);
        checkEditorialNote(context, value);
        checkStatus(context, value.getStatus());
        checkNote(context, value);
        checkEquivalentProperty(context, value);
        checkSubPropertyOf(context, value);
        checkPrefixOrIdentifier(context, value.getIdentifier(), "identifier", ValidationConstants.RESOURCE_IDENTIFIER_MAX_LENGTH, updateProperty);
        checkDomain(context, value);
        checkRange(context, value);

        return !isConstraintViolationAdded();
    }

    private void checkEquivalentProperty(ConstraintValidatorContext context, ResourceDTO resourceDTO){
        var equivalentResource = resourceDTO.getEquivalentResource();
        if(equivalentResource != null){
            equivalentResource.forEach(eq -> {
                var asUri = NodeFactory.createURI(eq);
                //if namespace is resolvable make sure class can be found in resolved namespace
                if(importsRepository.graphExists(asUri.getNameSpace())
                        && !importsRepository.resourceExistsInGraph(asUri.getNameSpace(), asUri.getURI())){
                    addConstraintViolation(context, "resource-not-found-in-resolved-namespace", "eqClass");
                }
            });
        }
    }

    private void checkSubPropertyOf(ConstraintValidatorContext context, ResourceDTO resourceDTO){
        var getSubResourceOf = resourceDTO.getSubResourceOf();
        if(getSubResourceOf != null) {
            getSubResourceOf.forEach(sub -> {
                var asUri = NodeFactory.createURI(sub);
                //if namespace is resolvable make sure class can be found in resolved namespace
                if(importsRepository.graphExists(asUri.getNameSpace())
                        && !importsRepository.resourceExistsInGraph(asUri.getNameSpace(), asUri.getURI())){
                    addConstraintViolation(context, "resource-not-found-in-resolved-namespace", "subClassOf");
                }
            });
        }
    }


    private void checkDomain(ConstraintValidatorContext context, ResourceDTO resourceDTO){
        var domain = resourceDTO.getDomain();
        if(domain != null && !domain.isBlank()){
            var checkImports = !domain.startsWith(defaultNamespace);
            if(!resourceService.checkIfResourceIsOneOfTypes(domain, List.of(RDFS.Class, OWL.Class), checkImports)){
                addConstraintViolation(context, "not-class-or-doesnt-exist", "domain");
            }
        }
    }

    private void checkRange(ConstraintValidatorContext context, ResourceDTO resourceDTO){
        var range = resourceDTO.getRange();
        if(range != null && !range.isBlank()){
            if(resourceType.equals(ResourceType.ASSOCIATION)){
                var checkImports = !range.startsWith(defaultNamespace);
                if(!resourceService.checkIfResourceIsOneOfTypes(range, List.of(RDFS.Class, OWL.Class), checkImports)){
                    addConstraintViolation(context, "not-class-or-doesnt-exist", "range");
                }
            }else{
                if(!ModelConstants.SUPPORTED_DATA_TYPES.contains(range)){
                    addConstraintViolation(context, "value-not-allowed", "range");
                }
            }

        }
    }
}
