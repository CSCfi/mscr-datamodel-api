package fi.vm.yti.datamodel.api.v2.validator;

import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.PropertyShapeDTO;
import fi.vm.yti.datamodel.api.v2.dto.ResourceType;
import fi.vm.yti.datamodel.api.v2.service.ResourceService;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDFS;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;

public class PropertyShapeValidator extends BaseValidator implements ConstraintValidator<ValidPropertyShape, PropertyShapeDTO> {

    boolean updateProperty;

    @Autowired
    private ResourceService resourceService;

    
    @Value("${defaultNamespace}")
    private String defaultNamespace;    

    @Override
    public void initialize(ValidPropertyShape constraintAnnotation) {
        this.updateProperty = constraintAnnotation.updateProperty();
    }

    @Override
    public boolean isValid(PropertyShapeDTO value, ConstraintValidatorContext context) {
        setConstraintViolationAdded(false);

        checkLabel(context, value);
        checkEditorialNote(context, value);
        checkStatus(context, value.getStatus());
        checkNote(context, value);
        checkPrefixOrIdentifier(context, value.getIdentifier(), "identifier", ValidationConstants.RESOURCE_IDENTIFIER_MAX_LENGTH, updateProperty);
        checkType(context, value);
        checkPath(context, value);
        checkClassType(context, value);
        checkDataType(context, value);
        checkCodeList(context, value);
        checkCommonTextArea(context, value.getDefaultValue(), "defaultValue");
        checkCommonTextArea(context, value.getHasValue(), "hasValue");
        if (value.getAllowedValues() != null) {
            value.getAllowedValues().forEach(v -> checkCommonTextArea(context, v, "allowedValues"));
        }
        return !isConstraintViolationAdded();
    }

    private void checkType(ConstraintValidatorContext context, PropertyShapeDTO resourceDTO){
        if(!updateProperty && resourceDTO.getType() == null){
            addConstraintViolation(context, ValidationConstants.MSG_VALUE_MISSING, "type");
        }else if (updateProperty && resourceDTO.getType() != null){
            addConstraintViolation(context, ValidationConstants.MSG_NOT_ALLOWED_UPDATE, "type");
        }
    }

    private void checkPath(ConstraintValidatorContext context, PropertyShapeDTO dto) {
        var path = dto.getPath();
        if(path != null && !path.isBlank()){
            var checkImports = !path.startsWith(defaultNamespace);
            if(!resourceService.checkIfResourceIsOneOfTypes(path, List.of(OWL.ObjectProperty, OWL.DatatypeProperty), checkImports)){
                addConstraintViolation(context, "not-property-or-doesnt-exist", "path");
            }
        }
    }

    private void checkClassType(ConstraintValidatorContext context, PropertyShapeDTO dto) {
        var classType = dto.getClassType();
        if (classType != null && !dto.getType().equals(ResourceType.ASSOCIATION)) {
            // only allowed for associations
            addConstraintViolation(context, "sh-class-not-allowed", "classType");
        } else if (classType != null) {
            var checkImports = !classType.startsWith(defaultNamespace);
            if(!resourceService.checkIfResourceIsOneOfTypes(classType, List.of(RDFS.Class, OWL.Class), checkImports)){
                addConstraintViolation(context, "not-class-or-doesnt-exist", "path");
            }
        }
    }

    private void checkDataType(ConstraintValidatorContext context, PropertyShapeDTO dto) {
        if (dto.getDataType() != null && !ModelConstants.SUPPORTED_DATA_TYPES.contains(dto.getDataType())) {
            addConstraintViolation(context, "unsupported-datatype", "dataType");
        }
    }

    private void checkCodeList(ConstraintValidatorContext context, PropertyShapeDTO dto) {
        if(dto.getCodeList() != null && !dto.getCodeList().startsWith(ModelConstants.CODELIST_NAMESPACE)){
            addConstraintViolation(context, "invalid-codelist-uri", "codelist");
        }
    }
}
