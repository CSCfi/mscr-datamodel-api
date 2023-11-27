package fi.vm.yti.datamodel.api.v2.validator;

import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.SchemaDTO;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.apache.jena.rdf.model.ResourceFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class SchemaValidator extends BaseValidator implements
        ConstraintValidator<ValidSchema, SchemaDTO> {

    @Autowired
    private CoreRepository coreRepository;

    boolean updateSchema;    

    @Override
    public void initialize(ValidSchema constraintAnnotation) {
        updateSchema = constraintAnnotation.updateSchema();
    }

    @Override
    public boolean isValid(SchemaDTO dto, ConstraintValidatorContext context) {
        setConstraintViolationAdded(false);
        checkStatus(context, dto.getStatus());
        checkLanguages(context, dto);
        checkLabels(context, dto);
        checkDescription(context, dto);
        checkOrganizations(context, dto);
        
        checkNamespace(context, dto);
        checkState(context, dto.getState());
        checkVisibility(context, dto.getVisibility(), dto.getState());
        
        return !isConstraintViolationAdded();
    }
    
	/**
     * If updating, aggregation key cannot be set
     * 
     */
    private void checkNamespace(ConstraintValidatorContext context, SchemaDTO dto){
    	var ns = dto.getNamespace();
    	if(ns == null) {
    		addConstraintViolation(context, ValidationConstants.MSG_VALUE_MISSING, "namespace");
    	}
    	else if(!ns.startsWith("http")) {
    		addConstraintViolation(context, "invaldi-namespace-value", "namespace");
    	}
    	
    }
    
    /**
     * Check if languages are valid
     * @param context Constraint validator context
     * @param dataModel Data model
     */
    private void checkLanguages(ConstraintValidatorContext context, SchemaDTO schema){
        var languages = schema.getLanguages();

        if (languages.isEmpty()) {
            addConstraintViolation(context, ValidationConstants.MSG_VALUE_MISSING, "languages");
            return;
        }

        languages.forEach(language -> {
            //Matches RFC-4646
            if(!language.matches("^[a-z]{2,3}(?:-[A-Z]{2,3}(?:-[a-zA-Z]{4})?)?$")){
                addConstraintViolation(context, "does-not-match-rfc-4646", "languages");
            }
        });
    }

    /**
     * Check if labels are valid
     * @param context Constraint validator context
     * @param dataModel Data Model
     */
    private void checkLabels(ConstraintValidatorContext context, SchemaDTO schema){
        final var labelPropertyLabel = "label";
        var labels = schema.getLabel();
        var languages = schema.getLanguages();

        if (labels == null || labels.isEmpty() || labels.values().stream().anyMatch(label -> label == null || label.isBlank())) {
            addConstraintViolation(context, ValidationConstants.MSG_VALUE_MISSING, "label");
        } else if(labels.size() != languages.size()){
            addConstraintViolation(context, "label-language-count-mismatch", labelPropertyLabel);
        } else {
            labels.forEach((key, value) -> {
                if (!languages.contains(key)) {
                    addConstraintViolation(context, "language-not-in-language-list." + key, labelPropertyLabel);
                }
                checkCommonTextField(context, value, labelPropertyLabel);
            });
        }
    }

    /**
     * Check if descriptions are valid
     * @param context Constraint validator context
     * @param dataModel Data model
     */
    private void checkDescription(ConstraintValidatorContext context, SchemaDTO schema){
        var description = schema.getDescription();
        var languages = schema.getLanguages();
        description.forEach((key, value) -> {
            if (!languages.contains(key)) {
                addConstraintViolation(context, "language-not-in-language-list." + key, "description");
            }
            if(value.length() > ValidationConstants.TEXT_AREA_MAX_LENGTH){
                addConstraintViolation(context, ValidationConstants.MSG_OVER_CHARACTER_LIMIT + ValidationConstants.TEXT_AREA_MAX_LENGTH, "description");
            }
        });
    }

    /**
     * Check if organizations are valid
     * @param context Constraint validator context
     * @param dataModel DataModel
     */
    private void checkOrganizations(ConstraintValidatorContext context, SchemaDTO schema){
        var organizations = schema.getOrganizations();
        var existingOrgs = coreRepository.getOrganizations();
        organizations.forEach(org -> {
            var queryRes = ResourceFactory.createResource(ModelConstants.URN_UUID + org.toString());
            if(!existingOrgs.containsResource(queryRes)){
                addConstraintViolation(context, "does-not-exist." + org, "organizations");
            }
        });
    }

}
