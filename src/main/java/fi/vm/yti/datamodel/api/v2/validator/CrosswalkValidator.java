package fi.vm.yti.datamodel.api.v2.validator;

import fi.vm.yti.datamodel.api.v2.dto.CrosswalkDTO;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.SchemaDTO;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.apache.jena.rdf.model.ResourceFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class CrosswalkValidator extends BaseValidator implements
        ConstraintValidator<ValidCrosswalk, CrosswalkDTO> {

    @Autowired
    private CoreRepository coreRepository;

    boolean update;    

    @Override
    public void initialize(ValidCrosswalk constraintAnnotation) {
        update = constraintAnnotation.updateSchema();
    }

    @Override
    public boolean isValid(CrosswalkDTO dto, ConstraintValidatorContext context) {
        setConstraintViolationAdded(false);
        checkStatus(context, dto.getStatus());
        checkLanguages(context, dto);
        checkLabels(context, dto);
        checkDescription(context, dto);
        checkOrganizations(context, dto);
        
        return !isConstraintViolationAdded();
    }
    
    
    /**
     * Check if languages are valid
     * @param context Constraint validator context
     * @param dataModel Data model
     */
    private void checkLanguages(ConstraintValidatorContext context, CrosswalkDTO schema){
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
    private void checkLabels(ConstraintValidatorContext context, CrosswalkDTO schema){
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
    private void checkDescription(ConstraintValidatorContext context, CrosswalkDTO schema){
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
    private void checkOrganizations(ConstraintValidatorContext context, CrosswalkDTO schema){
        var organizations = schema.getOrganizations();
        var existingOrgs = coreRepository.getOrganizations();
        if(organizations.isEmpty()){
            addConstraintViolation(context, ValidationConstants.MSG_VALUE_MISSING, "organization");
            return;
        }
        organizations.forEach(org -> {
            var queryRes = ResourceFactory.createResource(ModelConstants.URN_UUID + org.toString());
            if(!existingOrgs.containsResource(queryRes)){
                addConstraintViolation(context, "does-not-exist." + org, "organizations");
            }
        });
    }

}
