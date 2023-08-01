package fi.vm.yti.datamodel.api.v2.validator;

import fi.vm.yti.datamodel.api.v2.dto.BaseDTO;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import jakarta.validation.ConstraintValidatorContext;

import java.lang.annotation.Annotation;

public abstract class BaseValidator implements Annotation{

    private boolean constraintViolationAdded;

    @Override
    public Class<? extends Annotation> annotationType() {
        return BaseValidator.class;
    }


    /**
     * Add constraint violation to the constraint validator context
     * @param context Constraint validator context
     * @param message Message
     * @param property Property
     */
    void addConstraintViolation(ConstraintValidatorContext context, String message, String property) {
        if (!this.constraintViolationAdded) {
            context.disableDefaultConstraintViolation();
            this.constraintViolationAdded = true;
        }

        context.buildConstraintViolationWithTemplate(message)
                .addPropertyNode(property)
                .addConstraintViolation();
    }

    public boolean isConstraintViolationAdded() {
        return constraintViolationAdded;
    }

    public void setConstraintViolationAdded(boolean constraintViolationAdded) {
        this.constraintViolationAdded = constraintViolationAdded;
    }


    public void checkLabel(ConstraintValidatorContext context, BaseDTO dto){
        var labels = dto.getLabel();
        if (labels == null || labels.isEmpty() || labels.values().stream().anyMatch(label -> label == null || label.isBlank())){
            addConstraintViolation(context, ValidationConstants.MSG_VALUE_MISSING, "label");
        }else {
            labels.forEach((lang, value) -> checkCommonTextField(context, value, "label"));
        }
    }

    public void checkEditorialNote(ConstraintValidatorContext context, BaseDTO dto){
        var editorialNote = dto.getEditorialNote();
        checkCommonTextArea(context, editorialNote, "editorialNote");
    }

    public void checkNote(ConstraintValidatorContext context, BaseDTO dto) {
        var notes = dto.getNote();
        if(notes != null){
            notes.forEach((lang, value) -> checkCommonTextArea(context, value, "note"));
        }
    }

    public void checkStatus(ConstraintValidatorContext context, Status status){
        //Status has to be defined when creating
        if(status == null){
            addConstraintViolation(context, ValidationConstants.MSG_VALUE_MISSING, "status");
        }
    }

    public void checkSubject(ConstraintValidatorContext context, BaseDTO dto){
        var subject = dto.getSubject();
        if (subject != null && !subject.matches("^https?://uri.suomi.fi/terminology/(.*)")) {
            addConstraintViolation(context, "invalid-terminology-uri", "subject");
        }
    }

    public void checkPrefixOrIdentifier(ConstraintValidatorContext context, final String value, String propertyName, final int maxLength, boolean update){
        if(!update && (value == null || value.isBlank())){
            addConstraintViolation(context, ValidationConstants.MSG_VALUE_MISSING, propertyName);
            return;
        }else if(update && value != null){
            addConstraintViolation(context, ValidationConstants.MSG_NOT_ALLOWED_UPDATE, propertyName);
            return;
        }else if(value == null){
            //no need to check further if null
            return;
        }

        if(value.length() < ValidationConstants.PREFIX_MIN_LENGTH || value.length() > maxLength){
            addConstraintViolation(context, propertyName + "-character-count-mismatch", propertyName);
        }
    }

    public void checkReservedIdentifier(ConstraintValidatorContext context, BaseDTO dto) {
        if (dto.getIdentifier() != null && dto.getIdentifier().startsWith("corner-")) {
            addConstraintViolation(context, "reserved-identifier", "identifier");
        }
    }

    public void checkCommonTextArea(ConstraintValidatorContext context, String value, String property) {
        if(value != null && value.length() > ValidationConstants.TEXT_AREA_MAX_LENGTH){
            addConstraintViolation(context, ValidationConstants.MSG_OVER_CHARACTER_LIMIT
                    + ValidationConstants.TEXT_AREA_MAX_LENGTH, property);
        }
    }

    public void checkCommonTextField(ConstraintValidatorContext context, String value, String property) {
        if(value != null && value.length() > ValidationConstants.TEXT_FIELD_MAX_LENGTH){
            addConstraintViolation(context, ValidationConstants.MSG_OVER_CHARACTER_LIMIT
                    + ValidationConstants.TEXT_FIELD_MAX_LENGTH, property);
        }
    }}
