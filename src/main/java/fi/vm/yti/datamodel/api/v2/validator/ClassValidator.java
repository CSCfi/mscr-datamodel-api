package fi.vm.yti.datamodel.api.v2.validator;

import fi.vm.yti.datamodel.api.v2.dto.ClassDTO;
import fi.vm.yti.datamodel.api.v2.repository.ImportsRepository;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import org.apache.jena.graph.NodeFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class ClassValidator extends BaseValidator implements
        ConstraintValidator<ValidClass, ClassDTO> {

    @Autowired
    private ImportsRepository importsRepository;

    boolean updateClass;

    @Override
    public void initialize(ValidClass constraintAnnotation) {
        updateClass = constraintAnnotation.updateClass();
    }

    @Override
    public boolean isValid(ClassDTO classDTO, ConstraintValidatorContext context) {
        setConstraintViolationAdded(false);

        checkLabel(context, classDTO);
        checkEditorialNote(context, classDTO);
        checkNote(context, classDTO);
        checkStatus(context, classDTO.getStatus());
        checkEquivalentClass(context, classDTO);
        checkSubClassOf(context, classDTO);
        checkSubject(context, classDTO);
        checkPrefixOrIdentifier(context, classDTO.getIdentifier(), "identifier", ValidationConstants.RESOURCE_IDENTIFIER_MAX_LENGTH, updateClass);
        checkReservedIdentifier(context, classDTO);

        return !isConstraintViolationAdded();
    }

    private void checkEquivalentClass(ConstraintValidatorContext context, ClassDTO classDTO){
        var equivalentClass = classDTO.getEquivalentClass();
        if(equivalentClass != null){
            equivalentClass.forEach(eqClass -> {
                var asUri = NodeFactory.createURI(eqClass);
                //if namespace is resolvable make sure class can be found in resolved namespace
                if(importsRepository.graphExists(asUri.getNameSpace())
                        && !importsRepository.resourceExistsInGraph(asUri.getNameSpace(), asUri.getURI())){
                    addConstraintViolation(context, "class-not-found-in-resolved-namespace", "eqClass");
                }
            });
        }
    }

    private void checkSubClassOf(ConstraintValidatorContext context, ClassDTO classDTO){
        var subClassOf = classDTO.getSubClassOf();
        if(subClassOf != null) {
            subClassOf.forEach(subClass -> {
                var asUri = NodeFactory.createURI(subClass);
                //if namespace is resolvable make sure class can be found in resolved namespace
                if(importsRepository.graphExists(asUri.getNameSpace())
                        && !importsRepository.resourceExistsInGraph(asUri.getNameSpace(), asUri.getURI())){
                    addConstraintViolation(context, "class-not-found-in-resolved-namespace", "subClassOf");
                }
            });
        }
    }

}
