package fi.vm.yti.datamodel.api.v2.validator;

import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.NodeInfo;
import fi.vm.yti.datamodel.api.v2.dto.ProcessingInfo;
import fi.vm.yti.datamodel.api.v2.dto.SchemaDTO;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.util.List;

import org.apache.jena.rdf.model.ResourceFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class MappingValidator extends BaseValidator implements
        ConstraintValidator<ValidMapping, MappingDTO> {

    boolean updateSchema;    

    @Override
    public void initialize(ValidMapping constraintAnnotation) {
        updateSchema = constraintAnnotation.updateSchema();
    }

    @Override
    public boolean isValid(MappingDTO dto, ConstraintValidatorContext context) {
        setConstraintViolationAdded(false);
        checkNodeInfo(context, dto.getSource());
        checkNodeInfo(context, dto.getTarget());
        checkProcessingInfo(context, dto.getProcessing());
        checkPredicate(context, dto);
        return !isConstraintViolationAdded();
    }
    
	private void checkPredicate(ConstraintValidatorContext context, MappingDTO dto) {
		if(dto.getPredicate() == null || dto.getPredicate().equals("")) {
			addConstraintViolation(context, ValidationConstants.MSG_VALUE_MISSING, "predicate");
		}
		
		if(!dto.getPredicate().startsWith("http")) {
			addConstraintViolation(context, ValidationConstants.PREFIX_REGEX, "predicate must start with http");
		}
		
	}

	private void checkProcessingInfo(ConstraintValidatorContext context, ProcessingInfo pi) {
		if(pi != null) {
			if(pi.getId() == null || pi.getId().equals("")) {
				addConstraintViolation(context, ValidationConstants.MSG_VALUE_MISSING, "processing id");
				
			}
		}
	}

	private void checkNodeInfo(ConstraintValidatorContext context, List<NodeInfo> nodes) {
		if(nodes == null || nodes.isEmpty()) {
			addConstraintViolation(context, ValidationConstants.MSG_VALUE_MISSING, "source or target");
			return;
		}
		nodes.forEach(node -> {
			if(node.getId() == null || node.getId().equals("")) {
				addConstraintViolation(context, ValidationConstants.MSG_VALUE_MISSING, "node id");
			}
		});
		
	}

}
