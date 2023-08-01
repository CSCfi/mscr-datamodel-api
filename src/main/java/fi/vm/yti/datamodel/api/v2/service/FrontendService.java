package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.v2.dto.OrganizationDTO;
import fi.vm.yti.datamodel.api.v2.dto.ServiceCategoryDTO;
import fi.vm.yti.datamodel.api.v2.mapper.OrganizationMapper;
import fi.vm.yti.datamodel.api.v2.mapper.ServiceCategoryMapper;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Service;

import java.util.List;

import static fi.vm.yti.datamodel.api.v2.dto.ModelConstants.DEFAULT_LANGUAGE;

@Service
public class FrontendService {

    private final CoreRepository coreRepository;
    public FrontendService(CoreRepository coreRepository) {
        this.coreRepository = coreRepository;
    }

    public List<OrganizationDTO> getOrganizations(@NotNull String sortLanguage, boolean includeChildOrganizations) {
        var organizations = coreRepository.getOrganizations();
        var dtos = OrganizationMapper.mapToListOrganizationDTO(organizations);

        dtos.sort((a, b) -> {
            var labelA = a.getLabel().getOrDefault(sortLanguage, a.getLabel().get(DEFAULT_LANGUAGE));
            var labelB = b.getLabel().getOrDefault(sortLanguage, b.getLabel().get(DEFAULT_LANGUAGE));
            return labelA.compareTo(labelB);
        });

        return includeChildOrganizations ? dtos : dtos.stream()
                .filter(dto -> dto.getParentOrganization() == null)
                .toList();
    }

    public List<ServiceCategoryDTO> getServiceCategories(@NotNull String sortLanguage) {
        var serviceCategories = coreRepository.getServiceCategories();
        var dtos = ServiceCategoryMapper.mapToListServiceCategoryDTO(serviceCategories);

        dtos.sort((a, b) -> {
            var labelA = a.getLabel().getOrDefault(sortLanguage, a.getLabel().get(DEFAULT_LANGUAGE));
            var labelB = b.getLabel().getOrDefault(sortLanguage, b.getLabel().get(DEFAULT_LANGUAGE));
            return labelA.compareTo(labelB);
        });

        return dtos;
    }
}
