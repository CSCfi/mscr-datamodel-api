package fi.vm.yti.datamodel.api.v2.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import fi.vm.yti.datamodel.api.v2.dto.GroupManagementOrganizationDTO;
import fi.vm.yti.datamodel.api.v2.dto.GroupManagementUserDTO;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.ResourceCommonDTO;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.security.YtiUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static fi.vm.yti.datamodel.api.v2.mapper.OrganizationMapper.mapGroupManagementOrganizationToModel;

@Service
public class GroupManagementService {

    private static final Logger LOG = LoggerFactory.getLogger(GroupManagementService.class);
    private final CoreRepository coreRepository;

    private final WebClient webClient;

    @Value("${fake.login.allowed:false}")
    private boolean fakeLoginAllowed;

    Cache<String, YtiUser> userCache;

    public GroupManagementService(
            @Qualifier("groupManagementClient") WebClient webClient,
            CoreRepository coreRepository) {
        this.webClient = webClient;
        this.coreRepository = coreRepository;
        userCache = CacheBuilder.newBuilder().build();
    }

    public void initOrganizations() {

        try {
            var organizations = webClient.get().uri(builder -> builder
                            .path("/organizations")
                            .queryParam("onlyValid", "true")
                            .build())
                    .retrieve()
                    .bodyToMono(new ParameterizedTypeReference<List<GroupManagementOrganizationDTO>>() {
                    })
                    .block();

            var model = mapGroupManagementOrganizationToModel(organizations);
            coreRepository.put(ModelConstants.ORGANIZATION_GRAPH, model);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public void updateUsers() {
        // TODO:
    }

    public Consumer<ResourceCommonDTO> mapUser() {
        // TODO: fetch users and set them to cache
        return (var dto) -> {
            if (dto.getCreator().getId() == null || dto.getModifier().getId() == null) {
                return;
            }
            var creator = userCache.getIfPresent(dto.getCreator().getId());
            var modifier = userCache.getIfPresent(dto.getModifier().getId());
            if (creator != null) {
                dto.getCreator().setName(creator.getFirstName() + " " + creator.getLastName());
            } else {
                dto.getCreator().setName("fake user");
            }

            if (modifier != null) {
                dto.getModifier().setName(modifier.getFirstName() + " " + modifier.getLastName());
            } else {
                dto.getModifier().setName("fake user");
            }
        };
    }

    public List<UUID> getChildOrganizations(UUID orgId) {
        return List.of();
    }

    public List<GroupManagementUserDTO> getFakeableUsers() {

        if (fakeLoginAllowed) {

            try {
                return webClient.get().uri(builder -> builder
                                .path("/users")
                                .build())
                        .retrieve()
                        .bodyToMono(new ParameterizedTypeReference<List<GroupManagementUserDTO>>() {
                        }).block();

            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
        return List.of();
    }
}
