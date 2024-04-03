package fi.vm.yti.datamodel.api.v2.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import fi.vm.yti.datamodel.api.v2.dto.GroupManagementOrganizationDTO;
import fi.vm.yti.datamodel.api.v2.dto.GroupManagementUserDTO;
import fi.vm.yti.datamodel.api.v2.dto.Iow;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.OwnerDTO;
import fi.vm.yti.datamodel.api.v2.dto.ResourceCommonDTO;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import fi.vm.yti.datamodel.api.v2.mapper.OrganizationMapper;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.security.YtiUser;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static fi.vm.yti.datamodel.api.v2.mapper.OrganizationMapper.mapOrganizationsToModel;

@Service
public class GroupManagementService {

    private static final Logger LOG = LoggerFactory.getLogger(GroupManagementService.class);
    private final CoreRepository coreRepository;

    private final WebClient webClient;

    @Value("${fake.login.allowed:false}")
    private boolean fakeLoginAllowed;

    private final Cache<String, GroupManagementUserDTO> userCache;

    public GroupManagementService(
            @Qualifier("groupManagementClient") WebClient webClient,
            CoreRepository coreRepository) {
        this.webClient = webClient;
        this.coreRepository = coreRepository;
        userCache = CacheBuilder.newBuilder().build();
    }

    public void initOrganizations() {
    	
        var organizations = webClient.get().uri(builder -> builder
                        .path("/organizations")
                        .queryParam("onlyValid", "true")
                        .build())
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<List<GroupManagementOrganizationDTO>>() {
                })
                .block(Duration.of(10, ChronoUnit.SECONDS));
        if(organizations == null || organizations.isEmpty()){
            throw new GroupManagementException("No organizations found, is group management service down?");
        }

        var model = ModelFactory.createDefaultModel();
        mapOrganizationsToModel(organizations, model);
        coreRepository.put(ModelConstants.ORGANIZATION_GRAPH, model);
        LOG.info("Initialized organizations with {} organizations", organizations.size());
    }

    @Scheduled(cron = "0 */30 * * * *")
    public void updateOrganizations() {
        LOG.info("Updating organizations cache");
        var organizations = webClient.get().uri(builder -> builder
                        .path("/organizations")
                        .queryParam("onlyValid", "true")
                        .build())
                .ifModifiedSince(ZonedDateTime.now().minusMinutes(30))
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<List<GroupManagementOrganizationDTO>>() {
                })
                .block();

        if (organizations != null && !organizations.isEmpty()) {
            var model = coreRepository.fetch(ModelConstants.ORGANIZATION_GRAPH);
            mapOrganizationsToModel(organizations, model);
            coreRepository.put(ModelConstants.ORGANIZATION_GRAPH, model);
            coreRepository.invalidateOrganizationCache();
            LOG.info("Updated {} organizations to fuseki", organizations.size());
        }else {
            LOG.info("No updates to organizations found");
        }
    }

    public void initUsers() {
        LOG.info("Initializing user cache");
        var users = webClient.get()
                .uri(builder -> builder
                        .path("/users")
                        .build())
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<List<GroupManagementUserDTO>>() {
                })
                .block();

        if(users == null || users.isEmpty()){
            throw new GroupManagementException("No users found, is group service down?");
        }

        var map = users.stream().collect(Collectors.toMap(user -> user.getId().toString(), user -> user));
        userCache.invalidateAll();
        userCache.putAll(map);
        LOG.info("Initialized user cache with {} users", map.size());
    }

    @Scheduled(cron = "0 */30 * * * *")
    public void updateUsers() {
        LOG.info("Updating user cache");
        var users = webClient.get()
                .uri(builder -> builder
                    .path("/users")
                .build())
                .ifModifiedSince(ZonedDateTime.now().minusMinutes(30))
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<List<GroupManagementUserDTO>>() {
                })
                .block();

        if(users != null && !users.isEmpty()){
            var oldSize = userCache.size();
            var map = users.stream().collect(Collectors.toMap(user -> user.getId().toString(), user -> user));
            userCache.putAll(map);
            LOG.info("Updated {} users to cache, old count: {}, new count: {}", map.size(), oldSize, userCache.size());
        }else{
            LOG.info("No modifications to users found");
        }
    }

    public Consumer<ResourceCommonDTO> mapUser() {
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
    
    public Consumer<OwnerDTO> mapOwner() {
    	return (var dto) -> {
    		if(dto.getId() == null) {
    			return;
    		}
    		var user = userCache.getIfPresent(dto.getId());
    		if(user != null) {
    			dto.setName(user.getFirstName() + " " + user.getLastName());
    			
    		} else {
    			Model orgModel = coreRepository.getOrganizationsByIds(Set.of(UUID.fromString(dto.getId())));
    			if(orgModel != null) {
    				var orgDTOs = OrganizationMapper.mapToListOrganizationDTO(orgModel);
    				if(!orgDTOs.isEmpty()) {
    					dto.setName(orgDTOs.iterator().next().getLabel());
    				}
    				else {
    					LOG.info("Could not map owner info. Owner is referring a user or an org that no longer exists");	
    				}
    			}
    			else {
    				LOG.info("Could not map owner info. Owner is referring a user or an org that no longer exists");
    			}
    		}
    		
    	};
    }
    
    public List<UUID> getChildOrganizations(UUID orgId) {
        var orgUrn = ModelConstants.URN_UUID + orgId.toString();
        if(!coreRepository.resourceExistsInGraph(ModelConstants.ORGANIZATION_GRAPH, orgUrn)){
            LOG.warn("Organization not found {}", orgUrn);
            return new ArrayList<>();
        }
        var model = coreRepository.getOrganizations();

        var resource = model.getResource(orgUrn);

        return MapperUtils.arrayPropertyToList(resource, Iow.parentOrganization).stream()
                .map(MapperUtils::getUUID).toList();
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

    public Set<UUID> getOrganizationsForUser(YtiUser user) {
        final var rolesInOrganizations = user.getRolesInOrganizations();

        var orgIds = new HashSet<>(rolesInOrganizations.keySet());

        // show child organization's incomplete content for main organization users
        var childOrganizationIds = orgIds.stream()
                .map(this::getChildOrganizations)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        orgIds.addAll(childOrganizationIds);
        return orgIds;
    }

    private static final class GroupManagementException extends RuntimeException{
        private GroupManagementException(String message) {
            super(message);
        }
    }
}
