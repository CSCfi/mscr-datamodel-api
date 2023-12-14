package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.dto.MSCRUser;
import fi.vm.yti.datamodel.api.v2.dto.OrganizationDTO;
import fi.vm.yti.datamodel.api.v2.service.FrontendService;
import fi.vm.yti.datamodel.api.v2.service.GroupManagementService;
import fi.vm.yti.security.AuthenticatedUserProvider;
import fi.vm.yti.security.YtiUser;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Tag(name = "Users")
public class UserController {

    private final AuthenticatedUserProvider userProvider;
    private final GroupManagementService groupService;
    private final FrontendService frontendService;

    UserController(AuthenticatedUserProvider userProvider, GroupManagementService groupService, FrontendService frontendService) {
        this.userProvider = userProvider;
        this.groupService = groupService;
        this.frontendService = frontendService;
    }

    @GetMapping("v2/user_old")    
    @Operation(description = "Get authenticated user")
    @ApiResponse(responseCode = "200", description = "User object")
    public YtiUser getUser() {
        return userProvider.getUser();
    }
    
    @GetMapping("v2/user")    
    @Operation(description = "Get authenticated MSCR user")
    @ApiResponse(responseCode = "200", description = "MSCR User object")
    public MSCRUser getMSCRUser() {
        YtiUser ytiuser = userProvider.getUser();        
        Set<UUID> ids = groupService.getOrganizationsForUser(ytiuser);
        List<OrganizationDTO> orgs = frontendService.getOrganizationsByID(ids);
        MSCRUser user = new MSCRUser(ytiuser, orgs);
        return user;
    }    
}