package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.dto.GroupManagementUserDTO;
import fi.vm.yti.datamodel.api.v2.service.GroupManagementService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("v2/fakeable-users")
@Tag(name = "Admin")
public class FakeableUserController {
    private final GroupManagementService groupManagementService;

    FakeableUserController(GroupManagementService groupManagementService) {
        this.groupManagementService = groupManagementService;
    }

    @GetMapping
    @Operation(description = "Get fakeable users")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "List of user objects")
    })
    public List<GroupManagementUserDTO> getFakeableUsers() {
        return groupManagementService.getFakeableUsers();
    }
}
