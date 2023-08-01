package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.v2.endpoint.EndpointUtils;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResolvingException;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.repository.ImportsRepository;
import fi.vm.yti.datamodel.api.v2.validator.ValidationConstants;
import fi.vm.yti.security.AuthenticatedUserProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
@Import({
        NamespaceResolver.class
})
class NamespaceResolverTest {

    @MockBean
    ImportsRepository importsRepository;

    @MockBean
    OpenSearchIndexer openSearchIndexer;

    @MockBean
    AuthenticatedUserProvider userProvider;

    @Autowired
    NamespaceResolver namespaceResolver;


    @BeforeEach
    void setup(){
        when(userProvider.getUser()).thenReturn(EndpointUtils.mockUser);
    }

    @Test
    void shouldTryToResolve() {
        namespaceResolver.resolve("notrealaddress", false);
        verify(userProvider).getUser();
        verify(importsRepository).graphExists(anyString());
    }

    @ParameterizedTest
    @MethodSource("reservedNamespaceProvider")
    void shouldFailToResolveReserved(String namespace) {
        var error = assertThrows(ResolvingException.class, () -> namespaceResolver.resolve(namespace, false));
        assertEquals("Error during resolution: Reserved namespace", error.getMessage());
    }

    private static Stream<Arguments> reservedNamespaceProvider(){
        return ValidationConstants.RESERVED_NAMESPACES.values().stream().map(Arguments::of);
    }

    @Test
    void testForceResolution() {
        when(namespaceResolver.namespaceAlreadyResolved(anyString())).thenReturn(true);

        var error = assertThrows(ResolvingException.class, () -> namespaceResolver.resolve("notrealaddress", false));
        assertEquals("Error during resolution: Already resolved", error.getMessage());

        var result = namespaceResolver.resolve("notrealaddress", true);
        assertFalse(result);
    }
}
