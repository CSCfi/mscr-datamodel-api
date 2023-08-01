package fi.vm.yti.datamodel.api.security;

import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.security.AuthenticatedUserProvider;
import fi.vm.yti.security.YtiUser;
import org.apache.jena.iri.IRI;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.vocabulary.DCTerms;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.EnumSet;
import java.util.UUID;
import java.util.stream.Collectors;

import static fi.vm.yti.security.Role.ADMIN;
import static fi.vm.yti.security.Role.DATA_MODEL_EDITOR;

@Service
@Profile("!junit")
public class AuthorizationManagerImpl implements AuthorizationManager {

    private final AuthenticatedUserProvider userProvider;
    private final ModelConstants modelConstants;

    AuthorizationManagerImpl(AuthenticatedUserProvider userProvider,
    		ModelConstants modelConstants) {
        this.userProvider = userProvider;
        this.modelConstants = modelConstants;
    }

    public boolean hasRightToAnyOrganization(Collection<UUID> organizations) {
        YtiUser user = getUser();
        return user.isSuperuser() || user.isInAnyRole(EnumSet.of(ADMIN, DATA_MODEL_EDITOR), organizations);
    }
    
    public boolean hasRightToModel(String prefix, Model model){
        var oldRes = model.getResource(modelConstants.getDefaultNamespace() + prefix);
        var organizations = oldRes.listProperties(DCTerms.contributor).toList().stream().map(prop -> {
            var orgUri = prop.getObject().toString();
            return UUID.fromString(
                    orgUri.substring(
                            orgUri.lastIndexOf(":")+ 1));
        }).collect(Collectors.toSet());
        return hasRightToAnyOrganization(organizations);
    }

    public boolean isAdminOfAnyOrganization(Collection<UUID> organizations) {
        YtiUser user = getUser();
        return user.isSuperuser() || user.isInAnyRole(EnumSet.of(ADMIN), organizations);
    }

    private YtiUser getUser() {
        return this.userProvider.getUser();
    }

    public boolean hasRightToRemoveClassReference(IRI modelIRI,
                                                  IRI classIRI) {
        // TODO
        return true;
    }

    public boolean hasRightToAddClassReference(IRI modelIRI,
                                               String classId) {
        // TODO
        return true;
    }

    public boolean hasRightToAddPredicateReference(IRI modelIRI,
                                                   String predicateId) {
        // TODO
        return true;
    }

    public boolean hasRightToRemovePredicateReference(IRI modelIRI,
                                                      IRI predicateIRI) {
        // TODO
        return true;
    }

    public boolean canReplicate() {
        return this.getUser().isSuperuser();
    }

    public boolean hasRightToDoMigration() {
        return getUser().isSuperuser();
    }

    public boolean hasRightToRunSparqlQuery() {
        return getUser().isSuperuser();
    }

    public boolean hasRightToSuggestConcept() {
        YtiUser user = getUser();
        return user.isSuperuser() || !user.getOrganizations(ADMIN, DATA_MODEL_EDITOR).isEmpty();
    }

    public boolean hasRightToDropDatabase() {
        return getUser().isSuperuser();
    }
}
