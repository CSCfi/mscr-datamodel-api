package fi.vm.yti.datamodel.api.service;

import java.util.Collection;
import java.util.UUID;

import org.apache.jena.iri.IRI;
import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;

@Component
@Profile("junit")
public class TestAuthorizationManagerImpl implements AuthorizationManager {

    private static final Logger logger = LoggerFactory.getLogger(TestAuthorizationManagerImpl.class.getName());

    @Override
    public boolean hasRightToAnyOrganization(final Collection<UUID> organizations) {
        return true;
    }

    @Override
    public boolean hasRightToModel(final String prefix, final Model model) {
        return true;
    }

    @Override
    public boolean isAdminOfAnyOrganization(final Collection<UUID> organizations) {
        return true;
    }

    @Override
    public boolean hasRightToRemoveClassReference(final IRI modelIRI,
                                                  final IRI classIRI) {
        return true;
    }

    @Override
    public boolean hasRightToAddClassReference(final IRI modelIRI,
                                               final String classId) {
        return true;
    }

    @Override
    public boolean hasRightToAddPredicateReference(final IRI modelIRI,
                                                   final String predicateId) {
        return true;
    }

    @Override
    public boolean hasRightToRemovePredicateReference(final IRI modelIRI,
                                                      final IRI predicateIRI) {
        return true;
    }

    @Override
    public boolean canReplicate() {
        return true;
    }

    @Override
    public boolean hasRightToDoMigration() {
        return true;
    }

    @Override
    public boolean hasRightToRunSparqlQuery() {
        return true;
    }

    @Override
    public boolean hasRightToSuggestConcept() {
        return true;
    }

    @Override
    public boolean hasRightToDropDatabase() {
        return true;
    }

	@Override
	public boolean hasRightToModelMSCR(String pid, Model model) {
		return true;
	}

}
