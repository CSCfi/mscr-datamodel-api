package fi.vm.yti.datamodel.api.v2.opensearch.dto;

import fi.vm.yti.datamodel.api.v2.dto.ModelType;
import fi.vm.yti.datamodel.api.v2.dto.Status;

import java.util.Set;
import java.util.UUID;

public class CountRequest {
    private String query;
    private String language;
    private Set<ModelType> type;
    private Set<Status> status;
    private Set<UUID> organizations;
    private Set<String> groups;
    private Set<UUID> includeIncompleteFrom;

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public Set<ModelType> getType() {
        return type;
    }

    public void setType(Set<ModelType> type) {
        this.type = type;
    }

    public Set<Status> getStatus() {
        return status;
    }

    public void setStatus(Set<Status> status) {
        this.status = status;
    }

    public Set<UUID> getOrganizations() {
        return organizations;
    }

    public void setOrganizations(Set<UUID> organizations) {
        this.organizations = organizations;
    }

    public Set<String> getGroups() {
        return groups;
    }

    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

    public Set<UUID> getIncludeIncompleteFrom() {
        return includeIncompleteFrom;
    }

    public void setIncludeIncompleteFrom(Set<UUID> includeIncompleteFrom) {
        this.includeIncompleteFrom = includeIncompleteFrom;
    }
}
