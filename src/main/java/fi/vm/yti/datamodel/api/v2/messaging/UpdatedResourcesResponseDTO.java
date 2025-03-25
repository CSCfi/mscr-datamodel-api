package fi.vm.yti.datamodel.api.v2.messaging;

import java.util.List;

import fi.vm.yti.datamodel.api.v2.opensearch.index.UpdatedResourceDTO;


public class UpdatedResourcesResponseDTO {

    private Meta meta;
    private List<UpdatedResourceDTO> results;

    public UpdatedResourcesResponseDTO() {
    }

    public Meta getMeta() {
        return meta;
    }

    public void setMeta(final Meta meta) {
        this.meta = meta;
    }

    public List<UpdatedResourceDTO> getResults() {
        return results;
    }

    public void setResults(final List<UpdatedResourceDTO> results) {
        this.results = results;
    }
}