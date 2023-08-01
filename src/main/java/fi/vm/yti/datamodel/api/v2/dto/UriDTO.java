package fi.vm.yti.datamodel.api.v2.dto;

import com.google.common.base.Objects;

public class UriDTO {
    private String uri;
    private String curie;

    public UriDTO(String uri) {
        this.uri = uri;
    }

    public UriDTO(String uri, String curie) {
        this.uri = uri;
        this.curie = curie;
    }

    public String getCurie() {
        return curie;
    }

    public String getUri() {
        return uri;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UriDTO uriDTO = (UriDTO) o;
        return Objects.equal(uri, uriDTO.uri);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(uri);
    }
}
