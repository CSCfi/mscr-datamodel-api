package fi.vm.yti.datamodel.api.v2.mapper;

import fi.vm.yti.datamodel.api.v2.dto.GroupManagementOrganizationDTO;
import fi.vm.yti.datamodel.api.v2.dto.Iow;
import fi.vm.yti.datamodel.api.v2.dto.OrganizationDTO;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.SKOS;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static fi.vm.yti.datamodel.api.v2.dto.ModelConstants.URN_UUID;
import static fi.vm.yti.datamodel.api.v2.dto.ModelConstants.USED_LANGUAGES;
import static fi.vm.yti.datamodel.api.v2.mapper.MapperUtils.getUUID;
import static fi.vm.yti.datamodel.api.v2.mapper.MapperUtils.localizedPropertyToMap;

public class OrganizationMapper {

    private OrganizationMapper() {
    }

    public static void mapOrganizationsToModel(List<GroupManagementOrganizationDTO> organizations, Model orgModel) {
        organizations.forEach(organization -> {
            var resource = orgModel.createResource(URN_UUID + organization.getUuid());
            //remove all old properties before remapping them
            resource.removeProperties();

            resource.addProperty(RDF.type, FOAF.Organization);

            USED_LANGUAGES.forEach(lang -> {
                var label = organization.getPrefLabel().get(lang);
                var description = organization.getDescription().get(lang);

                if (StringUtils.isNotBlank(label)) {
                    resource.addLiteral(SKOS.prefLabel, ResourceFactory.createLangLiteral(label, lang));
                }
                if (StringUtils.isNotBlank(description)) {
                    resource.addLiteral(DCTerms.description, ResourceFactory.createLangLiteral(description, lang));
                }
            });

            if (StringUtils.isNotBlank(organization.getParentId())) {
                resource.addProperty(Iow.parentOrganization, URN_UUID + organization.getParentId());
            }
            if (StringUtils.isNotBlank(organization.getUrl())) {
                resource.addLiteral(FOAF.homepage, organization.getUrl());
            }
        });
    }

    public static Set<OrganizationDTO> mapOrganizationsToDTO(Set<String> organizations, Model organizationModel) {
        return organizations.stream().map(org -> {
            var orgRes = organizationModel.getResource(org);
            var labels = localizedPropertyToMap(orgRes, SKOS.prefLabel);
            var id = getUUID(orgRes.getURI());

            var parentId = getUUID(MapperUtils.propertyToString(orgRes, Iow.parentOrganization));
            if(id == null){
                throw new MappingError("Could not map organization");
            }
            return new OrganizationDTO(id.toString(), labels, parentId);
        }).collect(Collectors.toSet());
    }

    public static List<OrganizationDTO> mapToListOrganizationDTO(Model organizationModel) {
        var iterator = organizationModel.listResourcesWithProperty(RDF.type, FOAF.Organization);
        List<OrganizationDTO> result = new ArrayList<>();

        while (iterator.hasNext()) {
            var resource = iterator.next().asResource();

            var labels = localizedPropertyToMap(resource, SKOS.prefLabel);
            var id = getUUID(resource.getURI());

            var parentId = getUUID(MapperUtils.propertyToString(resource, Iow.parentOrganization));

            if (id != null) {
                result.add(new OrganizationDTO(id.toString(), labels, parentId));
            }
        }
        return result;
    }

}
