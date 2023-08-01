package fi.vm.yti.datamodel.api.v2.utils;

import fi.vm.yti.datamodel.api.v2.dto.DCAP;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class DataModelUtils {

    private DataModelUtils() {
        //Util class
    }

    public static String encode(String param) {
        return URLEncoder.encode(param, StandardCharsets.UTF_8);
    }

    public static void addPrefixesToModel(String modelURI, Model model) {
        model.setNsPrefixes(ModelConstants.PREFIXES);
        model.setNsPrefix(MapperUtils.getModelIdFromNamespace(modelURI), modelURI + ModelConstants.RESOURCE_SEPARATOR);

        var modelResource = model.getResource(modelURI);

        List.of(OWL.imports, DCTerms.requires).forEach(property ->
                modelResource.listProperties(property).forEach(res -> {
                    var uri = res.getObject().toString();
                    if (uri.startsWith(ModelConstants.SUOMI_FI_NAMESPACE)) {
                        var prefix = MapperUtils.getModelIdFromNamespace(uri);
                        model.setNsPrefix(prefix, uri + ModelConstants.RESOURCE_SEPARATOR);
                    } else if (!uri.contains("uri.suomi.fi")) {
                        // handle external namespaces, skip for now terminologies and code lists
                        var extResource = model.getResource(uri);
                        var extPrefix = MapperUtils.propertyToString(extResource, DCAP.preferredXMLNamespacePrefix);
                        if (extPrefix != null) {
                            model.setNsPrefix(extPrefix, uri);
                        }
                    }
                })
        );
    }
}
