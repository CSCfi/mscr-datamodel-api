package fi.vm.yti.datamodel.api.v2.repository;

import org.apache.jena.rdfconnection.RDFConnection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
public class ImportsRepository extends BaseRepository {

    public ImportsRepository(@Value(("${endpoint}")) String endpoint) {
        super(
                RDFConnection.connect(endpoint + "/imports/get"),
                RDFConnection.connect(endpoint + "/imports/data"),
                RDFConnection.connect(endpoint + "/imports/sparql")
        );
    }
}
