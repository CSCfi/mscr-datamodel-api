package fi.vm.yti.datamodel.api.v2.repository;

import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.service.JenaQueryException;
import org.apache.jena.arq.querybuilder.AskBuilder;
import org.apache.jena.arq.querybuilder.UpdateBuilder;
import org.apache.jena.arq.querybuilder.WhereBuilder;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.update.UpdateRequest;
import org.springframework.http.HttpStatus;

import java.util.function.Consumer;

public abstract class BaseRepository {

    RDFConnection read;
    RDFConnection write;
    RDFConnection sparql;
    RDFConnection update;

    protected BaseRepository(RDFConnection read, RDFConnection write) {
        this.read = read;
        this.write = write;
    }

    protected BaseRepository(RDFConnection read, RDFConnection write, RDFConnection sparql) {
        this.read = read;
        this.write = write;
        this.sparql = sparql;
    }

    protected BaseRepository(RDFConnection read, RDFConnection write, RDFConnection sparql, RDFConnection update) {
        this.read = read;
        this.write = write;
        this.sparql = sparql;
        this.update = update;
    }

    public Model fetch(String graph) {
        try {
            return read.fetch(graph);
        } catch (HttpException ex) {
            if (ex.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
                throw new ResourceNotFoundException(graph);
            } else {
                throw new JenaQueryException();
            }
        }
    }

    public void put(String graph, Model model) {
        write.put(graph, model);
    }

    public void delete(String graph) {
        try{
            write.delete(graph);
        } catch (HttpException ex) {
            if (ex.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
                throw new ResourceNotFoundException(graph);
            } else {
                throw new JenaQueryException();
            }
        }
    }

    public void deleteResource(String resource) {
        var deleteBuilder = new UpdateBuilder();
        var expr = deleteBuilder.getExprFactory();
        var filter = expr.or(expr.eq("?s", NodeFactory.createURI(resource)), expr.eq("?o", NodeFactory.createURI(resource)));
        deleteBuilder.addDelete("?g", "?s", "?p", "?o")
                .addGraph("?g", new WhereBuilder().addWhere("?s", "?p", "?o").addFilter(filter));
        try{
            update.update(deleteBuilder.buildRequest());
        } catch (HttpException ex) {
            if (ex.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
                throw new ResourceNotFoundException(resource);
            } else {
                throw new JenaQueryException();
            }
        }
    }

    public boolean graphExists(String graph) {
        var askBuilder = new AskBuilder()
                .addGraph(NodeFactory.createURI(graph), "?s", "?p", "?o");
        return this.queryAsk(askBuilder.build());
    }

    public boolean resourceExistsInGraph(String graph, String resource) {
        var askBuilder = new AskBuilder()
                .addGraph(NodeFactory.createURI(graph),
                        NodeFactory.createURI(resource), "?p", "?o");
        return this.queryAsk(askBuilder.build());
    }

    public Model queryConstruct(Query query) {
        return sparql.queryConstruct(query);
    }

    public void querySelect(Query query, Consumer<QuerySolution> consumer) {
        sparql.querySelect(query, consumer);
    }

    public void querySelect(String query, Consumer<QuerySolution> consumer) {
        sparql.querySelect(query, consumer);
    }

    public boolean queryAsk(Query query) {
        try {
            return sparql.queryAsk(query);
        }catch(HttpException ex){
            throw new JenaQueryException();
        }
    }

    public void queryUpdate(String query) {
        update.update(query);
    }

    public void queryUpdate(UpdateRequest query) {
        update.update(query);
    }
}
