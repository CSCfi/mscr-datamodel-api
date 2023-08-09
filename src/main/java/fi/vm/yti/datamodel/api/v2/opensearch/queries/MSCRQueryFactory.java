package fi.vm.yti.datamodel.api.v2.opensearch.queries;

import static fi.vm.yti.datamodel.api.v2.opensearch.OpenSearchUtil.logPayload;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.SortOptionsBuilders;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.mapping.FieldType;
import org.opensearch.client.opensearch._types.query_dsl.ExistsQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.QueryBuilders;
import org.opensearch.client.opensearch.core.SearchRequest;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.MSCRSearchRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;

public class MSCRQueryFactory {
	
	private static List<String> indices = List.of(OpenSearchIndexer.OPEN_SEARCH_INDEX_CROSSWALK, OpenSearchIndexer.OPEN_SEARCH_INDEX_SCHEMA);
	
	public static SearchRequest createMSCRQuery(MSCRSearchRequest request) {

		var must = new ArrayList<Query>();
		var mustNot = new ArrayList<Query>();

        var queryString = request.getQuery();
        if(queryString != null && !queryString.isBlank()){
            must.add(QueryFactoryUtils.labelQuery(queryString));
        }
        
       
        var type = request.getType();
        if(type != null && !type.isEmpty()){
            var typeQuery =  QueryFactoryUtils.termsQuery("type", type.stream().map(MSCRType::name).toList());
            must.add(typeQuery);            
        }

        var status = request.getStatus();
        if(status != null && !status.isEmpty()){
            var statusQuery = QueryFactoryUtils.termsQuery("status", status.stream().map(Status::name).toList());
            must.add(statusQuery);              
        }
        
        var organizations = request.getOrganizations();
        if(organizations != null && !organizations.isEmpty()){
            var orgsQuery = QueryFactoryUtils.termsQuery("contributor", organizations.stream().map(UUID::toString).toList());
            must.add(orgsQuery);

        }

        var format = request.getFormat();
        if(format != null && !format.isEmpty()){
            var formatQuery =  QueryFactoryUtils.termsQuery("format", format.stream().toList());
            must.add(formatQuery);            
        }

        var sourceType = request.getSourceType();
        if(sourceType != null) {
        	var sourceTypeQuery = QueryFactoryUtils.termQuery("format", "MSCR");
        	if(sourceType == MSCR.SOURCE_TYPE.HOSTED) {
        		must.add(sourceTypeQuery);
        	}
        	else {
        		mustNot.add(sourceTypeQuery);
        	}
        	
        }
        
        var sourceSchemas = request.getSourceSchemas();
        if(sourceSchemas != null && !sourceSchemas.isEmpty()){
            var sourceSchemasQuery = QueryFactoryUtils.termsQuery("sourceSchema", sourceSchemas.stream().toList());
            must.add(sourceSchemasQuery);
        }        
        
        var targetSchemas = request.getTargetSchemas();
        if(targetSchemas != null && !targetSchemas.isEmpty()){
            var targetSchemasQuery = QueryFactoryUtils.termsQuery("targetSchema", targetSchemas.stream().toList());
            must.add(targetSchemasQuery);
        }  

        
        var finalQuery = QueryBuilders.bool();
        finalQuery.must(must);
        if(!mustNot.isEmpty()) {        	
        	finalQuery.mustNot(mustNot);
        }
        
        // only return the latest version
        // --> hasRevision is empty --> no hasRevision field 
        finalQuery.mustNot(new ExistsQuery.Builder().field("hasRevision").build()._toQuery());
        
        var sortLang = request.getSortLang() != null ? request.getSortLang() : QueryFactoryUtils.DEFAULT_SORT_LANG;
        var sort = SortOptionsBuilders.field()
                .field("label." + sortLang + ".keyword")
                .order(SortOrder.Asc)
                .unmappedType(FieldType.Keyword)
                .build();

        
        var sr = new SearchRequest.Builder()        		
                .index(indices)                
                .size(QueryFactoryUtils.pageSize(request.getPageSize()))
                .from(QueryFactoryUtils.pageFrom(request.getPageFrom()))
                .sort(SortOptions.of(s -> s.field(sort)))                
                .query(finalQuery.build()._toQuery());

        if(request.isIncludeFacets()) {
        	// always add all aggregations
        	sr.aggregations("type", QueryFactoryUtils.termAggregation("type", 2));
        	sr.aggregations("status", QueryFactoryUtils.termAggregation("status", 6));
        	sr.aggregations("format", QueryFactoryUtils.termAggregation("format", 10));
        	sr.aggregations("organization", QueryFactoryUtils.termAggregation("organization.keyword", 1000));
        	sr.aggregations("isReferenced", QueryFactoryUtils.termAggregation("isReferenced.keyword", 2));        	
        }          
        var srFinal = sr.build();
        logPayload(srFinal);
        return srFinal;
	}

}
