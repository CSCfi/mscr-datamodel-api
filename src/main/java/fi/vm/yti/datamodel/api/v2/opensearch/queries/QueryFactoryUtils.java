package fi.vm.yti.datamodel.api.v2.opensearch.queries;


import fi.vm.yti.datamodel.api.v2.dto.Status;
import org.opensearch.client.opensearch._types.FieldValue;
import org.opensearch.client.opensearch._types.query_dsl.*;

import java.util.Collection;

public class QueryFactoryUtils {

    private QueryFactoryUtils(){
        //Utility class
    }

    public static final int DEFAULT_PAGE_FROM = 0;
    public static final int DEFAULT_PAGE_SIZE = 10;
    public static final int INTERNAL_SEARCH_PAGE_SIZE = 10000;
    public static final String DEFAULT_SORT_LANG = "fi";

    public static int pageFrom(Integer pageFrom){
        if(pageFrom == null || pageFrom <= 0){
            return DEFAULT_PAGE_FROM;
        }else{
            return  pageFrom;
        }
    }

    public static int pageSize(Integer pageSize) {
        if(pageSize == null || pageSize <= 0){
            return DEFAULT_PAGE_SIZE;
        }else{
            return pageSize;
        }
    }

    //COMMON QUERIES

    public static Query hideIncompleteStatusQuery(){
        var termQuery = TermQuery.of(q -> q
                .field("status")
                .value(FieldValue.of(Status.INCOMPLETE.name()))
                )._toQuery();
        return BoolQuery.of(q -> q.mustNot(termQuery))._toQuery();
    }

    public static Query termsQuery(String field, Collection<String> values){
        return TermsQuery.of(q -> q
                .field(field)
                .terms(t -> t
                        .value(values.stream().map(FieldValue::of).toList())))
                ._toQuery();
    }

    public static Query termQuery(String field, String value){
        if(value == null || value.isBlank()){
            return null;
        }
        return TermQuery.of(q -> q
                        .field(field)
                        .value(FieldValue.of(value)))
                ._toQuery();
    }

    public static Query labelQuery(String query) {
        return QueryStringQuery.of(q -> q
                .query("*" + query + "*")
                .fields("label.*")
                .fuzziness("2")
                )._toQuery();
    }

}
