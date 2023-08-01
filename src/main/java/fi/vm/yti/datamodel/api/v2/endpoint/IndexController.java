package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("v2/index")
@Tag(name = "Index" )
public class IndexController {

    private final OpenSearchIndexer indexer;

    public IndexController(OpenSearchIndexer indexer) {
        this.indexer = indexer;
    }

    @Operation(summary = "Reindex all datamodels")
    @PostMapping(value = "/reindex")
    public void reIndex(@RequestParam(required = false) String index) {
        indexer.reindex(index);
    }
}
