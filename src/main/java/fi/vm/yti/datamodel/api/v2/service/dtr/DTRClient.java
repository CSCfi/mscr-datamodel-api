package fi.vm.yti.datamodel.api.v2.service.dtr;

import java.net.HttpURLConnection;
import java.net.URL;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;

import fi.vm.yti.datamodel.api.v2.utils.RestUtils;

@Service
public class DTRClient {

	@Value("${dtr.typeAPIEndpoint}")
	private String typeAPIEndpoint;

	@Value("${dtr.typesense.collectionURL}")
	private String typeSenseCollectionURL;

	@Value("${dtr.typesense.key}")
	private String typeSenseKey;

	
	private String doGet(String urlString, String keyHeader, String keyValue) throws Exception {
		URL url = new URL(urlString);
		
		HttpURLConnection con = null;
		try {
			con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");
			con.addRequestProperty("Content-type", "application/json");
			if(keyHeader != null && keyValue != null) {
				con.addRequestProperty(keyHeader, keyValue);
			}
			int status = con.getResponseCode();
			if (status != 200) {
				throw new Exception("Could fetch a DTR type using url " + url);
			}
			String content = RestUtils.readContent(con.getInputStream());
			return content;
		} catch (Exception ex) {
			throw ex;
		} finally {
			if (con != null) {
				con.disconnect();
			}
		}
		
	}
	public String getTypeAsJSONSchema(String pid) throws Exception {
		return doGet(this.typeAPIEndpoint + pid, null, null);
	}
	
	public String searchTypes(String queryBy, String query, String filterBy, int page, int pageSize) throws Exception {
		return doGet(
				UriComponentsBuilder.fromUriString(typeSenseCollectionURL + "/documents/search")
				.queryParam("q", query)
				.queryParam("query_by", queryBy)
				.queryParam("filter_by",  filterBy)
				.queryParam("page", page)
				.queryParam("per_page", pageSize)
				.build()
				.toString(),
				"X-TYPESENSE-API-KEY",
				typeSenseKey);
		
	}
}
