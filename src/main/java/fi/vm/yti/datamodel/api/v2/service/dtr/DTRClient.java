package fi.vm.yti.datamodel.api.v2.service.dtr;

import java.net.HttpURLConnection;
import java.net.URL;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import fi.vm.yti.datamodel.api.v2.utils.RestUtils;

@Service
public class DTRClient {

	@Value("${dtr.typeAPIEndpoint}")
	private String typeAPIEndpoint;
	
	public String getTypeAsJSONSchema(String pid) throws Exception {
		URL url = new URL(this.typeAPIEndpoint + pid);
		HttpURLConnection con = null;
		try {
			con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");
			con.addRequestProperty("Content-type", "application/json");  			
			int status = con.getResponseCode();
			if (status != 200) {
				throw new Exception("Could not mint PID using url " + url);
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
}
