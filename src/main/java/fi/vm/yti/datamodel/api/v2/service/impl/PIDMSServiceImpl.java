package fi.vm.yti.datamodel.api.v2.service.impl;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.PIDType;
import fi.vm.yti.datamodel.api.v2.service.PIDService;
import fi.vm.yti.datamodel.api.v2.utils.RestUtils;

@Profile({"test", "prod"})
@Service
public class PIDMSServiceImpl implements PIDService {

	@Value("${pidms.apikey}")
	private String apikey;

	@Value("${pidms.prefix}")
	private String prefix;

	@Value("${pidms.url}")
	private String url;

	@Value("${pidms.mscrUrl}")
	private String mscrUrl;
	
	@Override
	public String mint(PIDType pidType, MSCRType contentType, String id) throws Exception {
		URL url = new URL(this.url + "/v1/pid");
		HttpURLConnection con = null;
		try {
			con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("POST");
			con.addRequestProperty("apikey", apikey);
			con.addRequestProperty("Content-type", "application/json");
			con.setDoOutput(true);
			String generatedUrl = mscrUrl + "/" + contentType.toString().toLowerCase() + "/" + id;
  			String body = "{ \"url\": \"" + generatedUrl + "\", \"type\": \"Handle\", \"persist\": \"0\"}";
  			
  			OutputStream os = con.getOutputStream();
  			OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");    
  			osw.write(body);
  			osw.flush();
  			osw.close();
  			os.close();
  			
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

	@Override
	public String mintPartIdentifier(String pid) {
		return pid + "@mapping=" + UUID.randomUUID();
	}

	@Override
	public String mapToInternal(String id) throws Exception {
		if (id.startsWith("mscr:")) {
			return id;
		} else if (id.startsWith(this.prefix)) {
			String pid = id;
			String partId = null;
			if(id.indexOf("@") > 0) {
				partId = id.substring(id.indexOf("@mapping=")+9);
				pid = id.substring(0, id.indexOf("@"));
			}
			// resolve to url - use either hdl.handle.net or PIDMS api
			URL url = new URL(this.url + "/get/v1/pid/" + URLEncoder.encode(pid));
			HttpURLConnection con = null;
			try {
				con = (HttpURLConnection) url.openConnection();
				con.setRequestMethod("GET");
				con.addRequestProperty("apikey", apikey);
				int status = con.getResponseCode();
				if (status != 200) {
					throw new Exception("Could not resolve URI using url " + url);
				}
				String content = RestUtils.readContent(con.getInputStream());
				String internalPID = content.substring(content.lastIndexOf("/") + 1);
				if(partId != null) {
					return internalPID + "@mapping=" + partId; 
				}
				else {
					return internalPID;
				}
			} catch (Exception ex) {
				throw ex;
			} finally {
				if (con != null) {
					con.disconnect();
				}
			}

		}
		throw new RuntimeException("Unknown identifier type");
	}
	


}
