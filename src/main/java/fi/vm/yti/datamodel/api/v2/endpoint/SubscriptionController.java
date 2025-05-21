package fi.vm.yti.datamodel.api.v2.endpoint;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.search.Hit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClient.RequestHeadersSpec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;

import fi.vm.yti.datamodel.api.v2.dto.messaging.AddSubscription;
import fi.vm.yti.datamodel.api.v2.dto.messaging.SubscriptionResponse;
import fi.vm.yti.datamodel.api.v2.dto.messaging.UserInfo;
import fi.vm.yti.datamodel.api.v2.dto.messaging.DeleteSubscription;
import fi.vm.yti.datamodel.api.v2.dto.messaging.GetSubscription;
import fi.vm.yti.datamodel.api.v2.messaging.IntegrationContainerRequest;
import fi.vm.yti.datamodel.api.v2.messaging.IntegrationResourceDTO;
import fi.vm.yti.datamodel.api.v2.messaging.UpdatedResourcesResponseDTO;
import fi.vm.yti.datamodel.api.v2.messaging.Meta;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.opensearch.index.UpdatedResourceDTO;
import fi.vm.yti.datamodel.api.v2.opensearch.queries.ModelQueryFactory;
import fi.vm.yti.datamodel.api.v2.service.IntegrationService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

@RestController
@RequestMapping("v2/subscriptions")
@Tag(name = "Subscription" )
public class SubscriptionController {

	private final WebClient webClient;
	
	
    @Value("${env}")
    private String env;
    
    @Value("${fake.login.mail}")
    private String fakeLoginEmail;
    
    
	@Value(value = "${messaging.baseurl}")
	private String messageAPIUrl;
	
	@Autowired
	private ObjectMapper om;
	
	public SubscriptionController(WebClient.Builder webClientBuilder) {
		this.webClient = webClientBuilder.build();
	}	
	
    private void addSessionId(RequestHeadersSpec<?> r, Cookie[] cookies) {
		if(cookies != null) {
			for(int i = 0; i < cookies.length; i++ ) {
				Cookie c = cookies[i];
				if(c.getName().startsWith("_shibsession_")) {
					r.cookie(c.getName(), c.getValue());
				}
			}
			
		}
	}
    
	private String getMessagingAPIURL(String path) {
		String url = messageAPIUrl + path;
		if(env.equals("dev")) {
			url = url + "?fake.login.mail=" + fakeLoginEmail;
		}
		return url;
		
	}
    @PutMapping(path = "", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    ResponseEntity<SubscriptionResponse> add(@RequestBody AddSubscription action, HttpServletRequest request) throws Exception { 
		var r = webClient.post()
				.uri(getMessagingAPIURL("subscriptions"))
				.bodyValue(om.writeValueAsString(action))
				.accept(MediaType.ALL)
				.header("Authorization", request.getHeader("Authorization"))				
				.header("Content-Type", "application/json");
		addSessionId(r, request.getCookies());
				
		return r.retrieve()
				.toEntity(SubscriptionResponse.class).block();

    }

	@DeleteMapping(path = "", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    ResponseEntity<SubscriptionResponse> delete(@RequestBody DeleteSubscription action, HttpServletRequest request) throws Exception { 
		var r = webClient.post()
				.uri(getMessagingAPIURL("subscriptions"))
				.bodyValue(om.writeValueAsString(action))
				.accept(MediaType.ALL)
				.header("Authorization", request.getHeader("Authorization"))				
				.header("Content-Type", "application/json");
		addSessionId(r, request.getCookies());
				
		return r.retrieve()
				.toEntity(SubscriptionResponse.class).block();

    }    
    
    @PostMapping(path = "", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    ResponseEntity<SubscriptionResponse> get(@RequestBody GetSubscription action, HttpServletRequest request) throws Exception { 
		var r = webClient.post()
				.uri(getMessagingAPIURL("subscriptions"))
				.bodyValue(om.writeValueAsString(action))
				.accept(MediaType.ALL)
				.header("Authorization", request.getHeader("Authorization"))				
				.header("Content-Type", "application/json");
		addSessionId(r, request.getCookies());
				
		return r.retrieve()
				.toEntity(SubscriptionResponse.class).block();

    } 
    
    @GetMapping(path = "", produces = APPLICATION_JSON_VALUE)
    ResponseEntity<UserInfo> getUserInfo(HttpServletRequest request) throws Exception {
		var r = webClient.post()
				.uri(getMessagingAPIURL("user"))
				.accept(MediaType.ALL)
				.header("Authorization", request.getHeader("Authorization"))				
				.header("Content-Type", "application/json");
		addSessionId(r, request.getCookies());
				
		return r.retrieve()
				.toEntity(UserInfo.class).block();
    }       
 
}
