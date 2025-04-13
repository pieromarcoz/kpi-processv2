package pe.farmaciasperuanas.digital.process.kpi.infrastructure.config.mongo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import pe.farmaciasperuanas.digital.process.kpi.domain.DTO.SecretDto;

import java.io.UnsupportedEncodingException;
import java.util.Base64;

@Configuration
@Slf4j
public class MongoConfig {

    private String database;
    private String uri;

    @Value("${route-secret-endpoint}")
    private String route;

    @Value("${route-secret}")
    private String secretId;

    @Value("${app-key}")
    private String appKey;

    @PostConstruct
    public void init() throws JsonProcessingException, UnsupportedEncodingException {
        log.info("Initializing MongoDB configuration with Secret Manager");
        log.info("Secret Manager endpoint: {}", route);

        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.set("app-key", appKey);

        HttpEntity<?> entity = new HttpEntity<>(headers);

        UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(route)
                .queryParam("secretId", secretId);

        log.debug("Requesting secret from: {}", builder.toUriString());

        ResponseEntity<String> response = restTemplate.exchange(builder.toUriString(), HttpMethod.GET, entity, String.class);
        String responseBody = response.getBody();

        SecretDto secretDto = desencriptar(responseBody);

        this.uri = secretDto.getUri();
        this.database = secretDto.getDatabase();

        log.info("Successfully retrieved MongoDB credentials from Secret Manager");
        log.debug("Database: {}", database);
    }

    @Bean
    @Primary
    public MongoClient reactiveMongoClient() {
        return MongoClients.create(uri);
    }

    @Bean(name = "reactiveMongoTemplate")
    @Primary
    public ReactiveMongoTemplate reactiveMongoTemplate() {
        return new ReactiveMongoTemplate(reactiveMongoClient(), database);
    }

    private SecretDto desencriptar(String response) throws UnsupportedEncodingException, JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(response);
        String response1 = jsonNode.get("content").asText();

        byte[] decode = Base64.getDecoder().decode(response1.getBytes());
        String response2 = new String(decode, "utf-8");

        return objectMapper.readValue(response2, SecretDto.class);
    }
}