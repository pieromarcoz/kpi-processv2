package pe.farmaciasperuanas.digital.process.kpi.infrastructure.config.exception;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Component
@Order(2)
public class TransactionFilter implements WebFilter {

    @Value("${validation-custom.app-id-flag:0}")
    private String customValidationAppIdFlag;

    @Value("${validation-custom.app-id:12345}")
    private String customValidationAppId;

    @NotNull
    @Override
    public Mono<Void> filter(ServerWebExchange exchange, @NotNull WebFilterChain chain) {

        // Permitir solicitudes OPTIONS (preflight CORS)
        if (exchange.getRequest().getMethod() == HttpMethod.OPTIONS) {
            exchange.getResponse().setStatusCode(HttpStatus.OK);
            return exchange.getResponse().setComplete(); // 🔹 RESPONDE CON 200 OK
        }


        // Verificar si la solicitud es para Swagger
        if (isSwaggerRequest(exchange)) {
            return chain.filter(exchange);
        }

        // Solo validar app-id si la bandera está activada
        if (customValidationAppIdFlag.equalsIgnoreCase("1")) {
            final String appId = exchange.getRequest().getHeaders().getFirst("app-id");
            if (!validateAppId(appId)) {
                return generateErrorResponse(exchange);
            }
        }

        return chain.filter(exchange);
    }

    private boolean isSwaggerRequest(ServerWebExchange exchange) {
        // Verificar si la URI de la solicitud contiene "/swagger-ui" o "/v2/api-docs"
        String requestPath = exchange.getRequest().getPath().value();
        return requestPath.contains("/swagger-ui") || requestPath.contains("/v2/api-docs") || requestPath.contains("/v3/api-docs");
    }

    private boolean validateAppId(String appId) {
        // Realizar validación personalizada del app-id
        return appId != null && appId.equals(customValidationAppId);
    }

    private Mono<Void> generateErrorResponse(ServerWebExchange exchange) {
        ErrorCustomResponse errorCustomResponse = new ErrorCustomResponse();

        errorCustomResponse.setCode(HttpStatus.UNAUTHORIZED.value() + "");
        errorCustomResponse.setMessage(HttpStatus.UNAUTHORIZED.getReasonPhrase());
        errorCustomResponse.setDetail("The app-id is not valid.");

        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
        String jsonResponse = "";
        try {
            jsonResponse = convertObjectToJson(errorCustomResponse);
        } catch (JsonProcessingException e) {
            jsonResponse = "{\"error\": \"Error al procesar el objeto JSON\"}";
        }

        return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(jsonResponse.getBytes())));
    }

    private String convertObjectToJson(Object object) throws JsonProcessingException {
        if (object == null) {
            return null;
        }
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(object);
    }
}
