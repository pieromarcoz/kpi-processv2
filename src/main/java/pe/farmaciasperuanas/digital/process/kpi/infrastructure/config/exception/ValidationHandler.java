package pe.farmaciasperuanas.digital.process.kpi.infrastructure.config.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Implement class for running Spring Boot framework.<br/>
 * <b>Copyright</b>: &copy; 2025 Digital.<br/>
 * <b>Company</b>: Digital.<br/>
 *

 * <u>Developed by</u>: <br/>
 * <ul>
 * <li>Jorge Triana</li>
 * </ul>
 * <u>Changes</u>:<br/>
 * <ul>
 * <li>Feb 28, 2025 ValidationHandler class.</li>
 * </ul>
 * @version 1.0
 */

@Slf4j
@ControllerAdvice
public class ValidationHandler {

    @ExceptionHandler(Exception.class)
    public Mono<ResponseEntity<ErrorCustomResponse>> handleException(Exception e, ServerWebExchange exchange) {

        ErrorCustomResponse serverResponseError = new ErrorCustomResponse();
        serverResponseError.setCode(HttpStatus.INTERNAL_SERVER_ERROR.value() + "");
        serverResponseError.setMessage(HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase());
        serverResponseError.setDetail(e.getMessage());

        return Mono.just(ResponseEntity.internalServerError().body(serverResponseError));
    }
}
