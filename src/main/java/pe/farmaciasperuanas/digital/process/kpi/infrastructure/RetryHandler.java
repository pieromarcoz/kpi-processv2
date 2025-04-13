package pe.farmaciasperuanas.digital.process.kpi.infrastructure;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Utilidad para manejar reintentos con backoff exponencial
 * según se especifica en los requisitos de manejo de errores
 */
@Slf4j
public class RetryHandler {

    /**
     * Ejecuta una operación con reintentos en caso de fallo
     * @param operation Operación a ejecutar
     * @param operationName Nombre descriptivo de la operación (para logs)
     * @param maxRetries Número máximo de reintentos
     * @param initialBackoff Tiempo inicial de espera entre reintentos (en milisegundos)
     * @param <T> Tipo de resultado de la operación
     * @return Un Mono que emite el resultado de la operación
     */
    public static <T> Mono<T> withRetry(
            Supplier<Mono<T>> operation,
            String operationName,
            int maxRetries,
            long initialBackoff) {

        return Mono.defer(operation)
                .retryWhen(Retry.backoff(maxRetries, Duration.ofMillis(initialBackoff))
                        .doBeforeRetry(retrySignal -> {
                            long attempt = retrySignal.totalRetries() + 1;

                            log.warn("Reintento {} de {} para operación {}: {}",
                                    attempt, maxRetries, operationName,
                                    retrySignal.failure().getMessage());
                        })
                        .onRetryExhaustedThrow((spec, signal) -> {
                            log.error("Reintentos agotados para operación {}: {}",
                                    operationName, signal.failure().getMessage());
                            return signal.failure();
                        })
                );
    }

    /**
     * Ejecuta una operación con reintentos usando configuración predeterminada
     * (3 reintentos con backoff exponencial iniciando en 1 segundo)
     * @param operation Operación a ejecutar
     * @param operationName Nombre descriptivo de la operación
     * @param <T> Tipo de resultado de la operación
     * @return Un Mono que emite el resultado de la operación
     */
    public static <T> Mono<T> withDefaultRetry(
            Supplier<Mono<T>> operation,
            String operationName) {
        return withRetry(operation, operationName, 3, 1000);
    }
}