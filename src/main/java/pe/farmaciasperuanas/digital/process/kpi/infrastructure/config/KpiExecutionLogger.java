package pe.farmaciasperuanas.digital.process.kpi.infrastructure.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Componente para registrar logs detallados de ejecución de KPIs
 * como se especifica en el punto 3.a de los requisitos
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class KpiExecutionLogger {

    private final ReactiveMongoTemplate mongoTemplate;

    // Contenedor para métricas de ejecución
    private Map<String, KpiExecutionMetrics> executionMetrics = new HashMap<>();

    /**
     * Registra el inicio de ejecución de un KPI
     * @param kpiId Identificador del KPI
     * @param campaignId Identificador de la campaña
     * @param format Formato de medios
     * @return El timestamp de inicio para calcular duración después
     */
    public LocalDateTime logExecutionStart(String kpiId, String campaignId, String format) {
        LocalDateTime startTime = LocalDateTime.now();
        String executionId = createExecutionId(kpiId, campaignId, format, startTime);

        log.info("Inicio de cálculo de KPI - id: {}, campaña: {}, formato: {}",
                kpiId, campaignId, format);

        // Inicializar métricas de ejecución
        KpiExecutionMetrics metrics = new KpiExecutionMetrics();
        metrics.setKpiId(kpiId);
        metrics.setCampaignId(campaignId);
        metrics.setFormat(format);
        metrics.setStartTime(startTime);
        metrics.setStatus("RUNNING");

        executionMetrics.put(executionId, metrics);

        return startTime;
    }

    /**
     * Registra la finalización exitosa de ejecución de un KPI
     * @param kpiId Identificador del KPI
     * @param campaignId Identificador de la campaña
     * @param format Formato de medios
     * @param startTime Timestamp de inicio para calcular duración
     * @param value Valor calculado
     */
    public void logExecutionSuccess(String kpiId, String campaignId, String format,
                                    LocalDateTime startTime, double value) {
        LocalDateTime endTime = LocalDateTime.now();
        String executionId = createExecutionId(kpiId, campaignId, format, startTime);

        Duration duration = Duration.between(startTime, endTime);

        log.info("Cálculo de KPI exitoso - id: {}, campaña: {}, formato: {}, valor: {}, duración: {} ms",
                kpiId, campaignId, format, value, duration.toMillis());

        if (executionMetrics.containsKey(executionId)) {
            KpiExecutionMetrics metrics = executionMetrics.get(executionId);
            metrics.setEndTime(endTime);
            metrics.setDurationMs(duration.toMillis());
            metrics.setValue(value);
            metrics.setStatus("SUCCESS");

            saveExecutionMetrics(metrics);
        }
    }

    /**
     * Registra el fallo en la ejecución de un KPI
     * @param kpiId Identificador del KPI
     * @param campaignId Identificador de la campaña
     * @param format Formato de medios
     * @param startTime Timestamp de inicio para calcular duración
     * @param error Error ocurrido
     */
    public void logExecutionError(String kpiId, String campaignId, String format,
                                  LocalDateTime startTime, Throwable error) {
        LocalDateTime endTime = LocalDateTime.now();
        String executionId = createExecutionId(kpiId, campaignId, format, startTime);

        Duration duration = Duration.between(startTime, endTime);

        log.error("Error en cálculo de KPI - id: {}, campaña: {}, formato: {}, error: {}, duración: {} ms",
                kpiId, campaignId, format, error.getMessage(), duration.toMillis());

        if (executionMetrics.containsKey(executionId)) {
            KpiExecutionMetrics metrics = executionMetrics.get(executionId);
            metrics.setEndTime(endTime);
            metrics.setDurationMs(duration.toMillis());
            metrics.setErrorMessage(error.getMessage());
            metrics.setStatus("ERROR");

            saveExecutionMetrics(metrics);
        }
    }

    /**
     * Guarda las métricas de ejecución en MongoDB para análisis posterior
     * @param metrics Métricas a guardar
     */
    private void saveExecutionMetrics(KpiExecutionMetrics metrics) {
        mongoTemplate.save(metrics, "kpi_execution_logs")
                .subscribe(
                        saved -> log.debug("Métricas de ejecución guardadas: {}", saved.getId()),
                        error -> log.error("Error al guardar métricas de ejecución: {}", error.getMessage())
                );
    }

    /**
     * Crea un ID único para la ejecución basado en los parámetros
     */
    private String createExecutionId(String kpiId, String campaignId, String format, LocalDateTime timestamp) {
        return String.format("%s_%s_%s_%s",
                kpiId,
                campaignId,
                format,
                timestamp.toString().replace(":", "-"));
    }

    /**
     * Clase interna para representar métricas de ejecución
     */
    @lombok.Data
    private static class KpiExecutionMetrics {
        private String id;
        private String kpiId;
        private String campaignId;
        private String format;
        private LocalDateTime startTime;
        private LocalDateTime endTime;
        private Long durationMs;
        private Double value;
        private String status;
        private String errorMessage;
    }
}