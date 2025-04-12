package pe.farmaciasperuanas.digital.process.kpi.infrastructure.inbound.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import pe.farmaciasperuanas.digital.process.kpi.application.service.KpiCoverageValidator;
import pe.farmaciasperuanas.digital.process.kpi.application.service.KpiServiceImpl;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Controlador REST para validaciones y diagnósticos de KPIs
 * Proporciona endpoints para verificar la cobertura y calidad de los KPIs
 */
@RestController
@RequestMapping("/api/kpi/validation")
@Slf4j
@RequiredArgsConstructor
public class KpiValidationController {

    private final KpiCoverageValidator kpiCoverageValidator;
    private final KpiServiceImpl kpiService;

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Endpoint para verificar la cobertura de KPIs
     * Verifica si todos los KPIs necesarios están generados para cada campaña activa
     */
    @PostMapping("/coverage")
    public Mono<ResponseEntity<Map<String, Object>>> checkKpiCoverage() {
        log.info("Solicitud de verificación manual de cobertura de KPIs");
        LocalDateTime startTime = LocalDateTime.now();

        return kpiCoverageValidator.verifyKpiCoverage()
                .then(Mono.just(ResponseEntity.ok(Map.of(
                        "status", "success",
                        "message", "Verificación de cobertura de KPIs completada",
                        "startTime", startTime.format(DATE_FORMATTER),
                        "endTime", LocalDateTime.now().format(DATE_FORMATTER),
                        "note", "Revise los logs para ver resultados detallados de la verificación"
                ))));
    }

    /**
     * Endpoint para verificar valores nulos o cero en KPIs
     * Útil para identificar problemas de calidad en los datos
     */
    @GetMapping("/empty-values")
    public Mono<ResponseEntity<Map<String, Object>>> checkEmptyValues() {
        log.info("Solicitud de verificación de valores nulos o cero en KPIs");

        return kpiCoverageValidator.checkForEmptyValues()
                .map(results -> {
                    Map<String, Object> response = new HashMap<>();
                    response.put("status", "success");
                    response.put("message", "Verificación de valores completada");
                    response.put("timestamp", LocalDateTime.now().format(DATE_FORMATTER));
                    response.put("results", results);

                    return ResponseEntity.ok(response);
                });
    }

    /**
     * Endpoint para regenerar todos los KPIs
     * Útil para forzar un recálculo completo
     */
    @PostMapping("/regenerate")
    public Mono<ResponseEntity<Map<String, Object>>> regenerateAllKpis() {
        log.info("Solicitud de regeneración manual de todos los KPIs");
        LocalDateTime startTime = LocalDateTime.now();

        return kpiService.generateKpiFromSalesforceData()
                .map(result -> {
                    Map<String, Object> response = new HashMap<>();
                    response.put("status", "success");
                    response.put("message", "Regeneración de KPIs completada");
                    response.put("startTime", startTime.format(DATE_FORMATTER));
                    response.put("endTime", LocalDateTime.now().format(DATE_FORMATTER));
                    response.put("serviceResponse", result);

                    return ResponseEntity.ok(response);
                });
    }

    /**
     * Endpoint para obtener estadísticas de KPIs por campaña
     */
    @GetMapping("/stats/{campaignId}")
    public Mono<ResponseEntity<Map<String, Object>>> getKpiStats(@PathVariable String campaignId) {
        log.info("Solicitud de estadísticas de KPIs para campaña: {}", campaignId);

        // Esta implementación es un placeholder - en un caso real consultaríamos
        // la base de datos para obtener estadísticas reales
        return Mono.just(ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Estadísticas de KPIs para campaña: " + campaignId,
                "campaignId", campaignId,
                "timestamp", LocalDateTime.now().format(DATE_FORMATTER),
                "stats", Map.of(
                        "totalKpis", 24,
                        "lastUpdated", LocalDateTime.now().minusHours(3).format(DATE_FORMATTER),
                        "batchStatus", "COMPLETE"
                )
        )));
    }

    /**
     * Endpoint para consultar el estado de salud del servicio
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "kpi-validation",
                "timestamp", LocalDateTime.now().format(DATE_FORMATTER)
        ));
    }
}