package pe.farmaciasperuanas.digital.process.kpi.infrastructure.inbound.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import pe.farmaciasperuanas.digital.process.kpi.infrastructure.config.PerformanceMonitor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Controlador para acceder a métricas de rendimiento del servicio KPI
 */
@RestController
@RequestMapping("/api/kpi/metrics")
@Slf4j
@RequiredArgsConstructor
public class KpiPerformanceController {

    private final PerformanceMonitor performanceMonitor;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Obtiene estadísticas de rendimiento de todos los métodos monitoreados
     */
    @GetMapping("/performance")
    public ResponseEntity<Map<String, Object>> getPerformanceStatistics() {
        log.info("Solicitando estadísticas de rendimiento");

        Map<String, Object> response = new HashMap<>();
        response.put("timestamp", LocalDateTime.now().format(DATE_FORMATTER));
        response.put("statistics", performanceMonitor.getPerformanceStatistics());

        return ResponseEntity.ok(response);
    }

    /**
     * Resetea todas las estadísticas de rendimiento
     */
    @PostMapping("/performance/reset")
    public ResponseEntity<Map<String, Object>> resetStatistics() {
        log.info("Reseteando estadísticas de rendimiento");

        performanceMonitor.resetStatistics();

        Map<String, Object> response = new HashMap<>();
        response.put("timestamp", LocalDateTime.now().format(DATE_FORMATTER));
        response.put("message", "Estadísticas de rendimiento reseteadas correctamente");

        return ResponseEntity.ok(response);
    }
}