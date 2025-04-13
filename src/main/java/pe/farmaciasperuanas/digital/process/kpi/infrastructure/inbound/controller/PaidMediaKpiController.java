package pe.farmaciasperuanas.digital.process.kpi.infrastructure.inbound.controller;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import pe.farmaciasperuanas.digital.process.kpi.application.service.PaidMediaKpiCalculatorJob;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.service.PaidMediaKpiCalculatorService;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Controlador REST para operaciones relacionadas con KPIs de medios pagados
 */
@RestController
@RequestMapping("/api/paid-media/kpis")
@Slf4j
@RequiredArgsConstructor
public class PaidMediaKpiController {

    private final PaidMediaKpiCalculatorService paidMediaKpiCalculatorService;
    private final PaidMediaKpiCalculatorJob paidMediaKpiCalculatorJob;

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Endpoint para calcular manualmente los KPIs de medios pagados
     * @return Respuesta con información sobre el proceso
     */
    @PostMapping("/calculate")
    public Mono<ResponseEntity<Map<String, Object>>> calculateKpis() {
        log.info("Solicitud manual de cálculo de KPIs de medios pagados");

        LocalDateTime startTime = LocalDateTime.now();

        return paidMediaKpiCalculatorService.calculateAllPaidMediaKpis()
                .map(count -> {
                    LocalDateTime endTime = LocalDateTime.now();
                    long durationSeconds = java.time.Duration.between(startTime, endTime).getSeconds();

                    Map<String, Object> response = new HashMap<>();
                    response.put("status", "success");
                    response.put("message", "Cálculo de KPIs completado");
                    response.put("kpisCalculated", count);
                    response.put("startTime", startTime.format(DATE_TIME_FORMATTER));
                    response.put("endTime", endTime.format(DATE_TIME_FORMATTER));
                    response.put("durationSeconds", durationSeconds);

                    log.info("Cálculo manual completado: {} KPIs en {} segundos", count, durationSeconds);

                    return ResponseEntity.ok(response);
                })
                .onErrorResume(error -> {
                    log.error("Error en cálculo manual de KPIs: {}", error.getMessage(), error);

                    Map<String, Object> errorResponse = new HashMap<>();
                    errorResponse.put("status", "error");
                    errorResponse.put("message", "Error en cálculo de KPIs");
                    errorResponse.put("error", error.getMessage());

                    return Mono.just(ResponseEntity.internalServerError().body(errorResponse));
                });
    }

    /**
     * Endpoint para forzar la ejecución del job programado
     * @return Respuesta con información sobre el inicio del job
     */
    @PostMapping("/job/execute")
    public ResponseEntity<Map<String, Object>> forceJobExecution() {
        log.info("Solicitando ejecución forzada del job de cálculo de KPIs");

        // Ejecutar el job en un hilo separado para no bloquear la respuesta
        new Thread(() -> {
            try {
                paidMediaKpiCalculatorJob.calculatePaidMediaKpisManually();
            } catch (Exception e) {
                log.error("Error al ejecutar job manualmente: {}", e.getMessage(), e);
            }
        }).start();

        Map<String, Object> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", "Job de cálculo de KPIs iniciado");
        response.put("startTime", LocalDateTime.now().format(DATE_TIME_FORMATTER));
        response.put("note", "El proceso se ejecuta de forma asíncrona. Consulte los logs para ver el resultado.");

        return ResponseEntity.accepted().body(response);
    }

    /**
     * Endpoint para verificar el estado del servicio
     * @return Estado del servicio
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> status = new HashMap<>();
        status.put("service", "paid-media-kpi-calculator");
        status.put("status", "UP");
        status.put("timestamp", LocalDateTime.now().format(DATE_TIME_FORMATTER));

        return ResponseEntity.ok(status);
    }
}