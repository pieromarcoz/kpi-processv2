package pe.farmaciasperuanas.digital.process.kpi.infrastructure.inbound.controller;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
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


    @PostMapping("/calculate")
    public Mono<ResponseEntity<Map<String, Object>>> calculateKpis() {
     return null;

    }

    /**
     * Endpoint para forzar la ejecución del job programado
     * @return Respuesta con información sobre el inicio del job
     */

}