package pe.farmaciasperuanas.digital.process.kpi.infrastructure.inbound.controller;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import pe.farmaciasperuanas.digital.process.kpi.application.service.KpiPaidMediaServiceImpl;
import pe.farmaciasperuanas.digital.process.kpi.application.service.KpiServiceImpl;
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

    @Autowired
    private KpiPaidMediaServiceImpl kpiPaidMediaService;
    @PostMapping("/calculate")
    public Mono<Map<String, Object>> generatekpiPaidMedia() {
        return kpiPaidMediaService.generateKpiFromPaidMedia();

    }

    /**
     * Endpoint para forzar la ejecución del job programado
     * @return Respuesta con información sobre el inicio del job
     */

}