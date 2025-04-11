package pe.farmaciasperuanas.digital.process.kpi.domain.port.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Interfaz para el servicio de cálculo de KPIs de medios pagados
 */
@Service

public interface PaidMediaKpiCalculatorService {
    /**
     * Calcula todas las métricas de KPI para medios pagados
     * @return Un Mono que emite la cantidad total de KPIs calculados
     */
    Mono<Long> calculateAllPaidMediaKpis();
}