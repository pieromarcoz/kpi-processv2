package pe.farmaciasperuanas.digital.process.kpi.application.service;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.service.PaidMediaKpiCalculatorService;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Job programado para ejecutar el cálculo de KPIs de medios pagados
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaidMediaKpiCalculatorJob {

    private final PaidMediaKpiCalculatorService paidMediaKpiCalculatorService;
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Ejecuta el cálculo de métricas para medios pagados diariamente a las 3:00 AM
     * como se especifica en los detalles técnicos de la historia de usuario
     */
    @Scheduled(cron = "0 0 3 * * ?")
    public void calculatePaidMediaKpis() {
        LocalDateTime startTime = LocalDateTime.now();
        log.info("Iniciando job de cálculo de KPIs de medios pagados: {}",
                startTime.format(DATE_TIME_FORMATTER));

        paidMediaKpiCalculatorService.calculateAllPaidMediaKpis()
                .subscribe(
                        count -> {
                            LocalDateTime endTime = LocalDateTime.now();
                            log.info("Job de cálculo de KPIs de medios pagados completado: {} métricas calculadas en {} segundos",
                                    count,
                                    endTime.toEpochSecond(java.time.ZoneOffset.UTC) - startTime.toEpochSecond(java.time.ZoneOffset.UTC));
                        },
                        error -> {
                            log.error("Error en job de cálculo de KPIs de medios pagados: {}", error.getMessage(), error);
                            // Aquí se podría implementar la lógica de reintento o notificación
                            // como se especifica en los detalles técnicos (punto 1b y 1c)
                        }
                );
    }

    /**
     * Este método puede ser llamado manualmente para forzar el cálculo de KPIs
     * útil para pruebas o recalcular métricas bajo demanda
     */
    public void calculatePaidMediaKpisManually() {
        log.info("Iniciando cálculo manual de KPIs de medios pagados");
        calculatePaidMediaKpis();
    }
}