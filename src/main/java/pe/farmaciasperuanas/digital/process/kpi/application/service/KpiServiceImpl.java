package pe.farmaciasperuanas.digital.process.kpi.application.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.service.KpiService;
import pe.farmaciasperuanas.digital.process.kpi.infrastructure.outbound.adapter.KpiRepositoryImpl;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

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
 * <li>Feb 28, 2025 KpiServiceImpl class.</li>
 * </ul>
 * @version 1.0
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class KpiServiceImpl implements KpiService{

    @Autowired
    private KpiRepositoryImpl kpiRepository;

    @Override
    public Mono<Map<String, Object>> generateKpiFromSalesforceData() {
        return  generateKpiImpressions()
                .then(generateKpiScope())
                .then(generateKpiClicks())
                .then(generateKpiRates())
                .then(generateKpiDeliveredMailParent())
                .then(generateKpiSales())
                .then(generateKpiTransactions())
                .then(processSessions())
                .then(processRoasGeneral())
                .then(generateAllMetrics())
                .then(generatePushWebKpis())
                .then(Mono.just(new HashMap<String, Object>() {{
                    put("message", "Proceso completado con éxito");
                    put("status", "success");
                }}));
    }

    public Mono<Void> generateKpiImpressions() {
        return Flux.defer(() -> kpiRepository.generateKpiImpressions())
                .doOnNext(kpi -> System.out.println("Procesando KPI ImpressionsParent: " + kpi))
                .then();
    }

    public Mono<Void> generateKpiScope() {
        return Flux.defer(() -> kpiRepository.generateKpiScope())
                .doOnNext(kpi -> System.out.println("Procesando KPI generateKpiScope: " + kpi))
                .then();
    }

    public Mono<Void> generateKpiClicks() {
        log.info("Iniciando generación de KPIs de clicks");
        return Flux.defer(() -> kpiRepository.generateKpiClicks())
                .doOnNext(kpi -> log.info("KPI Clicks generado: {}", kpi))
                .doOnComplete(() -> log.info("Proceso de Clicks completado"))
                .doOnError(error -> log.error("Error en generateKpiClicks: {}", error.getMessage()))
                .then();
    }

    public Mono<Void> generateKpiRates() {
        log.info("Iniciando generación de KPIs de rates");
        return Flux.defer(() -> kpiRepository.generateKpiRates())
                .doOnNext(kpi -> log.info("KPI Rates generado: {}", kpi))
                .doOnComplete(() -> log.info("Proceso de Rates completado"))
                .doOnError(error -> log.error("Error en generateKpiRates: {}", error.getMessage()))
                .then();
    }

    public Mono<Void> generateKpiDeliveredMailParent() {
        return Flux.defer(() -> kpiRepository.generateKpiDeliveredMailParent())
                .doOnNext(kpi -> System.out.println("Procesando KPI generateKpiDeliveredMailParent: " + kpi))
                .then();
    }

    public Mono<Void> generateKpiSales() {
        return Flux.defer(() -> kpiRepository.generateKpiSales())
                .doOnNext(kpi -> System.out.println("Procesando KPI generateKpiSales: " + kpi))
                .then();
    }

    public Mono<Void> generateKpiTransactions() {
        return Flux.defer(() -> kpiRepository.generateKpiTransactions())
                .doOnNext(kpi -> System.out.println("Procesando KPI generateKpiTransactions: " + kpi))
                .then();
    }

    public Mono<Void> processSessions() {
        return Flux.defer(() -> kpiRepository.processSessions())
                .doOnNext(kpi -> System.out.println("Procesando KPI processSessions: " + kpi))
                .then();
    }

    public Mono<Void> processRoasGeneral() {
        return Flux.defer(() -> kpiRepository.generateKpiRoasGeneral())
                .doOnNext(kpi -> System.out.println("Procesando KPI RoasGeneral: " + kpi))
                .then();
    }
    public Mono<Void> generateAllMetrics() {
        return Mono.defer(() -> kpiRepository.generateAllMetrics())
                .doOnSuccess(unused -> System.out.println("Procesamiento completo de todas las métricas"))
                .onErrorResume(error -> {
                    System.err.println("Error al procesar métricas: " + error.getMessage());
                    return Mono.empty();
                });
    }
    public Mono<Void> generatePushWebKpis() {
        return Flux.defer(() -> kpiRepository.generatePushWebKpis())
                .doOnNext(kpi -> System.out.println("Procesando KPI generatePushWebKpis: " + kpi))
                .then();
    }
}