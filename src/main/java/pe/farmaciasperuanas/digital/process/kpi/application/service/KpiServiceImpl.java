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
        return generateAllMetrics()
                .then(generateKpiImpressions())
                .then(generateKpiScope())
//                .then(processImpressionsParent())
//                .then(processShippingScopeParent())
//                .then(processClicksParent())
//                .then(processImpressionsPushParent())
//                .then(processShippingScopePushParent())
//                .then(processSalesParent())
//                .then(processTransactionsParent())
//                .then(processSessionsParent())
//                .then(processSalesPushParent())
//                .then(processTransactionsPushParent())
//                .then(processSessionsPushParent())
//                .then(processSalesByFormat())
//                .then(processTransactionsByFormat())
//                .then(processSessionsByFormat())
//                .then(processClicksByFormat())
//                .then(processOpenRateParent())
//                .then(processCrParent())
//                .then(processClickRateByFormat())
//                .then(processOpenRatePushParent())
//                .then(processRoasGeneral())
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

//    public Mono<Void> processClicksParent() {
//        return Flux.defer(() -> kpiRepository.generateKpiClicksParents())
//                .doOnNext(kpi -> System.out.println("Procesando KPI ClicksParent: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processImpressionsPushParent() {
//        return Flux.defer(() -> kpiRepository.generateKpiImpressionsPushParents())
//                .doOnNext(kpi -> System.out.println("Procesando KPI ImpressionsPushParent: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processShippingScopePushParent() {
//        return Flux.defer(() -> kpiRepository.generateKpiShippingScopePushParents())
//                .doOnNext(kpi -> System.out.println("Procesando KPI ShippingScopePushParent: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processSalesParent() {
//        return Flux.defer(() -> kpiRepository.generateKpiSalesParents())
//                .doOnNext(kpi -> System.out.println("Procesando KPI SalesParent: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processTransactionsParent() {
//        return Flux.defer(() -> kpiRepository.generateKpiTransactionsParents())
//                .doOnNext(kpi -> System.out.println("Procesando KPI TransactionsParent: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processSessionsParent() {
//        return Flux.defer(() -> kpiRepository.generateKpiSessionsParents())
//                .doOnNext(kpi -> System.out.println("Procesando KPI SessionsParent: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processSalesPushParent() {
//        return Flux.defer(() -> kpiRepository.generateKpiSalesPushParents())
//                .doOnNext(kpi -> System.out.println("Procesando KPI SalesPushParent: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processTransactionsPushParent() {
//        return Flux.defer(() -> kpiRepository.generateKpiTransactionsPushParents())
//                .doOnNext(kpi -> System.out.println("Procesando KPI TransactionsPushParent: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processSessionsPushParent() {
//        return Flux.defer(() -> kpiRepository.generateKpiSessionsPushParents())
//                .doOnNext(kpi -> System.out.println("Procesando KPI SessionsPushParent: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processSalesByFormat() {
//        return Flux.defer(() -> kpiRepository.generateKpiSalesByFormat())
//                .doOnNext(kpi -> System.out.println("Procesando KPI SalesByFormat: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processTransactionsByFormat() {
//        return Flux.defer(() -> kpiRepository.generateKpiTransactionsByFormat())
//                .doOnNext(kpi -> System.out.println("Procesando KPI TransactionsByFormat: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processSessionsByFormat() {
//        return Flux.defer(() -> kpiRepository.generateKpiSessionsByFormat())
//                .doOnNext(kpi -> System.out.println("Procesando KPI SessionsByFormat: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processClicksByFormat() {
//        return Flux.defer(() -> kpiRepository.generateKpiClicksByFormat())
//                .doOnNext(kpi -> System.out.println("Procesando KPI ClicksByFormat: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processOpenRateParent() {
//        return Flux.defer(() -> kpiRepository.generateKpiOpenRateParents())
//                .doOnNext(kpi -> System.out.println("Procesando KPI OpenRateParent: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processCrParent() {
//        return Flux.defer(() -> kpiRepository.generateKpiCRParents())
//                .doOnNext(kpi -> System.out.println("Procesando KPI CRParent: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processClickRateByFormat() {
//        return Flux.defer(() -> kpiRepository.generateKpiClickRateByFormat())
//                .doOnNext(kpi -> System.out.println("Procesando KPI ClickRateByFormat: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processOpenRatePushParent() {
//        return Flux.defer(() -> kpiRepository.generateKpiOpenRatePushParents())
//                .doOnNext(kpi -> System.out.println("Procesando KPI OpenRatePushParent: " + kpi))
//                .then();
//    }
//
//    public Mono<Void> processRoasGeneral() {
//        return Flux.defer(() -> kpiRepository.generateKpiRoasGeneral())
//                .doOnNext(kpi -> System.out.println("Procesando KPI RoasGeneral: " + kpi))
//                .then();
//    }

//    public Mono<Void> processMetricsGeneral() {
//        return Flux.defer(() -> kpiRepository.generateMetricsGeneral())
//                .doOnNext(metrics -> System.out.println("Procesando Metrics General: " + metrics))
//                .then();
//    }
//
    public Mono<Void> generateAllMetrics() {
        return Mono.defer(() -> kpiRepository.generateAllMetrics())
                .doOnSuccess(unused -> System.out.println("Procesamiento completo de todas las métricas"))
                .onErrorResume(error -> {
                    System.err.println("Error al procesar métricas: " + error.getMessage());
                    return Mono.empty();
                });
    }
}