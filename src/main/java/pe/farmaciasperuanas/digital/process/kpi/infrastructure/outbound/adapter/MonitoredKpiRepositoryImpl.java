package pe.farmaciasperuanas.digital.process.kpi.infrastructure.outbound.adapter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.stereotype.Repository;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Metrics;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.KpiRepository;
import pe.farmaciasperuanas.digital.process.kpi.infrastructure.config.PerformanceMonitor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Implementación decorada del repositorio KPI que monitorea el rendimiento
 * de los métodos críticos utilizando PerformanceMonitor.
 *
 * Esta clase aplica el patrón Decorator para añadir monitores de rendimiento
 * sin modificar la implementación original.
 *
 * <b>Copyright</b>: &copy; 2025 Digital.<br/>
 * <b>Company</b>: Digital.<br/>
 */
@Repository
@Primary  // Para que Spring use esta implementación en lugar de KpiRepositoryImpl
@Slf4j
public class MonitoredKpiRepositoryImpl implements KpiRepository {

    private final KpiRepositoryImpl kpiRepository;
    private final PerformanceMonitor performanceMonitor;

    @Autowired
    public MonitoredKpiRepositoryImpl(
            KpiRepositoryImpl kpiRepository,
            PerformanceMonitor performanceMonitor,
            ReactiveMongoTemplate reactiveMongoTemplate
    ) {
        this.kpiRepository = kpiRepository;
        this.performanceMonitor = performanceMonitor;

        // Inyectar manualmente en KpiRepositoryImpl si es necesario
        // (Esto es opcional, depende de cómo esté configurada la inyección de dependencias)
        // ReflectionUtils.setField(
        //     ReflectionUtils.findField(KpiRepositoryImpl.class, "reactiveMongoTemplate"),
        //     kpiRepository,
        //     reactiveMongoTemplate
        // );
    }

    @Override
    public Flux<Kpi> generateKpiImpressionsParents() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiImpressionsParents",
                () -> kpiRepository.generateKpiImpressionsParents()
        );
    }

    @Override
    public Flux<Kpi> generateKpiShippingScopeParents() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiShippingScopeParents",
                () -> kpiRepository.generateKpiShippingScopeParents()
        );
    }

    @Override
    public Flux<Kpi> generateKpiClicksParents() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiClicksParents",
                () -> kpiRepository.generateKpiClicksParents()
        );
    }

    @Override
    public Flux<Kpi> generateKpiImpressionsPushParents() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiImpressionsPushParents",
                () -> kpiRepository.generateKpiImpressionsPushParents()
        );
    }

    @Override
    public Flux<Kpi> generateKpiShippingScopePushParents() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiShippingScopePushParents",
                () -> kpiRepository.generateKpiShippingScopePushParents()
        );
    }

    @Override
    public Flux<Kpi> generateKpiSalesParents() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiSalesParents",
                () -> kpiRepository.generateKpiSalesParents()
        );
    }

    @Override
    public Flux<Kpi> generateKpiTransactionsParents() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiTransactionsParents",
                () -> kpiRepository.generateKpiTransactionsParents()
        );
    }

    @Override
    public Flux<Kpi> generateKpiSessionsParents() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiSessionsParents",
                () -> kpiRepository.generateKpiSessionsParents()
        );
    }

    @Override
    public Flux<Kpi> generateKpiSalesPushParents() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiSalesPushParents",
                () -> kpiRepository.generateKpiSalesPushParents()
        );
    }

    @Override
    public Flux<Kpi> generateKpiTransactionsPushParents() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiTransactionsPushParents",
                () -> kpiRepository.generateKpiTransactionsPushParents()
        );
    }

    @Override
    public Flux<Kpi> generateKpiSessionsPushParents() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiSessionsPushParents",
                () -> kpiRepository.generateKpiSessionsPushParents()
        );
    }

    @Override
    public Flux<Kpi> generateKpiClicksByFormat() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiClicksByFormat",
                () -> kpiRepository.generateKpiClicksByFormat()
        );
    }

    @Override
    public Flux<Kpi> generateKpiSalesByFormat() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiSalesByFormat",
                () -> kpiRepository.generateKpiSalesByFormat()
        );
    }

    @Override
    public Flux<Kpi> generateKpiTransactionsByFormat() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiTransactionsByFormat",
                () -> kpiRepository.generateKpiTransactionsByFormat()
        );
    }

    @Override
    public Flux<Kpi> generateKpiSessionsByFormat() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiSessionsByFormat",
                () -> kpiRepository.generateKpiSessionsByFormat()
        );
    }

    @Override
    public Flux<Kpi> generateKpiOpenRateParents() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiOpenRateParents",
                () -> kpiRepository.generateKpiOpenRateParents()
        );
    }

    @Override
    public Flux<Kpi> generateKpiCRParents() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiCRParents",
                () -> kpiRepository.generateKpiCRParents()
        );
    }

    @Override
    public Flux<Kpi> generateKpiClickRateByFormat() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiClickRateByFormat",
                () -> kpiRepository.generateKpiClickRateByFormat()
        );
    }

    @Override
    public Flux<Kpi> generateKpiOpenRatePushParents() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiOpenRatePushParents",
                () -> kpiRepository.generateKpiOpenRatePushParents()
        );
    }

    @Override
    public Flux<Kpi> generateKpiRoasGeneral() {
        return performanceMonitor.monitor(
                "KpiRepository.generateKpiRoasGeneral",
                () -> kpiRepository.generateKpiRoasGeneral()
        );
    }

    @Override
    public Flux<Metrics> generateMetricsGeneral() {
        return performanceMonitor.monitor(
                "KpiRepository.generateMetricsGeneral",
                () -> kpiRepository.generateMetricsGeneral()
        );
    }

    // Método adicional para monitorear el método generateAllMetrics
    public Mono<Void> generateAllMetrics() {
        return performanceMonitor.monitor(
                "KpiRepository.generateAllMetrics",
                () -> kpiRepository.generateAllMetrics()
        );
    }
}