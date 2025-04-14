package pe.farmaciasperuanas.digital.process.kpi.domain.port.repository;

import org.springframework.stereotype.Repository;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Metrics;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Interface for running Spring Boot framework.<br/>
 * <b>Class</b>: Application<br/>
 * <b>Copyright</b>: &copy; 2025 Digital.<br/>
 * <b>Company</b>: Digital.<br/>
 *

 * <u>Developed by</u>: <br/>
 * <ul>
 * <li>Jorge Triana</li>
 * </ul>
 * <u>Changes</u>:<br/>
 * <ul>
 * <li>Feb 28, 2025 KpiRepository Interface.</li>
 * </ul>
 * @version 1.0
 */

@Repository
public interface KpiRepository {
    Flux<Kpi> generateKpiImpressions();
    Flux<Kpi> generateKpiScope();
    Flux<Kpi> generateKpiClicksParents();
    Flux<Kpi> generateKpiClicksByFormat();
    Flux<Kpi> generateKpiRates();
    Flux<Kpi> generateKpiClickRatesByFormat();
    Flux<Kpi> generateKpiPushAppOpenRate();
//    Flux<Kpi> generateKpiImpressionsPushParents();
//    Flux<Kpi> generateKpiShippingScopePushParents();
//    Flux<Kpi> generateKpiClicksParents();
//    Flux<Kpi> generateKpiSalesParents();
//    Flux<Kpi> generateKpiTransactionsParents();
//    Flux<Kpi> generateKpiSessionsParents();
//    Flux<Kpi> generateKpiSalesPushParents();
//    Flux<Kpi> generateKpiTransactionsPushParents();
//    Flux<Kpi> generateKpiSessionsPushParents();
//    Flux<Kpi> generateKpiClicksByFormat();
//    Flux<Kpi> generateKpiSalesByFormat();
//    Flux<Kpi> generateKpiTransactionsByFormat();
//    Flux<Kpi> generateKpiSessionsByFormat();
//    Flux<Kpi> generateKpiOpenRateParents();
//    Flux<Kpi> generateKpiCRParents();
//    Flux<Kpi> generateKpiClickRateByFormat();
//    Flux<Kpi> generateKpiOpenRatePushParents();
//    Flux<Kpi> generateKpiRoasGeneral();
      Flux<Metrics> generateMetricsGeneral();
}
