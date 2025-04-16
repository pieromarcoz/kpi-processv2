package pe.farmaciasperuanas.digital.process.kpi.domain.port.repository;

import org.springframework.stereotype.Repository;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
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
      Flux<Kpi> generatePushWebKpis();
      Flux<Kpi> generateKpiClicks();
      Flux<Kpi> generateKpiRates();
      Flux<Kpi> generateKpiDeliveredMailParent();
      Flux<Kpi> generateKpiSales();
      Flux<Kpi> generateKpiTransactions();
      Flux<Kpi> processSessions();
      Flux<Object> generateKpiRoasGeneral();
      Mono<Void> generateMetricsGeneral();

}
