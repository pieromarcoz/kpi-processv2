package pe.farmaciasperuanas.digital.process.kpi.domain.port.repository;

import org.springframework.stereotype.Repository;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface KpiPaidMediaRepository {
      Flux<Kpi> CalculateInvestment();
      Flux<Kpi> CalculateRoas();
      Flux<Kpi> CalculateScope();
      Flux<Kpi> CalculateImpressions();
      Flux<Kpi> CalculateFrequency();
      Flux<Kpi> CalculateClics ();
      Flux<Kpi> CalculateCPC();
      Flux<Kpi> CalculateCTR();
      Flux<Kpi> CalculatePageEngagements();
      Flux<Kpi> CalculateCostPerPageEngagements();
      Flux<Kpi> CalculateCPM();
      Flux<Object> CalculateRevenue();
      Flux<Object>CalculateCPA();
      Flux<Object>CalculateConversions();
      Flux<Object>CalculateSessions();
      Flux<Object>CalculateThruPlay();
      Flux<Object>CalculateCostPerThruPlay();
      Flux<Object>CalculateImpressionShare();

}
