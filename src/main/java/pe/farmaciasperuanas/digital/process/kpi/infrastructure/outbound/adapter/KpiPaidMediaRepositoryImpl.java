package pe.farmaciasperuanas.digital.process.kpi.infrastructure.outbound.adapter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.stereotype.Repository;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.KpiPaidMediaRepository;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.KpiRepository;
import reactor.core.publisher.Flux;

@Repository
@Slf4j
@RequiredArgsConstructor
public class KpiPaidMediaRepositoryImpl  implements KpiPaidMediaRepository {

    @Autowired
    private ReactiveMongoTemplate reactiveMongoTemplate;

    private static final String MEDIO_PROPIO = "MEDIO_PROPIO";
    private static final String MEDIO_PAGADO = "MEDIO_PAGADO";

    @Override
    public Flux<Kpi> CalculateInvestment() {
        return null;
    }
    @Override
    public Flux<Kpi> CalculateRoas() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateScope() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateImpressions() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateFrequency() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateClics() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateCPC() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateCTR() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculatePageEngagements() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateCostPerPageEngagements() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateCPM() {
        return null;
    }

    @Override
    public Flux<Object> CalculateRevenue() {
        return null;
    }

    @Override
    public Flux<Object> CalculateCPA() {
        return null;
    }

    @Override
    public Flux<Object> CalculateConversions() {
        return null;
    }

    @Override
    public Flux<Object> CalculateSessions() {
        return null;
    }

    @Override
    public Flux<Object> CalculateThruPlay() {
        return null;
    }

    @Override
    public Flux<Object> CalculateCostPerThruPlay() {
        return null;
    }

    @Override
    public Flux<Object> CalculateImpressionShare() {
        return null;
    }


}
