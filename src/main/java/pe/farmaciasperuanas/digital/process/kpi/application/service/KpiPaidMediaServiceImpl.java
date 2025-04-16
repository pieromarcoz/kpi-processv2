package pe.farmaciasperuanas.digital.process.kpi.application.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.KpiPaidMediaRepository;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.service.KpiPaidMediaService;
import pe.farmaciasperuanas.digital.process.kpi.infrastructure.outbound.adapter.KpiRepositoryImpl;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
@Service
@Slf4j
@RequiredArgsConstructor
public class KpiPaidMediaServiceImpl implements KpiPaidMediaService {
    @Autowired
    private KpiPaidMediaRepository kpiPaidMediaRepository;

    @Override
    public Mono<Map<String, Object>> generateKpiFromPaidMedia() {
        return  CalculateInvestment()
                .then(Mono.just(new HashMap<String, Object>() {{
                    put("message", "Proceso completado con éxito para Paid Media");
                    put("status", "success");
                }}));
    }

    public Mono<Void> CalculateInvestment() {
        return Flux.defer(() -> kpiPaidMediaRepository.CalculateInvestment())
                .doOnNext(kpi -> System.out.println("Procesando KPI ImpressionsParent: " + kpi))
                .then();
    }



}
