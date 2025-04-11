package pe.farmaciasperuanas.digital.process.kpi.application.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@RequiredArgsConstructor
public class KpiJobServiceImpl {

    @Autowired
    private KpiServiceImpl kpiService;

    @PostConstruct
    public void startJob() {
        System.out.println("Job de kpi programado");
    }

    @Scheduled(cron = "0 0 0 * * ?")
    public void executeDailyKpiJob() {
        Mono<Void> result = kpiService.generateKpiFromSalesforceData()
                .doOnTerminate(() -> System.out.println("Kpi ejecutado con éxito"))
                .then();
        result.subscribe();
    }
}
