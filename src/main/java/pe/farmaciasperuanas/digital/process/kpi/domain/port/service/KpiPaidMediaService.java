package pe.farmaciasperuanas.digital.process.kpi.domain.port.service;

import reactor.core.publisher.Mono;

import java.util.Map;

public interface KpiPaidMediaService {

    Mono<Map<String, Object>> generateKpiFromPaidMedia();
}
