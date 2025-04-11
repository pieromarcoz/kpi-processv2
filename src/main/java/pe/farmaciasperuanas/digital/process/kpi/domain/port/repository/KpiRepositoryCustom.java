package pe.farmaciasperuanas.digital.process.kpi.domain.port.repository;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Repositorio para acceder a la colección de KPIs en MongoDB
 */
@Repository
public interface KpiRepositoryCustom extends ReactiveMongoRepository<Kpi, String> {

    /**
     * Busca KPIs por campaignId
     */
    Flux<Kpi> findByCampaignId(String campaignId);

    /**
     * Busca KPIs por campaignSubId
     */
    Flux<Kpi> findByCampaignSubId(String campaignSubId);

    /**
     * Busca KPIs por formato
     */
    Flux<Kpi> findByFormat(String format);

    /**
     * Busca KPIs por tipo
     */
    Flux<Kpi> findByType(String type);

    /**
     * Busca KPIs por campaignId y tipo
     */
    Flux<Kpi> findByCampaignIdAndType(String campaignId, String type);

    /**
     * Busca KPIs por campaignSubId y tipo
     */
    Flux<Kpi> findByCampaignSubIdAndType(String campaignSubId, String type);

    /**
     * Busca KPIs por campaignId y kpiId (código específico de KPI)
     */
    Flux<Kpi> findByCampaignIdAndFormatAndCreatedDateBetween(
            String campaignId,
            String format,
            LocalDateTime startDate,
            LocalDateTime endDate
    );

    /**
     * Busca KPIs por campaignId y rango de fechas
     */
    Flux<Kpi> findByCampaignIdAndCreatedDateBetween(
            String campaignId,
            LocalDateTime startDate,
            LocalDateTime endDate);

    /**
     * Busca KPIs por campaignId, lista de kpiIds y rango de fechas
     */
    Flux<Kpi> findByCampaignIdAndKpiIdInAndCreatedDateBetween(
            String campaignId,
            List<String> kpiIds,
            LocalDateTime startDate,
            LocalDateTime endDate);
}

