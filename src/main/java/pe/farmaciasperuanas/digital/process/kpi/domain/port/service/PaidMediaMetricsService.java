package pe.farmaciasperuanas.digital.process.kpi.domain.port.service;

import reactor.core.publisher.Mono;

/**
 * Interfaz para servicio de cálculo de métricas específicas
 */
public interface PaidMediaMetricsService {
    /**
     * Calcula la inversión para una campaña y formato específico
     */
    Mono<Double> calculateInvestment(String campaignId, String formatCode, String providerId, String campaignSubId);

    /**
     * Calcula el alcance para una campaña y formato específico
     */
    Mono<Double> calculateReach(String campaignId, String formatCode, String providerId, String campaignSubId);

    /**
     * Calcula las impresiones para una campaña y formato específico
     */
    Mono<Double> calculateImpressions(String campaignId, String formatCode);

    /**
     * Calcula los clics para una campaña y formato específico
     */
    Mono<Double> calculateClicks(String campaignId, String formatCode);

    /**
     * Calcula las ventas para una campaña y formato específico
     */
    Mono<Double> calculateSales(String campaignId, String formatCode);

    /**
     * Calcula las conversiones para una campaña y formato específico
     */
    Mono<Double> calculateConversions(String campaignId, String formatCode);

    /**
     * Calcula las sesiones para una campaña y formato específico
     */
    Mono<Double> calculateSessions(String campaignId, String formatCode, String providerId, String campaignSubId);

    /**
     * Calcula ThruPlay para videos de Meta
     */
    Mono<Double> calculateThruPlay(String campaignId);

    /**
     * Calcula Impression Share para Google Search
     */
    Mono<Double> calculateImpressionShare(String campaignId);

    /**
     * Calcula las visitas a página para una campaña y formato específico
     */
    Mono<Double> calculatePageVisits(String campaignId, String formatCode);
}