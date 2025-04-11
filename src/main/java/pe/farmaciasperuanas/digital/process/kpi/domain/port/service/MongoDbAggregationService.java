package pe.farmaciasperuanas.digital.process.kpi.domain.port.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.ArithmeticOperators;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;
import pe.farmaciasperuanas.digital.process.kpi.infrastructure.RetryHandler;

import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Servicio para ejecutar operaciones de agregación en MongoDB
 * para el cálculo de métricas de KPIs.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class MongoDbAggregationService {

    private final ReactiveMongoTemplate mongoTemplate;

    /**
     * Ejecuta una agregación en MongoDB y retorna un valor numérico
     * Si la colección no existe, retorna 0.0
     */
    public <T> Mono<Double> executeAggregation(
            String collection,
            AggregationOperation[] operations,
            Class<T> resultClass,
            String valueField) {

        log.debug("Ejecutando agregación en colección: {}", collection);

        // Verificar primero si la colección existe
        return mongoTemplate.collectionExists(collection)
                .flatMap(exists -> {
                    if (!exists) {
                        log.warn("La colección {} no existe, retornando valor por defecto 0.0", collection);
                        return Mono.just(0.0);
                    }

                    return RetryHandler.withDefaultRetry(
                            () -> {
                                Aggregation agg = Aggregation.newAggregation(operations);

                                return mongoTemplate.aggregate(agg, collection, resultClass)
                                        .map(result -> extractNumericValue(result, valueField))
                                        .next()
                                        .defaultIfEmpty(0.0);
                            },
                            "Ejecución de agregación MongoDB para colección " + collection
                    );
                })
                .onErrorResume(e -> {
                    log.error("Error ejecutando agregación en colección {}: {}", collection, e.getMessage(), e);
                    return Mono.just(0.0);
                });
    }

    /**
     * Extrae un valor numérico de un resultado de agregación
     */
    private <T> double extractNumericValue(T result, String valueField) {
        try {
            if (result instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) result;
                Object value = map.get(valueField);

                if (value == null) {
                    return 0.0;
                } else if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                } else {
                    try {
                        return Double.parseDouble(value.toString());
                    } catch (NumberFormatException e) {
                        log.warn("No se pudo convertir a número: {}", value);
                        return 0.0;
                    }
                }
            } else {
                log.warn("Resultado de agregación no es un mapa: {}", result);
                return 0.0;
            }
        } catch (Exception e) {
            log.error("Error extrayendo valor numérico de resultado: {}", e.getMessage(), e);
            return 0.0;
        }
    }

    /**
     * Calcula la inversión para una campaña y formato específico
     */
    public Mono<Double> calculateInvestment(String campaignId, String formatCode, String providerId, String campaignSubId) {

        // Determinar la colección adecuada según el formato
        String collection;
        if ("CM".equals(formatCode) || "VM".equals(formatCode)) {
            collection = "bq_analytics_slayer_ads_meta_metricas";
        } else {
            collection = "bq_analytics_slayer_ads_google_metricas";
        }

        // Definir las operaciones de agregación para hacer el join con Campaigns
        AggregationOperation[] operations = {
                // Hacer join directamente con la colección de campaigns usando campaignSubId
                Aggregation.lookup("campaigns", "campaignSubId", "campaignSubId", "campaignData"),

                // Desenrollar los resultados del lookup
                Aggregation.unwind("campaignData", true),

                // Filtrar por providerId
                Aggregation.match(Criteria.where("campaignData.providerId").is(providerId)),

                // Agrupar y sumar la inversión
                Aggregation.group().sum("Inversion_3_75").as("totalInversion"),

                // Proyectar con la división: SUM(Inversion_3_75)/0.40
                Aggregation.project()
                        .and(ArithmeticOperators.Divide.valueOf("totalInversion").divideBy(0.40))
                        .as("Inversion")
        };

        return executeAggregation(collection, operations, Map.class, "Inversion")
                .doOnSuccess(value -> log.debug("Inversión calculada para campaña {}, formato {}, proveedor {}: {}",
                        campaignId, formatCode, providerId, value))
                .defaultIfEmpty(0.0)
                .onErrorResume(e -> {
                    log.error("Error calculando inversión para campaña {}: {}", campaignId, e.getMessage(), e);
                    return Mono.just(0.0);
                });
    }
     /**
     * Calcula el alcance para una campaña y formato específico
     */
    public Mono<Double> calculateReach(String campaignId, String formatCode, String providerId, String campaignSubId) {
        String collection;

        // Determinar la colección adecuada según el formato
        if ("CM".equals(formatCode) || "VM".equals(formatCode)) {
            collection = "bq_analytics_slayer_ads_meta_alcance";
        } else {
            collection = "bq_analytics_slayer_ads_google_alcance";
        }

        log.debug("Buscando alcance para campaña ID: {}, SubID: {}, ProveedorID: {}",
                campaignId, campaignSubId, providerId);

        // Definir las operaciones de agregación para hacer el join con Campaigns
        AggregationOperation[] operations = {
                // Buscar documentos donde el campaignSubId coincida
                Aggregation.match(Criteria.where("campaignSubId").is(campaignSubId)),
                // Hacer lookup (join) con la colección Campaigns
                Aggregation.lookup("campaigns", "campaignSubId", "campaignSubId", "campaignData"),
                // Desenrollar los resultados del lookup
                Aggregation.unwind("campaignData"),
                // Filtrar por providerId en la colección Campaigns
                Aggregation.match(Criteria.where("campaignData.providerId").is(providerId)),
                // Ordenar por fecha de proceso descendente
                Aggregation.sort(Sort.Direction.DESC, "Date"),
                // Limitar a un solo registro (el más reciente)
                Aggregation.limit(1),
                // Proyectar solo el campo de alcance
                Aggregation.project().and("Alcance").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.debug("Alcance calculado para campaña {}, formato {}, proveedor {}: {}",
                        campaignId, formatCode, providerId, value))
                .defaultIfEmpty(0.0)
                .onErrorResume(e -> {
                    log.error("Error calculando alcance para campaña {}: {}", campaignId, e.getMessage(), e);
                    return Mono.just(0.0);
                });
    }



    /**
     * Calcula las impresiones para una campaña y formato específico
     */
    public Mono<Double> calculateImpressions(String campaignId, String formatCode) {
        String collection;

        if ("CM".equals(formatCode) || "VM".equals(formatCode)) {
            collection = "meta_metrics";
        } else {
            collection = "google_metrics";
        }

        AggregationOperation[] operations = {
                Aggregation.match(
                        Criteria.where("campaignId").is(campaignId)
                                .and("format").is(formatCode)
                ),
                Aggregation.group().sum("impressions").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.debug("Impresiones calculadas para campaña {}, formato {}: {}",
                        campaignId, formatCode, value));
    }

    /**
     * Calcula los clics para una campaña y formato específico
     */
    public Mono<Double> calculateClicks(String campaignId, String formatCode) {
        String collection;

        if ("CM".equals(formatCode) || "VM".equals(formatCode)) {
            collection = "meta_metrics";
        } else {
            collection = "google_metrics";
        }

        AggregationOperation[] operations = {
                Aggregation.match(
                        Criteria.where("campaignId").is(campaignId)
                                .and("format").is(formatCode)
                ),
                Aggregation.group().sum("clicks").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.debug("Clics calculados para campaña {}, formato {}: {}",
                        campaignId, formatCode, value));
    }

    /**
     * Calcula las ventas para una campaña y formato específico
     */
    public Mono<Double> calculateSales(String campaignId, String formatCode) {
        String collection = "sales"; // Podríamos tener colecciones separadas por formato

        AggregationOperation[] operations = {
                Aggregation.match(
                        Criteria.where("campaignId").is(campaignId)
                                .and("format").is(formatCode)
                ),
                Aggregation.group().sum("revenue").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.debug("Ventas calculadas para campaña {}, formato {}: {}",
                        campaignId, formatCode, value));
    }

    /**
     * Calcula las conversiones para una campaña y formato específico
     */
    public Mono<Double> calculateConversions(String campaignId, String formatCode) {
        String collection;

        if ("CM".equals(formatCode) || "VM".equals(formatCode)) {
            collection = "meta_metrics";
        } else {
            collection = "google_metrics";
        }

        AggregationOperation[] operations = {
                Aggregation.match(
                        Criteria.where("campaignId").is(campaignId)
                                .and("format").is(formatCode)
                ),
                Aggregation.group().sum("conversions").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.debug("Conversiones calculadas para campaña {}, formato {}: {}",
                        campaignId, formatCode, value));
    }

    /**
     * Calcula las sesiones para una campaña y formato específico
     */
    public Mono<Double> calculateSessions(String campaignId, String formatCode, String providerId, String campaignSubId) {
        String collection;

        if ("CM".equals(formatCode) || "VM".equals(formatCode)) {
            collection = "bq_analytics_slayer_ads_ga4_meta_tiktok";
        } else {
            collection = "bq_analytics_slayer_ads_ga4_meta_tiktok";
        }

        // Definir las operaciones de agregación para hacer el join con Campaigns
        AggregationOperation[] operations = {
                // Buscar documentos donde el campaignSubId coincida
                Aggregation.match(Criteria.where("campaignSubId").is(campaignSubId)),
                // Hacer lookup (join) con la colección Campaigns
                Aggregation.lookup("campaigns", "campaignSubId", "campaignSubId", "campaignData"),
                // Desenrollar los resultados del lookup
                Aggregation.unwind("campaignData"),
                // Filtrar por providerId en la colección Campaigns
                Aggregation.match(Criteria.where("campaignData.providerId").is(providerId)),
                // Ordenar por fecha de proceso descendente
                Aggregation.sort(Sort.Direction.DESC, "Date"),
                // Limitar a un solo registro (el más reciente)
                Aggregation.limit(1),
                // Proyectar solo el campo de Sesiones
                Aggregation.project().and("Sessions").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.debug("Sesiones calculado para campaña {}, formato {}, proveedor {}: {}",
                        campaignId, formatCode, providerId, value))
                .defaultIfEmpty(0.0)
                .onErrorResume(e -> {
                    log.error("Error calculando sesiones para campaña {}: {}", campaignId, e.getMessage(), e);
                    return Mono.just(0.0);
                });
    }

    /**
     * Calcula ThruPlay para videos de Meta
     */
    public Mono<Double> calculateThruPlay(String campaignId) {
        String collection = "meta_metrics";

        AggregationOperation[] operations = {
                Aggregation.match(
                        Criteria.where("campaignId").is(campaignId)
                                .and("format").is("VM")
                ),
                Aggregation.group().sum("thruplay").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.debug("ThruPlay calculado para campaña {}: {}",
                        campaignId, value));
    }

    /**
     * Calcula Impression Share para Google Search
     */
    public Mono<Double> calculateImpressionShare(String campaignId) {
        String collection = "google_metrics";

        AggregationOperation[] operations = {
                Aggregation.match(
                        Criteria.where("campaignId").is(campaignId)
                                .and("format").is("GS")
                ),
                Aggregation.group().avg("impression_share").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.debug("Impression Share calculado para campaña {}: {}",
                        campaignId, value));
    }

    /**
     * Calcula las visitas a página para una campaña y formato específico
     */
    public Mono<Double> calculatePageVisits(String campaignId, String formatCode) {
        String collection;

        if ("CM".equals(formatCode) || "VM".equals(formatCode)) {
            collection = "meta_metrics";
        } else {
            collection = "google_metrics";
        }

        AggregationOperation[] operations = {
                Aggregation.match(
                        Criteria.where("campaignId").is(campaignId)
                                .and("format").is(formatCode)
                ),
                Aggregation.group().sum("page_views").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.debug("Visitas a página calculadas para campaña {}, formato {}: {}",
                        campaignId, formatCode, value));
    }
}