package pe.farmaciasperuanas.digital.process.kpi.application.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.ArithmeticOperators;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.Campaign;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.Provider;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.service.PaidMediaMetricsService;
import pe.farmaciasperuanas.digital.process.kpi.infrastructure.RetryHandler;

import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Implementación del servicio de métricas para medios pagados utilizando ReactiveMongoTemplate
 * para operaciones avanzadas y agregaciones
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class PaidMediaMetricsServiceImpl implements PaidMediaMetricsService {
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

                    Aggregation agg = Aggregation.newAggregation(operations);
                    return mongoTemplate.aggregate(agg, collection, resultClass)
                            .map(result -> extractNumericValue(result, valueField))
                            .next()
                            .defaultIfEmpty(0.0);
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
     * Calcula la inversión para una campaña y formato específico utilizando los modelos Campaign y Provider
     */
    public Mono<Double> calculateInvestment(String campaignId, String formatCode, String providerId, String campaignSubId) {
        // Determinar la colección adecuada según el formato
        String collection;
        if ("CM".equals(formatCode) || "VM".equals(formatCode)) {
            collection = "bq_analytics_slayer_ads_meta_metricas";
        } else {
            collection = "bq_analytics_slayer_ads_google_metricas";
        }

        log.info("Calculando inversión para campaña: {}, formato: {}, proveedor: {}",
                campaignId, formatCode, providerId);

        // Definir las operaciones de agregación para hacer el join con Campaigns
        AggregationOperation[] operations = {
                // Buscar documentos donde el campaignSubId coincida
                Aggregation.match(Criteria.where("campaignSubId").is(campaignSubId)),

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

        // Corrección: Especificar claramente el tipo de retorno para evitar errores de inferencia
        return executeAggregation(collection, operations, Map.class, "Inversion")
                .doOnSuccess(value -> log.info("Inversión calculada para campaña {}, formato {}, proveedor {}: {}",
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
                .doOnSuccess(value -> log.info("Alcance calculado para campaña {}, formato {}, proveedor {}: {}",
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

        log.debug("Calculando impresiones para campaña: {}, formato: {}", campaignId, formatCode);

        AggregationOperation[] operations = {
                Aggregation.match(
                        Criteria.where("campaignId").is(campaignId)
                                .and("format").is(formatCode)
                ),
                Aggregation.group().sum("impressions").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.info("Impresiones calculadas para campaña {}, formato {}: {}",
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

        log.debug("Calculando clics para campaña: {}, formato: {}", campaignId, formatCode);

        AggregationOperation[] operations = {
                Aggregation.match(
                        Criteria.where("campaignId").is(campaignId)
                                .and("format").is(formatCode)
                ),
                Aggregation.group().sum("clicks").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.info("Clics calculados para campaña {}, formato {}: {}",
                        campaignId, formatCode, value));
    }

    /**
     * Calcula las ventas para una campaña y formato específico
     */
    public Mono<Double> calculateSales(String campaignId, String formatCode) {
        String collection = "sales"; // Podríamos tener colecciones separadas por formato

        log.debug("Calculando ventas para campaña: {}, formato: {}", campaignId, formatCode);

        AggregationOperation[] operations = {
                Aggregation.match(
                        Criteria.where("campaignId").is(campaignId)
                                .and("format").is(formatCode)
                ),
                Aggregation.group().sum("revenue").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.info("Ventas calculadas para campaña {}, formato {}: {}",
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

        log.debug("Calculando conversiones para campaña: {}, formato: {}", campaignId, formatCode);

        AggregationOperation[] operations = {
                Aggregation.match(
                        Criteria.where("campaignId").is(campaignId)
                                .and("format").is(formatCode)
                ),
                Aggregation.group().sum("conversions").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.info("Conversiones calculadas para campaña {}, formato {}: {}",
                        campaignId, formatCode, value));
    }

    /**
     * Calcula las sesiones para una campaña y formato específico
     * Utilizando información del proveedor y campaña
     */
    public Mono<Double> calculateSessions(String campaignId, String formatCode, String providerId, String campaignSubId) {
        String collection;

        if ("CM".equals(formatCode) || "VM".equals(formatCode)) {
            collection = "bq_analytics_slayer_ads_ga4_meta_tiktok";
        } else {
            collection = "bq_analytics_slayer_ads_ga4_meta_tiktok";
        }

        log.debug("Calculando sesiones para campaña ID: {}, formato: {}, proveedor: {}",
                campaignId, formatCode, providerId);

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

                // Hacer un segundo lookup con la colección Provider para obtener detalles del proveedor
                Aggregation.lookup("provider", "campaignData.providerId", "providerId", "providerData"),

                // Desenrollar los resultados del lookup de proveedores (opcional)
                Aggregation.unwind("providerData", true),

                // Ordenar por fecha de proceso descendente
                Aggregation.sort(Sort.Direction.DESC, "Date"),

                // Limitar a un solo registro (el más reciente)
                Aggregation.limit(1),

                // Proyectar solo el campo de Sesiones
                Aggregation.project().and("Sessions").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.info("Sesiones calculadas para campaña {}, formato {}, proveedor {}: {}",
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

        log.debug("Calculando ThruPlay para campaña: {}", campaignId);

        AggregationOperation[] operations = {
                Aggregation.match(
                        Criteria.where("campaignId").is(campaignId)
                                .and("format").is("VM")
                ),
                Aggregation.group().sum("thruplay").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.info("ThruPlay calculado para campaña {}: {}",
                        campaignId, value));
    }

    /**
     * Calcula Impression Share para Google Search
     */
    public Mono<Double> calculateImpressionShare(String campaignId) {
        String collection = "google_metrics";

        log.debug("Calculando Impression Share para campaña: {}", campaignId);

        AggregationOperation[] operations = {
                Aggregation.match(
                        Criteria.where("campaignId").is(campaignId)
                                .and("format").is("GS")
                ),
                Aggregation.group().avg("impression_share").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.info("Impression Share calculado para campaña {}: {}",
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

        log.debug("Calculando visitas a página para campaña: {}, formato: {}", campaignId, formatCode);

        AggregationOperation[] operations = {
                Aggregation.match(
                        Criteria.where("campaignId").is(campaignId)
                                .and("format").is(formatCode)
                ),
                Aggregation.group().sum("page_views").as("value")
        };

        return executeAggregation(collection, operations, Map.class, "value")
                .doOnSuccess(value -> log.info("Visitas a página calculadas para campaña {}, formato {}: {}",
                        campaignId, formatCode, value));
    }

    /**
     * Método adicional para calcular ROI (Return On Investment) basado en ventas e inversión
     */
    public Mono<Double> calculateROI(String campaignId, String formatCode, String providerId, String campaignSubId) {
        log.debug("Calculando ROI para campaña: {}, formato: {}", campaignId, formatCode);

        return calculateSales(campaignId, formatCode)
                .flatMap(sales ->
                        calculateInvestment(campaignId, formatCode, providerId, campaignSubId)
                                .map(investment -> {
                                    // Evitar división por cero
                                    if (investment <= 0) {
                                        return 0.0;
                                    }

                                    double roi = (sales - investment) / investment;
                                    log.info("ROI calculado para campaña {}, formato {}: {}", campaignId, formatCode, roi);
                                    return roi;
                                })
                )
                .defaultIfEmpty(0.0)
                .onErrorResume(e -> {
                    log.error("Error calculando ROI para campaña {}: {}", campaignId, e.getMessage(), e);
                    return Mono.just(0.0);
                });
    }

    /**
     * Método para obtener todas las métricas relacionadas con una campaña y formato
     * Retorna un objeto con todas las métricas calculadas
     */
    public Mono<Map<String, Double>> getAllMetricsForCampaign(String campaignId, String formatCode, String providerId, String campaignSubId) {
        log.info("Obteniendo todas las métricas para campaña: {}, formato: {}", campaignId, formatCode);

        // Creamos un mapa para almacenar todas las métricas
        Map<String, Double> metrics = new java.util.HashMap<>();

        // Calculamos las métricas una por una y las acumulamos en el mapa
        return calculateInvestment(campaignId, formatCode, providerId, campaignSubId)
                .doOnNext(value -> metrics.put("INV", value))
                .then(calculateReach(campaignId, formatCode, providerId, campaignSubId))
                .doOnNext(value -> metrics.put("ALC", value))
                .then(calculateImpressions(campaignId, formatCode))
                .doOnNext(value -> metrics.put("IMP", value))
                .then(calculateClicks(campaignId, formatCode))
                .doOnNext(value -> metrics.put("CLE", value))
                .then(calculateSales(campaignId, formatCode))
                .doOnNext(value -> metrics.put("COM", value))
                .then(calculateConversions(campaignId, formatCode))
                .doOnNext(value -> metrics.put("CON", value))
                .then(calculateSessions(campaignId, formatCode, providerId, campaignSubId))
                .doOnNext(value -> metrics.put("SES", value))
                .then(calculatePageVisits(campaignId, formatCode))
                .doOnNext(value -> metrics.put("VIP", value))
                .then(
                        "VM".equals(formatCode)
                                ? calculateThruPlay(campaignId).doOnNext(value -> metrics.put("THR", value))
                                : "GS".equals(formatCode)
                                ? calculateImpressionShare(campaignId).doOnNext(value -> metrics.put("IMS", value))
                                : Mono.just(0.0)
                )
                .then(Mono.defer(() -> {
                    // Calculamos métricas derivadas
                    double imp = metrics.getOrDefault("IMP", 0.0);
                    double alc = metrics.getOrDefault("ALC", 0.0);
                    double inv = metrics.getOrDefault("INV", 0.0);
                    double cle = metrics.getOrDefault("CLE", 0.0);
                    double com = metrics.getOrDefault("COM", 0.0);
                    double con = metrics.getOrDefault("CON", 0.0);
                    double vip = metrics.getOrDefault("VIP", 0.0);

                    // Frecuencia = Impresiones / Alcance
                    metrics.put("FRE", alc > 0 ? imp / alc : 0.0);

                    // ROAS = Ventas / Inversión
                    metrics.put("ROA", inv > 0 ? com / inv : 0.0);

                    // CPC = Inversión / Clics
                    metrics.put("CPC", cle > 0 ? inv / cle : 0.0);

                    // CTR = Clics / Impresiones
                    metrics.put("CTR", imp > 0 ? (cle / imp) : 0.0);

                    // CPM = (Inversión / Impresiones) * 1000
                    metrics.put("CPM", imp > 0 ? (inv / imp) * 1000 : 0.0);

                    // Costo por Visita = Inversión / Visitas
                    metrics.put("COV", vip > 0 ? inv / vip : 0.0);

                    // CPA = Inversión / Conversiones
                    metrics.put("CPA", con > 0 ? inv / con : 0.0);

                    // Formato específico: Video de Meta
                    if ("VM".equals(formatCode)) {
                        double thr = metrics.getOrDefault("THR", 0.0); // ThruPlay

                        // Costo por ThruPlay = Inversión / ThruPlay
                        metrics.put("CTH", thr > 0 ? inv / thr : 0.0);
                    }

                    log.info("Métricas calculadas para campaña {}, formato {}: {}", campaignId, formatCode, metrics);
                    return Mono.just(metrics);
                }))
                .onErrorResume(e -> {
                    log.error("Error obteniendo métricas para campaña {}: {}", campaignId, e.getMessage(), e);
                    return Mono.just(new java.util.HashMap<>());
                });
    }

    /**
     * Método para guardar un KPI calculado
     */
    public Mono<Kpi> saveKpi(Kpi kpi) {
        log.debug("Guardando KPI: {}", kpi);
        return mongoTemplate.save(kpi)
                .doOnSuccess(savedKpi -> log.info("KPI guardado correctamente: {}", savedKpi.getKpiId()))
                .onErrorResume(e -> {
                    log.error("Error guardando KPI {}: {}", kpi.getKpiId(), e.getMessage(), e);
                    return Mono.empty();
                });
    }
}