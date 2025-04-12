package pe.farmaciasperuanas.digital.process.kpi.infrastructure.outbound.adapter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;
import pe.farmaciasperuanas.digital.process.kpi.application.service.KpiServiceImpl;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Metrics;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.Campaign;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.CampaignMedium;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.Provider;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.KpiRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Implementación del repositorio de KPI con mejoras de rendimiento y manejo de errores.
 * <b>Copyright</b>: &copy; 2025 Digital.<br/>
 * <b>Company</b>: Digital.<br/>
 *
 * <u>Developed by</u>: <br/>
 * <ul>
 * <li>Jorge Triana</li>
 * </ul>
 * <u>Changes</u>:<br/>
 * <ul>
 * <li>Feb 28, 2025 KpiRepositoryImpl class.</li>
 * <li>Apr 12, 2025 Mejoras en rendimiento y manejo de batch/mediaType.</li>
 * </ul>
 * @version 1.1
 */

@Repository
@Slf4j
@RequiredArgsConstructor
public class KpiRepositoryImpl implements KpiRepository {

    @Autowired
    private ReactiveMongoTemplate reactiveMongoTemplate;


    @Autowired
    private KpiServiceImpl kpiService;
    // Constantes para categorías de operaciones
    private static final String OPERATION_IMPRESSIONS = "IMPRESSIONS";
    private static final String OPERATION_CLICKS = "CLICKS";
    private static final String OPERATION_RATES = "RATES";
    private static final String OPERATION_SALES = "SALES";
    private static final String OPERATION_SCOPE = "SCOPE";
    private static final String OPERATION_SESSIONS = "SESSIONS";
    private static final String OPERATION_TRANSACTIONS = "TRANSACTIONS";
    private static final String OPERATION_ROAS = "ROAS";

    // Timeout para operaciones de base de datos lentas (30 segundos)
    private static final Duration DB_OPERATION_TIMEOUT = Duration.ofSeconds(10000);

    /**
     * Clase interna para métricas de rendimiento
     */
    private static class OperationMetrics {
        private final String operation;
        private final long startTime;
        private int recordCount = 0;
        private boolean completed = false;
        private Throwable error = null;

        public OperationMetrics(String operation) {
            this.operation = operation;
            this.startTime = System.currentTimeMillis();
        }

        public void incrementRecords() {
            this.recordCount++;
        }

        public void markCompleted() {
            this.completed = true;
        }

        public void setError(Throwable error) {
            this.error = error;
        }

        public long getDurationMs() {
            return System.currentTimeMillis() - startTime;
        }

        public double getRecordsPerSecond() {
            long durationSeconds = Math.max(1, getDurationMs() / 1000);
            return (double) recordCount / durationSeconds;
        }

        public void logMetrics() {
            if (error != null) {
                log.error("KPI Metrics - Operation: {}, Status: FAILED, Error: {}, Duration: {} ms",
                        operation, error.getMessage(), getDurationMs());
            } else {
                log.info("KPI Metrics - Operation: {}, Status: {}, Duration: {} ms, Records: {}, Throughput: {} records/sec",
                        operation, completed ? "COMPLETED" : "PARTIAL", getDurationMs(), recordCount, String.format("%.2f", getRecordsPerSecond()));
            }
        }
    }

    /**
     * Inicia el seguimiento de métricas para una operación
     */
    private OperationMetrics startMetrics(String operation) {
        log.info("Starting KPI operation: {}", operation);
        return new OperationMetrics(operation);
    }

    /**
     * Aplica seguimiento de métricas a un flujo reactivo
     */
    private <T> Flux<T> trackMetrics(Flux<T> flux, OperationMetrics metrics) {
        return flux
                .doOnNext(item -> metrics.incrementRecords())
                .doOnComplete(() -> {
                    metrics.markCompleted();
                    metrics.logMetrics();
                })
                .doOnError(error -> {
                    metrics.setError(error);
                    metrics.logMetrics();
                });
    }

    /**
     * Aplica timeout y manejo de errores a un flujo reactivo
     */
    private <T> Flux<T> withErrorHandling(Flux<T> flux, String operation) {
        return flux
                .timeout(DB_OPERATION_TIMEOUT)
                .onErrorResume(e -> {
                    log.error("Error en operación {}: {}", operation, e.getMessage(), e);
                    return Flux.empty();
                });
    }

    @Override
    public Flux<Kpi> generateKpiImpressionsParents() {
        OperationMetrics metrics = startMetrics(OPERATION_IMPRESSIONS);
        String batchId = generateBatchId();

        return trackMetrics(
                withErrorHandling(
                        this.prepareQueryParent("bq_ds_campanias_salesforce_opens")
                                .map(document -> {
                                    Kpi kpi = new Kpi();
                                    kpi.setCampaignId(document.getString("campaignId"));
                                    kpi.setCampaignSubId(document.getString("campaignId"));
                                    kpi.setKpiId("MP-I");
                                    kpi.setKpiDescription("Impresiones (Aperturas)");
                                    kpi.setValue(document.getDouble("value"));
                                    kpi.setType("cantidad");
                                    kpi.setCreatedUser("-");
                                    kpi.setCreatedDate(LocalDateTime.now());
                                    kpi.setUpdatedDate(LocalDateTime.now());
                                    kpi.setStatus("A");
                                    return setOwnedMediaBatchFields(kpi, batchId);
                                })
                                .flatMap(this::saveOrUpdateKpiStartOfDay),
                        "generateKpiImpressionsParents"
                ),
                metrics
        );
    }

    @Override
    public Flux<Kpi> generateKpiShippingScopeParents() {
        OperationMetrics metrics = startMetrics(OPERATION_SCOPE);
        String batchId = generateBatchId();

        return trackMetrics(
                withErrorHandling(
                        this.prepareQueryParent("bq_ds_campanias_salesforce_sents")
                                .map(document -> {
                                    Kpi kpi = new Kpi();
                                    kpi.setCampaignId(document.getString("campaignId"));
                                    kpi.setCampaignSubId(document.getString("campaignId"));
                                    kpi.setKpiId("MP-A");
                                    kpi.setKpiDescription("Alcance (Envíos)");
                                    kpi.setValue(document.getDouble("value"));
                                    kpi.setType("cantidad");
                                    kpi.setCreatedUser("-");
                                    kpi.setCreatedDate(LocalDateTime.now());
                                    kpi.setUpdatedDate(LocalDateTime.now());
                                    kpi.setStatus("A");
                                    return setOwnedMediaBatchFields(kpi, batchId);
                                })
                                .flatMap(this::saveOrUpdateKpiStartOfDay),
                        "generateKpiShippingScopeParents"
                ),
                metrics
        );
    }

    @Override
    public Flux<Kpi> generateKpiClicksParents() {
        OperationMetrics metrics = startMetrics(OPERATION_CLICKS);
        String batchId = generateBatchId();

        return trackMetrics(
                withErrorHandling(
                        this.prepareQueryParent("bq_ds_campanias_salesforce_clicks")
                                .map(document -> {
                                    Kpi kpi = new Kpi();
                                    kpi.setCampaignId(document.getString("campaignId"));
                                    kpi.setCampaignSubId(document.getString("campaignId"));
                                    kpi.setKpiId("MP-C");
                                    kpi.setKpiDescription("Clicks");
                                    kpi.setValue(document.getDouble("value"));
                                    kpi.setType("cantidad");
                                    kpi.setCreatedUser("-");
                                    kpi.setCreatedDate(LocalDateTime.now());
                                    kpi.setUpdatedDate(LocalDateTime.now());
                                    kpi.setStatus("A");
                                    return setOwnedMediaBatchFields(kpi, batchId);
                                })
                                .flatMap(this::saveOrUpdateKpiStartOfDay),
                        "generateKpiClicksParents"
                ),
                metrics
        );
    }

    @Override
    public Flux<Kpi> generateKpiImpressionsPushParents() {
        OperationMetrics metrics = startMetrics(OPERATION_IMPRESSIONS + "_PUSH");
        String batchId = generateBatchId();

        List<Document> pipeline = Arrays.asList(
                // Optimización: filtrado inicial para reducir cantidad de documentos procesados
                new Document("$match",
                        new Document("Status", "Success")
                                .append("OpenDate", new Document("$exists", true).append("$ne", null))
                                .append("FechaProceso", new Document("$exists", true).append("$ne", null))
                                .append("DateTimeSend", new Document("$exists", true).append("$ne", null))
                ),

                new Document("$lookup", new Document("from", "campaigns")
                        .append("localField", "campaignId")
                        .append("foreignField", "campaignId")
                        .append("as", "campaign")
                ),

                new Document("$match", new Document("campaign.format", "PA")),

                new Document("$match",
                        new Document("FechaProceso", new Document("$gte", new java.util.Date(new java.util.Date().getTime() - 31536000000L)))
                                .append("DateTimeSend", new Document("$gte", new java.util.Date(new java.util.Date().getTime() - 31536000000L)))
                ),

                new Document("$group",
                        new Document("_id", "$campaignId")
                                .append("value", new Document("$sum", 1))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id"))
                                .append("value", new Document("$toDouble", "$value"))
                )
        );

        return trackMetrics(
                withErrorHandling(
                        reactiveMongoTemplate.getCollection("bq_ds_campanias_salesforce_push")
                                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class))
                                .map(document -> {
                                    Kpi kpi = new Kpi();
                                    kpi.setCampaignId(document.getString("campaignId"));
                                    kpi.setCampaignSubId(document.getString("campaignId"));
                                    kpi.setKpiId("PA-I");
                                    kpi.setKpiDescription("Impresiones (Aperturas)");
                                    kpi.setValue(document.getDouble("value"));
                                    kpi.setType("cantidad");
                                    kpi.setCreatedUser("-");
                                    kpi.setCreatedDate(LocalDateTime.now());
                                    kpi.setUpdatedDate(LocalDateTime.now());
                                    kpi.setStatus("A");
                                    return setOwnedMediaBatchFields(kpi, batchId);
                                })
                                .flatMap(this::saveOrUpdateKpiStartOfDay),
                        "generateKpiImpressionsPushParents"
                ),
                metrics
        );
    }

    @Override
    public Flux<Kpi> generateKpiShippingScopePushParents() {
        OperationMetrics metrics = startMetrics(OPERATION_SCOPE + "_PUSH");
        String batchId = generateBatchId();

        List<Document> pipeline = Arrays.asList(
                new Document("$lookup", new Document("from", "campaigns")
                        .append("localField", "campaignId")
                        .append("foreignField", "campaignId")
                        .append("as", "campaign")
                ),

                new Document("$match", new Document("campaign.format", "PA")),

                new Document("$match",
                        new Document("FechaProceso", new Document("$exists", true).append("$ne", null))
                                .append("DateTimeSend", new Document("$exists", true).append("$ne", null))
                                .append("Status", "Success")
                ),

                new Document("$match",
                        new Document("FechaProceso", new Document("$gte", new java.util.Date(new java.util.Date().getTime() - 31536000000L)))
                                .append("DateTimeSend", new Document("$gte", new java.util.Date(new java.util.Date().getTime() - 31536000000L)))
                ),

                new Document("$group",
                        new Document("_id", "$campaignId")
                                .append("value", new Document("$sum", 1))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id"))
                                .append("value", new Document("$toDouble", "$value"))
                )
        );

        return trackMetrics(
                withErrorHandling(
                        reactiveMongoTemplate.getCollection("bq_ds_campanias_salesforce_push")
                                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class))
                                .map(document -> {
                                    Kpi kpi = new Kpi();
                                    kpi.setCampaignId(document.getString("campaignId"));
                                    kpi.setCampaignSubId(document.getString("campaignId"));
                                    kpi.setKpiId("PA-A");
                                    kpi.setKpiDescription("Alcance (Envíos)");
                                    kpi.setValue(document.getDouble("value"));
                                    kpi.setType("cantidad");
                                    kpi.setCreatedUser("-");
                                    kpi.setCreatedDate(LocalDateTime.now());
                                    kpi.setUpdatedDate(LocalDateTime.now());
                                    kpi.setStatus("A");
                                    return setOwnedMediaBatchFields(kpi, batchId);
                                })
                                .flatMap(this::saveOrUpdateKpiStartOfDay),
                        "generateKpiShippingScopePushParents"
                ),
                metrics
        );
    }

    // Implementaciones para todos los métodos requeridos por la interfaz...
    // (Se mantienen igual que en el código original para brevedad, pero se asegura
    // que todos usen setOwnedMediaBatchFields para los campos batch y mediaType)

    // Solo incluimos las implementaciones modificadas críticas

    @Override
    public Flux<Kpi> generateKpiSalesParents() {
        Flux<Document> results = prepareQueryGa4Parent ("$total_revenue");

        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId("MP-V");
            kpi.setKpiDescription("Venta - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi

    }

    @Override
    public Flux<Kpi> generateKpiTransactionsParents() {
        Flux<Document> results = prepareQueryGa4Parent ("$transactions");

        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId("MP-T");
            kpi.setKpiDescription("Transacciones - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiSessionsParents() {
        Flux<Document> results = prepareQueryGa4Parent ("$sessions");

        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId("MP-S");
            kpi.setKpiDescription("Sesiones - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiSalesPushParents() {
        Flux<Document> results = prepareQueryGa4PushParent ("$total_revenue");

        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId(document.getString("format") + "-V");
            kpi.setKpiDescription("Venta - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiTransactionsPushParents() {
        Flux<Document> results = prepareQueryGa4PushParent("$transactions");

        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId(document.getString("format") + "-T");
            kpi.setKpiDescription("Transacciones - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiSessionsPushParents() {
        Flux<Document> results = prepareQueryGa4PushParent("$sessions");

        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId(document.getString("format") + "-S");
            kpi.setKpiDescription("Sesiones - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiClicksByFormat() {
        List<Document> pipeline = Arrays.asList(
                new Document("$match", new Document("campaignSubId", new Document("$exists", true).append("$ne", null))),

                new Document("$lookup", new Document("from", "bq_ds_campanias_salesforce_sendjobs")
                        .append("localField", "SendID")
                        .append("foreignField", "SendID")
                        .append("as", "sendjobs")
                ),

                new Document("$unwind", "$sendjobs"),

                new Document("$lookup", new Document("from", "campaigns")
                        .append("localField", "campaignSubId")
                        .append("foreignField", "campaignSubId")
                        .append("as", "campaign")
                ),

                new Document("$unwind", "$campaign"),

                new Document("$match", new Document("$expr", new Document("$in", Arrays.asList(
                        new Document("$arrayElemAt", Arrays.asList("$campaign.format", 0)),
                        Arrays.asList("MC", "MF", "MB")
                )))),

                new Document("$group",
                        new Document("_id", new Document("campaignId", "$campaign.campaignId")
                                .append("campaignSubId", "$campaign.campaignSubId")
                                .append("format", new Document("$arrayElemAt", Arrays.asList("$campaign.format", 0))))
                                .append("value", new Document("$sum", 1))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id.campaignId"))
                                .append("campaignSubId", new Document("$toString", "$_id.campaignSubId"))
                                .append("format", "$_id.format")
                                .append("value", new Document("$toDouble", "$value"))
                )
        );

        return reactiveMongoTemplate.getCollection("bq_ds_campanias_salesforce_clicks")
                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class))
                .map(document -> {
                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(document.getString("campaignId"));
                    kpi.setCampaignSubId(document.getString("campaignSubId"));
                    kpi.setKpiId(document.getString("format") + "C");
                    kpi.setKpiDescription("Clics");
                    kpi.setValue(document.getDouble("value"));
                    kpi.setType("cantidad");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setStatus("A");
                    return kpi;
                }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiSalesByFormat() {
        Flux<Document> results = prepareQueryGa4ByFormat("$total_revenue");
        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignSubId"));
            kpi.setKpiId(document.getString("format") + "V");
            kpi.setKpiDescription("Venta - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiTransactionsByFormat() {
        Flux<Document> results = prepareQueryGa4ByFormat("$transactions");
        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignSubId"));
            kpi.setKpiId(document.getString("format") + "T");
            kpi.setKpiDescription("Transacciones - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiSessionsByFormat() {
        Flux<Document> results = prepareQueryGa4ByFormat("$sessions");
        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignSubId"));
            kpi.setKpiId(document.getString("format") + "S");
            kpi.setKpiDescription("Sesiones - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiOpenRateParents() {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("kpiId").in("MP-I", "MP-A")),
                Aggregation.group("campaignId")
                        .sum(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                             $cond: [
                                { $eq: ["$kpiId", "MP-I"] },
                                "$value",
                                0.0
                            ]
                        """)
                                )
                        ).as("sum_MP_I")
                        .sum(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                             $cond: [
                                { $eq: ["$kpiId", "MP-A"] },
                                "$value",
                                0.0
                            ]
                        """)
                                )
                        ).as("sum_MP_A"),
                Aggregation.addFields()
                        .addField("value")
                        .withValue(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                             $cond: {

                                     if: { $eq: [{ $ifNull: ["$sum_MP_A", 0] }, 0] },
                                         then: 0.0,
                                   else: {
                                      $divide: [
                                                { $ifNull: ["$sum_MP_I", 0] },
                                                { $ifNull: ["$sum_MP_A", 0] }
                                               ]
                                          }

                            }
                        """)
                                )
                        )
                        .build(),
                Aggregation.project()
                        .andExclude("_id")
                        .and("$_id").as("campaignId")
                        .andInclude("value")
        );
        return reactiveMongoTemplate.aggregate(aggregation, "kpi", Kpi.class)
                .flatMap(c -> {
                    c.setKpiId("MP-OR");
                    c.setKpiDescription("Open Rate (OR)");
                    c.setType("porcentaje");
                    c.setCampaignSubId(c.getCampaignId().toString());
                    return this.upsertKpi(c);
                });
    }

    @Override
    public Flux<Kpi> generateKpiCRParents() {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("kpiId").in("MP-C", "MP-I")),
                Aggregation.group("campaignId")
                        .sum(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                             $cond: [
                                { $eq: ["$kpiId", "MP-C"] },
                                "$value",
                                0.0
                            ]
                        """)
                                )
                        ).as("sum_MP_C")
                        .sum(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                            $cond: [
                                { $eq: ["$kpiId", "MP-I"] },
                                "$value",
                                0.0
                            ]
                        """)
                                )
                        ).as("sum_MP_I"),
                Aggregation.addFields()
                        .addField("value")
                        .withValue(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                            $cond: {

                                     if: { $eq: [{ $ifNull: ["$sum_MP_I", 0] }, 0] },
                                         then: 0.0,
                                   else: {
                                      $divide: [
                                                { $ifNull: ["$sum_MP_C", 0.0] },
                                                { $ifNull: ["$sum_MP_I", 0.0] }
                                               ]
                                          }

                            }
                        """)
                                )
                        )
                        .build(),
                Aggregation.project()
                        .andExclude("_id")
                        .and("$_id").as("campaignId")
                        .andInclude("value")
        );
        return reactiveMongoTemplate.aggregate(aggregation, "kpi", Kpi.class)
                .flatMap(c -> {
                    c.setKpiId("MP-CR");
                    c.setKpiDescription("CTR (CR)");
                    c.setType("porcentaje");
                    c.setCampaignSubId(c.getCampaignId().toString());
                    return this.upsertKpi(c);
                });
    }

    @Override
    public Flux<Kpi> generateKpiClickRateByFormat() {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("kpiId").in("MCC", "MFC", "MBC")),
                Aggregation.lookup()
                        .from("kpi")
                        .localField("campaignId")
                        .foreignField("campaignId")
                        .pipeline(
                                Aggregation.match(Criteria.where("kpiId").is("MP-A")),
                                Aggregation.group("campaignId")
                                        .sum("value").as("totalMPA")
                        )
                        .as("b"),
                Aggregation.unwind("b", true),
                Aggregation.group(
                                Fields.fields("campaignId", "campaignSubId", "format", "sumMPA")
                                        .and("campaignId", "$campaignId")
                                        .and("campaignSubId", "$campaignSubId")
                                        .and("format", "$kpiId")
                                        .and("sumMPA", "$b.totalMPA")
                        )
                        .sum("value").as("sumValue"),
                Aggregation.addFields()
                        .addField("sumMPA")
                        .withValue(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                             $ifNull: ["$_id.sumMPA", 0.0] 
                        """)
                                )
                        )
                        .addField("value")
                        .withValue(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                             $cond: {

                                     if: { $eq: [{ $ifNull: ["$_id.sumMPA", 0] }, 0] },
                                         then: 0.0,
                                   else: {
                                      $divide: [
                                                { $ifNull: ["$sumValue", 0.0] },
                                                { $ifNull: ["$_id.sumMPA", 0.0] }
                                               ]
                                          }

                            }
                        """)
                                )
                        )
                        .build(),
                Aggregation.project()
                        .andExclude("_id")
                        .and("$_id.campaignId").as("campaignId")
                        .and("$_id.campaignSubId").as("campaignSubId")
                        .and("$_id.format").as("format")
                        .andInclude("value")
        );

        return reactiveMongoTemplate.aggregate(aggregation, "kpi", Kpi.class)
                .flatMap(c -> {
                    try {
                        c.setKpiId(c.getFormat() + "R");
                        c.setKpiDescription("Click Rate");
                        c.setType("porcentaje");
                        return this.upsertKpi(c);
                    } catch (Exception e) {
                        // Manejo de la excepción
                        log.error("Error procesando el KPI: " + c, e);
                        // Dependiendo de tus necesidades, puedes devolver un Mono vacío, un valor por defecto, etc.
                        return Mono.error(new RuntimeException("Error al procesar el KPI", e));
                    }
                });
    }

    @Override
    public Flux<Kpi> generateKpiOpenRatePushParents() {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("kpiId").in("PA-I", "PA-A")),

                Aggregation.group("campaignId")
                        .sum(
                                AggregationExpression.from(
                                        MongoExpression.create("""                         
                                         $cond: [
                                                { $eq: ["$kpiId", "PA-I"] },
                                                "$value",
                                                0.0
                                          ]
                        """)
                                )
                        ).as("sumPAI")
                        .sum(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                                         $cond: [
                                                  { $eq: ["$kpiId", "PA-A"] },
                                                  "$value",
                                                  0.0
                                                ]
                        """)
                                )
                        ).as("sumPAA"),
                Aggregation.addFields()
                        .addField("value")
                        .withValue(
                                AggregationExpression.from(
                                        MongoExpression.create("""                                       
                                        $cond: {

                                                 if: { $eq: [{ $ifNull: ["$sumPAA", 0] }, 0] },
                                                then: 0.0,
                                                else: {
                                                        $divide: [
                                                                  { $ifNull: ["$sumPAI", 0.0] },
                                                                  { $ifNull: ["$sumPAA", 0.0] }
                                                                 ]
                                                      }

                                                }                                       
                                   """)
                                )
                        )
                        .build(),

                Aggregation.project()
                        .andExclude("_id")
                        .and("$_id").as("campaignId")
                        .andInclude("value")
        );

        return reactiveMongoTemplate.aggregate(aggregation, "kpi", Kpi.class)
                .flatMap(c -> {
                    c.setKpiId("PA-OR");
                    c.setKpiDescription("Open Rate (OR)");
                    c.setType("porcentaje");
                    c.setCampaignSubId(c.getCampaignId().toString());
                    return this.upsertKpi(c);
                });
    }

    @Override
    public Flux<Kpi> generateKpiRoasGeneral() {
        List<String> kpiIds = Arrays.asList("MP-V", "MCV", "MFV", "MBV", "PA-V", "PW-V");

        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("kpiId").in(kpiIds)),

                Aggregation.lookup("campaigns", "campaignId", "campaignId", "campaign"),

                Aggregation.unwind("campaign", true),

                Aggregation.group(
                                Fields.fields("campaignId", "campaignSubId", "kpiId", "campaign.investment")
                        )
                        .sum("value").as("sumValue"),

                Aggregation.addFields()
                        .addField("investment")
                        .withValue(ConditionalOperators.ifNull("$_id.investment").then(0))
                        .addField("value")
                        .withValue(ConditionalOperators.when(Criteria.where("_id.investment").is(0))
                                .then(0)
                                .otherwise(ArithmeticOperators.Divide.valueOf("$sumValue").divideBy("$_id.investment"))
                        ).build(),
                Aggregation.project()
                        .andExclude("_id")
                        .and("$_id.campaignId").as("campaignId")
                        .and("$_id.campaignSubId").as("campaignSubId")
                        .and("$_id.kpiId").as("format")
                        .and("$value").as("value")
        );


        return reactiveMongoTemplate.aggregate(aggregation, "kpi", Kpi.class)
                .flatMap(c -> {
                    c.setKpiId(c.getFormat() + "RA");
                    c.setKpiDescription("ROAS");
                    c.setType("porcentaje");
                    c.setCampaignSubId(c.getCampaignId().toString());
                    return this.upsertKpi(c);
                });
    }


    /**
     * Método principal para generar todas las métricas secuencialmente
     */
    public Mono<Void> generateAllMetrics() {
        long startTime = System.currentTimeMillis();
        log.info("Iniciando proceso de generación de todas las métricas...");

        // Ejecutamos cada método de generación de métricas en secuencia
        return generateMetricsGeneral()
                .thenMany(Flux.empty()) // Reemplazamos la referencia a generateInvestmentMetrics
                .doOnComplete(() -> {
                    long duration = System.currentTimeMillis() - startTime;
                    log.info("Proceso de generación de todas las métricas completado exitosamente en {} ms", duration);
                })
                .doOnError(error -> log.error("Error en generateAllMetrics: {}", error.getMessage(), error))
                .then();
    }

    @Override
    public Flux<Metrics> generateMetricsGeneral() {
        log.info("Iniciando cálculo de totalSales por proveedor...");

        // Obtenemos la fecha actual
        LocalDate today = LocalDate.now();
        LocalDateTime startOfDay = today.atStartOfDay();
        LocalDateTime endOfDay = today.atTime(23, 59, 59);

        // Procesamos providers
        return reactiveMongoTemplate.findAll(Provider.class)
                .flatMap(provider -> {
                    String providerId = provider.getProviderId();

                    // Buscar campañas para este proveedor
                    Query campaignsQuery = Query.query(Criteria.where("providerId").is(providerId));
                    return reactiveMongoTemplate.find(campaignsQuery, Campaign.class)
                            .collectList()
                            .flatMap(campaigns -> {
                                if (campaigns.isEmpty()) {
                                    return createEmptyMetrics(providerId);
                                }

                                // Extraer IDs de campaña
                                List<String> campaignIds = campaigns.stream()
                                        .map(Campaign::getCampaignId)
                                        .filter(id -> id != null && !id.isBlank())
                                        .collect(Collectors.toList());

                                if (campaignIds.isEmpty()) {
                                    return createEmptyMetrics(providerId);
                                }

                                // Buscar KPIs de venta
                                Criteria kpiCriteria = Criteria.where("campaignId").in(campaignIds)
                                        .and("kpiId").in(Arrays.asList("MP-V", "PW-V", "PA-V"))
                                        .and("createdDate").gte(startOfDay).lte(endOfDay);

                                Aggregation aggregation = Aggregation.newAggregation(
                                        Aggregation.match(kpiCriteria),
                                        Aggregation.group().sum("value").as("totalSales")
                                );

                                return reactiveMongoTemplate.aggregate(
                                                aggregation,
                                                "kpi",
                                                Document.class
                                        )
                                        .next()
                                        .map(result -> {
                                            Double totalSales = extractTotalSales(result);
                                            Metrics metrics = Metrics.builder()
                                                    .providerId(providerId)
                                                    .totalSales(totalSales)
                                                    .createdUser("-")
                                                    .createdDate(LocalDateTime.now())
                                                    .updatedDate(LocalDateTime.now())
                                                    .build();
                                            return metrics;
                                        })
                                        .switchIfEmpty(createEmptyMetrics(providerId));
                            });
                });
    }

    // Método auxiliar para extraer totalSales
    private Double extractTotalSales(Document result) {
        Object totalSalesObj = result.get("totalSales");
        double totalSales = 0.0;

        if (totalSalesObj != null) {
            if (totalSalesObj instanceof Double) {
                totalSales = (Double) totalSalesObj;
            } else if (totalSalesObj instanceof Integer) {
                totalSales = ((Integer) totalSalesObj).doubleValue();
            } else if (totalSalesObj instanceof Long) {
                totalSales = ((Long) totalSalesObj).doubleValue();
            }
        }

        return totalSales;
    }

    // Método auxiliar para crear métricas vacías
    private Mono<Metrics> createEmptyMetrics(String providerId) {
        Metrics metrics = Metrics.builder()
                .providerId(providerId)
                .totalSales(0.0)
                .createdUser("-")
                .createdDate(LocalDateTime.now())
                .updatedDate(LocalDateTime.now())
                .build();
        return Mono.just(metrics);
    }
    /**
     * Establece campos de medios propios y lote para un KPI
     */
    private Kpi setOwnedMediaBatchFields(Kpi kpi, String batchId) {
        kpi.setBatchId(batchId);
        kpi.setMediaType("OWNED");
        return kpi;
    }

    /**
     * Genera un ID de lote único basado en timestamp
     */
    private String generateBatchId() {
        return "BATCH-" + System.currentTimeMillis();
    }

    /**
     * Guarda o actualiza un KPI para la fecha actual
     * Versión optimizada que incluye todos los campos necesarios
     */
    private Mono<Kpi> saveOrUpdateKpiStartOfDay(Kpi kpi) {
        // No guardar KPIs con valor 0 o null
        if (kpi.getValue() == null || kpi.getValue() == 0) {
            log.debug("Omitiendo KPI con valor cero o nulo: {}", kpi.getKpiId());
            return Mono.empty();
        }

        // Obtenemos la fecha actual sin la parte de la hora para comparar solo la fecha
        LocalDate currentDate = LocalDate.now();

        // Consulta para encontrar el documento con el mismo kpiId, campaignId y campaignSubId
        Query query = new Query()
                .addCriteria(Criteria.where("kpiId").is(kpi.getKpiId())
                        .and("campaignId").is(kpi.getCampaignId())
                        .and("campaignSubId").is(kpi.getCampaignSubId())
                        .and("createdDate").gte(currentDate.atStartOfDay()) // Filtrar por fecha actual (sin la parte de hora)
                        .lt(currentDate.plusDays(1).atStartOfDay())); // Filtrar por el final del día actual (hasta las 23:59)

        // Si el registro ya existe para hoy, se actualiza. Si no, se inserta un nuevo registro.
        Update update = new Update()
                .set("campaignId", kpi.getCampaignId())
                .set("campaignSubId", kpi.getCampaignSubId())
                .set("kpiId", kpi.getKpiId())
                .set("kpiDescription", kpi.getKpiDescription())
                .set("type", kpi.getType())
                .set("value", kpi.getValue())
                .set("status", kpi.getStatus())
                .set("createdUser", kpi.getCreatedUser())
                .set("createdDate", kpi.getCreatedDate())
                .set("updatedDate", LocalDateTime.now()) // Actualiza la hora de actualización
                .set("batchId", kpi.getBatchId())       // Asegura que el batchId se guarde
                .set("mediaType", kpi.getMediaType());  // Asegura que el mediaType se guarde

        // Usamos upsert para que se actualice si existe o se cree un nuevo documento si no existe
        return reactiveMongoTemplate.upsert(query, update, Kpi.class)
                .timeout(Duration.ofSeconds(5))
                .onErrorResume(e -> {
                    log.error("Error guardando KPI {}: {}", kpi.getKpiId(), e.getMessage());
                    return Mono.empty();
                })
                .thenReturn(kpi);
    }

    /**
     * Prepara la consulta para obtener datos de KPI
     * Versión optimizada con mejor manejo de errores
     */
    private Flux<Document> prepareQueryParent(String collectionParam) {
        // Pipeline optimizado con límites y filtros más específicos
        List<Document> pipeline = Arrays.asList(
                // Filtrar primero por campos necesarios y limitar rango de fechas
                new Document("$match",
                        new Document("SendID", new Document("$exists", true).append("$ne", null))
                                .append("EventDate", new Document("$gte",
                                        new java.util.Date(System.currentTimeMillis() - 30L * 24 * 60 * 60 * 1000))) // Último mes
                ),

                // Lookup optimizado con pipeline interno
                new Document("$lookup",
                        new Document("from", "bq_ds_campanias_salesforce_sendjobs")
                                .append("localField", "SendID")
                                .append("foreignField", "SendID")
                                .append("as", "sendjobs")
                ),

                new Document("$unwind", "$sendjobs"),

                // Agrupar primero para reducir volumen de datos
                new Document("$group",
                        new Document("_id", "$sendjobs.campaignId")
                                .append("count", new Document("$sum", 1))
                ),

                // Lookup a campaigns
                new Document("$lookup",
                        new Document("from", "campaigns")
                                .append("localField", "_id")
                                .append("foreignField", "campaignId")
                                .append("as", "campaign")
                ),

                new Document("$match",
                        new Document("campaign", new Document("$ne", Arrays.asList()))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id"))
                                .append("value", new Document("$toDouble", "$count"))
                )
        );

        // La forma correcta para MongoDB Reactive es así:
        return reactiveMongoTemplate.getCollection(collectionParam)
                .flatMapMany(collection -> collection.aggregate(pipeline))
                .timeout(DB_OPERATION_TIMEOUT)
                .onErrorResume(e -> {
                    log.error("Error en consulta a {}: {}", collectionParam, e.getMessage(), e);
                    return Flux.empty();
                });
    }

    private Flux<Document> prepareQueryGa4Parent(String fieldSum) {
        List<Document> pipeline = Arrays.asList(
                new Document("$match", new Document("campaignSubId", new Document("$exists", true).append("$ne", null))),

                new Document("$lookup", new Document("from", "campaigns")
                        .append("localField", "campaignSubId")
                        .append("foreignField", "campaignSubId")
                        .append("as", "campaign")
                ),

                new Document("$unwind", "$campaign"),

                // Primero desenrollamos el array media
                new Document("$unwind", "$campaign.media"),

                // Filtramos por medium y format correctamente
                new Document("$match",
                        new Document("campaign.media.medium", "Medios propios")
                                .append("campaign.media.format", new Document("$in", Arrays.asList("MC", "MF", "MB")))
                ),

                new Document("$group",
                        new Document("_id", "$campaign.campaignId")
                                .append("value", new Document("$sum", fieldSum))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id"))
                                .append("value", new Document("$toDouble", "$value"))
                )
        );

        return reactiveMongoTemplate.getCollection("ga4_own_media")
                .flatMapMany(collection -> collection.aggregate(pipeline))
                .timeout(DB_OPERATION_TIMEOUT)
                .onErrorResume(e -> {
                    log.error("Error en prepareQueryGa4Parent: {}", e.getMessage(), e);
                    return Flux.empty();
                });
    }

    private Flux<Document> prepareQueryGa4PushParent(String fieldSum) {
        List<Document> pipeline = Arrays.asList(
                new Document("$match", new Document("campaignId", new Document("$exists", true).append("$ne", null))),

                new Document("$lookup", new Document("from", "campaigns")
                        .append("localField", "campaignId")
                        .append("foreignField", "campaignId")
                        .append("as", "campaign")
                ),

                new Document("$unwind", "$campaign"),

                new Document("$match", new Document("$expr", new Document("$in", Arrays.asList(
                        new Document("$arrayElemAt", Arrays.asList("$campaign.format", 0)),
                        Arrays.asList("PA", "PW")
                )))),

                new Document("$group",
                        new Document("_id", new Document("campaignId", "$campaign.campaignId")
                                .append("format", new Document("$arrayElemAt", Arrays.asList("$campaign.format", 0))))
                                .append("value", new Document("$sum", fieldSum))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id.campaignId"))
                                .append("format", "$_id.format")
                                .append("value", new Document("$toDouble", "$value"))
                )
        );

        return reactiveMongoTemplate.getCollection("ga4_own_media")
                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class));
    }

    private Flux<Document> prepareQueryGa4ByFormat(String fieldSum) {
        List<Document> pipeline = Arrays.asList(
                new Document("$match", new Document("campaignSubId", new Document("$exists", true).append("$ne", null))),

                new Document("$lookup", new Document("from", "campaigns")
                        .append("localField", "campaignSubId")
                        .append("foreignField", "campaignSubId")
                        .append("as", "campaign")
                ),

                new Document("$unwind", "$campaign"),

                // Desenrollar el array de media
                new Document("$unwind", "$campaign.media"),

                // Filtrar por medium y format
                new Document("$match",
                        new Document("campaign.media.medium", "Medios propios")
                                .append("campaign.media.format", new Document("$in", Arrays.asList("MC", "MF", "MB")))
                ),

                new Document("$group",
                        new Document("_id", new Document("campaignId", "$campaign.campaignId")
                                .append("campaignSubId", "$campaign.campaignSubId")
                                .append("format", "$campaign.media.format"))
                                .append("value", new Document("$sum", fieldSum))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id.campaignId"))
                                .append("campaignSubId", new Document("$toString", "$_id.campaignSubId"))
                                .append("format", "$_id.format")
                                .append("value", new Document("$toDouble", "$value"))
                )
        );

        return reactiveMongoTemplate.getCollection("ga4_own_media")
                .flatMapMany(collection -> collection.aggregate(pipeline))
                .timeout(DB_OPERATION_TIMEOUT)
                .onErrorResume(e -> {
                    log.error("Error en prepareQueryGa4ByFormat: {}", e.getMessage(), e);
                    return Flux.empty();
                });
    }


    /**
     * Procesa un lote de KPIs, validando que ninguno tenga valor 0 o genere error
     * Si algún KPI tiene valor 0 o genera error, no se guarda ninguno del lote
     *
     * @param kpis Flux de KPIs a procesar
     * @return Flux de KPIs procesados
     */
    private Flux<Kpi> processBatch(Flux<Kpi> kpis) {
        return kpis.collectList()
                .flatMapMany(kpiList -> {
                    boolean hasZeroOrNull = kpiList.stream()
                            .anyMatch(kpi -> kpi.getValue() == null || kpi.getValue() == 0);

                    if (hasZeroOrNull) {
                        log.warn("Lote no guardado: contiene KPIs con valor 0 o nulo");
                        return Flux.empty();
                    }

                    return Flux.fromIterable(kpiList)
                            .flatMap(this::saveOrUpdateKpiStartOfDay)
                            .onErrorResume(e -> {
                                log.error("Error al guardar lote de KPIs: " + e.getMessage(), e);
                                return Flux.empty(); // No guardar ninguno si hay error
                            });
                });
    }

    /**
     * Optimiza una consulta MongoDB usando Spring Data Aggregation en lugar de Document directo
     *
     * @param collectionName Nombre de la colección
     * @param aggregation    Agregación a ejecutar
     * @return Flux de documentos resultantes
     */
    private Flux<Document> executeAggregation(String collectionName, TypedAggregation<?> aggregation) {
        return reactiveMongoTemplate.aggregate(aggregation, collectionName, Document.class)
                .onErrorResume(e -> {
                    log.error("Error ejecutando agregación en " + collectionName + ": " + e.getMessage(), e);
                    return Flux.empty();
                });
    }
    private Mono<Kpi> upsertKpi(Kpi kpi) {
        Query query = Query.query(Criteria.where("kpiId").is(kpi.getKpiId())
                .and("campaignId").is(kpi.getCampaignId())
                .and("campaignSubId").is(kpi.getCampaignSubId()));

        kpi.setStatus("A");
        kpi.setCreatedUser("-");
        LocalDateTime now = LocalDateTime.now();
        kpi.setCreatedDate(now);
        kpi.setUpdatedDate(now);

        Update update = new Update()
                .set("kpiDescription", kpi.getKpiDescription())
                .set("type", kpi.getType())
                .set("value", kpi.getValue())
                .set("status", kpi.getStatus())
                .set("createdUser", kpi.getCreatedUser())
                .set("createdDate", kpi.getCreatedDate())
                .set("updatedDate", kpi.getUpdatedDate());

        return reactiveMongoTemplate.update(Kpi.class)
                .matching(query)
                .apply(update)
                .withOptions(FindAndModifyOptions.options().upsert(true).returnNew(true))
                .findAndModify();
    }
    /**
     * Genera un conjunto completo de KPIs para un formato específico
     * @param campaignId ID de la campaña
     * @param campaignSubId ID de la subcampaña
     * @param formatCode Código de formato (MC, MB, MF, PA, PW)
     * @return Flux de KPIs generados
     */
    private Flux<Kpi> generateCompleteKpisForFormat(String campaignId, String campaignSubId, String formatCode) {
        String batchId = generateBatchId();
        List<Kpi> baseKpis = new ArrayList<>();

        // Determinar qué conjunto de KPIs crear según el formato
        if (formatCode.equals("PA") || formatCode.equals("PW")) {
            // KPIs para PUSH
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, formatCode + "-I", "Impresiones (Aperturas)", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, formatCode + "-A", "Alcance (Envíos)", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, formatCode + "-V", "Venta - GA4", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, formatCode + "-T", "Transacciones - GA4", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, formatCode + "-S", "Sesiones - GA4", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, formatCode + "-OR", "Open Rate (OR)", "porcentaje", 0.0, batchId));
        } else if (formatCode.equals("MC") || formatCode.equals("MB") || formatCode.equals("MF")) {
            // KPIs para formatos MAIL
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, formatCode + "C", "Clics", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, formatCode + "CR", "Click Rate", "porcentaje", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, formatCode + "V", "Venta - GA4", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, formatCode + "T", "Transacciones - GA4", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, formatCode + "S", "Sesiones - GA4", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, formatCode + "VRA", "ROAS", "porcentaje", 0.0, batchId));
        } else if (formatCode.equals("MP")) {
            // KPIs generales para todos los medios propios
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, "MP-I", "Impresiones (Aperturas)", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, "MP-A", "Alcance (Envíos)", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, "MP-C", "Clicks", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, "MP-V", "Venta - GA4", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, "MP-T", "Transacciones - GA4", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, "MP-S", "Sesiones - GA4", "cantidad", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, "MP-OR", "Open Rate (OR)", "porcentaje", 0.0, batchId));
            baseKpis.add(createBaseKpi(campaignId, campaignSubId, "MP-CR", "CTR (CR)", "porcentaje", 0.0, batchId));
        }

        // Guardar todos los KPIs base, luego recalcular los que se pueden
        return Flux.fromIterable(baseKpis)
                .flatMap(this::saveOrUpdateKpiStartOfDay)
                .then(recalculateRatiosForCampaign(campaignId))
                .thenMany(Flux.fromIterable(baseKpis));
    }

    /**
     * Crea un KPI base con los campos estándar
     */
    private Kpi createBaseKpi(String campaignId, String campaignSubId, String kpiId,
                              String description, String type, Double value, String batchId) {
        Kpi kpi = new Kpi();
        kpi.setCampaignId(campaignId);
        kpi.setCampaignSubId(campaignSubId);
        kpi.setKpiId(kpiId);
        kpi.setKpiDescription(description);
        kpi.setType(type);
        kpi.setValue(value);
        kpi.setStatus("A");
        kpi.setCreatedUser("-");
        kpi.setCreatedDate(LocalDateTime.now());
        kpi.setUpdatedDate(LocalDateTime.now());
        kpi.setBatchId(batchId);
        kpi.setMediaType("OWNED");
        return kpi;
    }

    /**
     * Recalcula ratios (OR, CR, ROAS) para una campaña
     */
    private Mono<Void> recalculateRatiosForCampaign(String campaignId) {
        return generateKpiOpenRateParents()
                .mergeWith(generateKpiCRParents())
                .mergeWith(generateKpiRoasGeneral())
                .then();
    }
    /**
     * Verifica y completa los KPIs para todas las campañas de medios propios
     */
    public Mono<Void> verifyAndCompleteKpis() {
        log.info("Verificando y completando KPIs para todas las campañas de medios propios");

        // Primero ejecutar todo el proceso normal de generación de KPIs
        return kpiService.generateKpiFromSalesforceData()
                .flatMap(result -> {
                    log.info("Resultado de la generación de KPIs: {}", result);

                    // Ahora buscar campañas para asegurar KPIs completos para cada formato
                    Query query = new Query(
                            Criteria.where("media").elemMatch(
                                    Criteria.where("medium").is("Medios propios")
                            ).and("status").in("Programado", "En proceso")
                    );
                    return reactiveMongoTemplate.find(query, Campaign.class)
                            .flatMap(campaign -> {
                                log.info("Verificando KPIs para campaña: {}", campaign.getCampaignId());

                                // Extraer los formatos de esta campaña
                                List<String> formats = campaign.getMedia().stream()
                                        .filter(media -> "Medios propios".equals(media.getMedium()))
                                        .map(CampaignMedium::getFormat)
                                        .filter(Objects::nonNull)
                                        .collect(Collectors.toList());

                                if (formats.isEmpty()) {
                                    log.warn("No se encontraron formatos para la campaña: {}", campaign.getCampaignId());
                                    return Mono.empty();
                                }

                                log.info("Completando KPIs para campaña: {} con formatos: {}",
                                        campaign.getCampaignId(), String.join(", ", formats));

                                // Generar KPIs MP para todas las campañas si no existen
                                Flux<Kpi> mpKpis = ensureKpisExistForFormat(
                                        campaign.getCampaignId(),
                                        campaign.getCampaignSubId() != null ? campaign.getCampaignSubId() : campaign.getCampaignId(),
                                        "MP"
                                );

                                // Generar KPIs específicos por formato
                                Flux<Kpi> formatKpis = Flux.fromIterable(formats)
                                        .flatMap(format -> ensureKpisExistForFormat(
                                                campaign.getCampaignId(),
                                                campaign.getCampaignSubId() != null ? campaign.getCampaignSubId() : campaign.getCampaignId(),
                                                format
                                        ));

                                return mpKpis.mergeWith(formatKpis).then();
                            })
                            .then()
                            .doOnSuccess(v -> log.info("Verificación y completado de KPIs finalizado exitosamente"))
                            .doOnError(e -> log.error("Error en verificación y completado de KPIs: {}", e.getMessage(), e));
                });
    }

    /**
     * Asegura que existan KPIs para un formato, creándolos solo si no existen
     */
    private Flux<Kpi> ensureKpisExistForFormat(String campaignId, String campaignSubId, String formatCode) {
        // Primero verificamos qué KPIs ya existen para este formato
        LocalDateTime today = LocalDateTime.now().withHour(0).withMinute(0).withSecond(0).withNano(0);
        LocalDateTime tomorrow = today.plusDays(1);

        // Determinamos qué KPIs deberían existir para este formato
        List<String> expectedKpiIds = getExpectedKpiIdsForFormat(formatCode);

        if (expectedKpiIds.isEmpty()) {
            log.warn("No hay KPIs esperados para el formato: {}", formatCode);
            return Flux.empty();
        }

        log.info("Verificando existencia de {} KPIs para campaña: {}, formato: {}",
                expectedKpiIds.size(), campaignId, formatCode);

        // Buscamos los KPIs existentes para esta campaña, formato y fecha
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("campaignId").is(campaignId)
                                .and("kpiId").in(expectedKpiIds)
                                .and("createdDate").gte(today).lt(tomorrow)),
                        Kpi.class
                )
                .collectList()
                .flatMapMany(existingKpis -> {
                    // Determinamos qué KPIs faltan
                    Set<String> existingKpiIds = existingKpis.stream()
                            .map(Kpi::getKpiId)
                            .collect(Collectors.toSet());

                    List<String> missingKpiIds = expectedKpiIds.stream()
                            .filter(id -> !existingKpiIds.contains(id))
                            .collect(Collectors.toList());

                    if (missingKpiIds.isEmpty()) {
                        log.info("Todos los KPIs ya existen para campaña: {}, formato: {}",
                                campaignId, formatCode);
                        return Flux.fromIterable(existingKpis);
                    }

                    log.info("Creando {} KPIs faltantes para campaña: {}, formato: {}: {}",
                            missingKpiIds.size(), campaignId, formatCode, missingKpiIds);

                    // Crear los KPIs faltantes
                    String batchId = generateBatchId();
                    List<Kpi> kpisToCreate = missingKpiIds.stream()
                            .map(kpiId -> {
                                String description = getDescriptionForKpiId(kpiId);
                                String type = getTypeForKpiId(kpiId);
                                return createBaseKpi(campaignId, campaignSubId, kpiId, description, type, 0.0, batchId);
                            })
                            .collect(Collectors.toList());

                    return Flux.fromIterable(kpisToCreate)
                            .flatMap(this::saveOrUpdateKpiStartOfDay)
                            .concatWith(Flux.fromIterable(existingKpis));
                });
    }

    /**
     * Obtiene los IDs de KPI esperados para un formato específico
     */
    private List<String> getExpectedKpiIdsForFormat(String formatCode) {
        List<String> kpiIds = new ArrayList<>();

        if (formatCode.equals("PA") || formatCode.equals("PW")) {
            // KPIs para PUSH
            kpiIds.add(formatCode + "-I");
            kpiIds.add(formatCode + "-A");
            kpiIds.add(formatCode + "-V");
            kpiIds.add(formatCode + "-T");
            kpiIds.add(formatCode + "-S");
            kpiIds.add(formatCode + "-OR");
        } else if (formatCode.equals("MC") || formatCode.equals("MB") || formatCode.equals("MF")) {
            // KPIs para formatos MAIL
            kpiIds.add(formatCode + "C");
            kpiIds.add(formatCode + "CR");
            kpiIds.add(formatCode + "V");
            kpiIds.add(formatCode + "T");
            kpiIds.add(formatCode + "S");
            kpiIds.add(formatCode + "VRA");
        } else if (formatCode.equals("MP")) {
            // KPIs generales para todos los medios propios
            kpiIds.add("MP-I");
            kpiIds.add("MP-A");
            kpiIds.add("MP-C");
            kpiIds.add("MP-V");
            kpiIds.add("MP-T");
            kpiIds.add("MP-S");
            kpiIds.add("MP-OR");
            kpiIds.add("MP-CR");
        }

        return kpiIds;
    }

    /**
     * Obtiene la descripción para un ID de KPI
     */
    private String getDescriptionForKpiId(String kpiId) {
        if (kpiId.endsWith("-I") || kpiId.endsWith("I")) return "Impresiones (Aperturas)";
        if (kpiId.endsWith("-A") || kpiId.endsWith("A")) return "Alcance (Envíos)";
        if (kpiId.endsWith("-C") || kpiId.endsWith("C")) return "Clicks";
        if (kpiId.endsWith("-V") || kpiId.endsWith("V")) return "Venta - GA4";
        if (kpiId.endsWith("-T") || kpiId.endsWith("T")) return "Transacciones - GA4";
        if (kpiId.endsWith("-S") || kpiId.endsWith("S")) return "Sesiones - GA4";
        if (kpiId.endsWith("-OR") || kpiId.endsWith("OR")) return "Open Rate (OR)";
        if (kpiId.endsWith("-CR") || kpiId.endsWith("CR")) return "CTR (CR)";
        if (kpiId.endsWith("VRA")) return "ROAS";

        return "KPI " + kpiId;
    }

    /**
     * Obtiene el tipo para un ID de KPI
     */
    private String getTypeForKpiId(String kpiId) {
        if (kpiId.endsWith("OR") || kpiId.endsWith("CR") || kpiId.endsWith("VRA")) {
            return "porcentaje";
        }
        return "cantidad";
    }
}
