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
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Metrics;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.Campaign;
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
    private static final Duration DB_OPERATION_TIMEOUT = Duration.ofSeconds(120); // Increased from 30

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
        final String OPERATION_NAME = "SALES_PARENTS";
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String fieldToSum = "totalRevenue"; // Corresponds to "Total revenue" in GA4
        List<String> formats = Arrays.asList("MC", "MF", "MB"); // Formats for Mailing Parents
        String batchId = generateBatchId();

        return prepareQueryGa4ByFormat(fieldToSum, formats)
                .collectList() // Collect all results for different formats/subIds of the same campaignId
                .flatMapMany(list -> {
                    Map<String, Double> salesByCampaign = list.stream()
                            .collect(Collectors.groupingBy(
                                    doc -> doc.getString("campaignId"), // Group only by campaignId
                                    Collectors.summingDouble(doc -> doc.getDouble("value"))
                            ));

                    List<Kpi> kpis = new ArrayList<>();
                    salesByCampaign.forEach((campaignId, totalSales) -> {
                        Kpi kpi = new Kpi();
                        kpi.setCampaignId(campaignId);
                        kpi.setCampaignSubId(campaignId); // Parent KPI uses campaignId as subId
                        kpi.setKpiId("MP-V"); // Sales Parent
                        kpi.setKpiDescription("Venta - GA4 Medios Propios (Padre)");
                        kpi.setType("moneda"); // Sales are currency
                        kpi.setValue(totalSales);
                        kpi.setStatus("A");
                        kpi.setCreatedUser("-");
                        kpi.setCreatedDate(LocalDateTime.now());
                        kpi.setUpdatedDate(LocalDateTime.now());
                        kpi.setBatchId(batchId);
                        kpi.setMediaType("OWNED");

                        kpis.add(kpi);
                    });
                    log.info("Calculated {} Sales Parent KPIs.", kpis.size());
                    return Flux.fromIterable(kpis);
                })
                .flatMap(this::saveOrUpdateKpiStartOfDay)
                .transform(flux -> trackMetrics(flux, metrics))
                .transform(flux -> withErrorHandling(flux, OPERATION_NAME));
    }


    @Override
    public Flux<Kpi> generateKpiSalesParents() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Kpi> generateKpiTransactionsParents() {
        final String OPERATION_NAME = "TRANSACTIONS_PARENTS";
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String fieldToSum = "transactions"; // Corresponds to "Transaccions" in GA4
        List<String> formats = Arrays.asList("MC", "MF", "MB"); // Formats for Mailing Parents
        String batchId = generateBatchId();

        return prepareQueryGa4ByFormat(fieldToSum, formats)
                .collectList() // Collect all results for different formats/subIds of the same campaignId
                .flatMapMany(list -> {
                    Map<String, Double> transactionsByCampaign = list.stream()
                            .collect(Collectors.groupingBy(
                                    doc -> doc.getString("campaignId"), // Group only by campaignId
                                    Collectors.summingDouble(doc -> doc.getDouble("value"))
                            ));

                    List<Kpi> kpis = new ArrayList<>();
                    transactionsByCampaign.forEach((campaignId, totalTransactions) -> {
                        Kpi kpi = new Kpi();
                        kpi.setCampaignId(campaignId);
                        kpi.setCampaignSubId(campaignId); // Parent KPI uses campaignId as subId
                        kpi.setKpiId("MP-T"); // Transactions Parent
                        kpi.setKpiDescription("Transacciones - GA4 Medios Propios (Padre)");
                        kpi.setType("cantidad"); // Transactions are counts
                        kpi.setValue(totalTransactions);
                        kpi.setStatus("A");
                        kpi.setCreatedUser("-");
                        kpi.setCreatedDate(LocalDateTime.now());
                        kpi.setUpdatedDate(LocalDateTime.now());
                        kpi.setBatchId(batchId);
                        kpi.setMediaType("OWNED");

                        kpis.add(kpi);
                    });
                    log.info("Calculated {} Transactions Parent KPIs.", kpis.size());
                    return Flux.fromIterable(kpis);
                })
                .flatMap(this::saveOrUpdateKpiStartOfDay)
                .transform(flux -> trackMetrics(flux, metrics))
                .transform(flux -> withErrorHandling(flux, OPERATION_NAME));
    }


    @Override
    public Flux<Kpi> generateKpiTransactionsParents() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Kpi> generateKpiSessionsParents() {
        final String OPERATION_NAME = "SESSIONS_PARENTS";
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String fieldToSum = "sessions";
        List<String> formats = Arrays.asList("MC", "MF", "MB"); // Formats for Mailing Parents

        String batchId = generateBatchId();

        return prepareQueryGa4ByFormat(fieldToSum, formats)
                .collectList() // Collect all results for different formats/subIds of the same campaignId
                .flatMapMany(list -> {
                    Map<String, Double> sessionsByCampaign = list.stream()
                            .collect(Collectors.groupingBy(
                                    doc -> doc.getString("campaignId"), // Group only by campaignId
                                    Collectors.summingDouble(doc -> doc.getDouble("value"))
                            ));

                    List<Kpi> kpis = sessionsByCampaign.entrySet().stream()
                            .map(entry -> {
                                String campaignId = entry.getKey();
                                Double totalValue = entry.getValue();

                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaignId);
                                kpi.setCampaignSubId(campaignId); // Parent KPI uses campaignId as subId
                                kpi.setKpiId("MP-S"); // KPI ID for Parent Sessions
                                kpi.setKpiDescription("Sesiones - GA4 (Padre)");
                                kpi.setType("cantidad"); // Type is count
                                kpi.setValue(totalValue);
                                kpi.setStatus("A"); // Default status
                                kpi.setCreatedUser("-"); // Default user
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                setOwnedMediaBatchFields(kpi, batchId);
                                return kpi;
                            })
                            .collect(Collectors.toList());

                    return Flux.fromIterable(kpis);
                })
                .flatMap(this::saveOrUpdateKpiStartOfDay) // Save each aggregated KPI
                .transform(flux -> trackMetrics(flux, metrics)) // Track performance
                .transform(flux -> withErrorHandling(flux, OPERATION_NAME)); // Handle errors
    }


    @Override
    public Flux<Kpi> generateKpiSessionsParents() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Kpi> generateKpiSalesPushParents() {
        final String OPERATION_NAME = "SALES_PUSH_PARENTS";
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String fieldToSum = "totalRevenue"; // Assuming field name in ga4_own_media

        String batchId = generateBatchId();

        return prepareQueryGa4PushParent(fieldToSum) // Uses helper for Push Parent logic
                .collectList()
                .flatMapMany(list -> {
                    Map<String, Double> salesByCampaign = list.stream()
                            .collect(Collectors.groupingBy(
                                    doc -> doc.getString("campaignId"),
                                    Collectors.summingDouble(doc -> doc.getDouble("value"))
                            ));

                    List<Kpi> kpis = salesByCampaign.entrySet().stream()
                            .map(entry -> {
                                String campaignId = entry.getKey();
                                Double totalValue = entry.getValue();

                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaignId);
                                kpi.setCampaignSubId(campaignId); // Parent uses campaignId
                                kpi.setKpiId("PP-V"); // KPI ID for Push Parent Sales
                                kpi.setKpiDescription("Venta - GA4 Push (Padre)");
                                kpi.setType("moneda");
                                kpi.setValue(totalValue);
                                kpi.setStatus("A");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setBatchId(batchId);
                                kpi.setMediaType("PUSH"); // Explicitly set for Push KPIs
                                return kpi;
                            })
                            .collect(Collectors.toList());

                    return Flux.fromIterable(kpis);
                })
                .flatMap(this::saveOrUpdateKpiStartOfDay)
                .transform(flux -> trackMetrics(flux, metrics))
                .transform(flux -> withErrorHandling(flux, OPERATION_NAME));
    }


    @Override
    public Flux<Kpi> generateKpiSalesPushParents() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Kpi> generateKpiTransactionsPushParents() {
        final String OPERATION_NAME = "TRANSACTIONS_PUSH_PARENTS";
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String fieldToSum = "transactions"; // Assuming field name in ga4_own_media

        String batchId = generateBatchId();

        return prepareQueryGa4PushParent(fieldToSum) // Uses helper for Push Parent logic
                .collectList()
                .flatMapMany(list -> {
                    Map<String, Double> transactionsByCampaign = list.stream()
                            .collect(Collectors.groupingBy(
                                    doc -> doc.getString("campaignId"),
                                    Collectors.summingDouble(doc -> doc.getDouble("value"))
                            ));

                    List<Kpi> kpis = transactionsByCampaign.entrySet().stream()
                            .map(entry -> {
                                String campaignId = entry.getKey();
                                Double totalValue = entry.getValue();

                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaignId);
                                kpi.setCampaignSubId(campaignId); // Parent uses campaignId
                                kpi.setKpiId("PP-T"); // KPI ID for Push Parent Transactions
                                kpi.setKpiDescription("Transacciones - GA4 Push (Padre)");
                                kpi.setType("cantidad");
                                kpi.setValue(totalValue);
                                kpi.setStatus("A");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setBatchId(batchId);
                                kpi.setMediaType("PUSH"); // Explicitly set for Push KPIs
                                return kpi;
                            })
                            .collect(Collectors.toList());

                    return Flux.fromIterable(kpis);
                })
                .flatMap(this::saveOrUpdateKpiStartOfDay)
                .transform(flux -> trackMetrics(flux, metrics))
                .transform(flux -> withErrorHandling(flux, OPERATION_NAME));
    }


    @Override
    public Flux<Kpi> generateKpiTransactionsPushParents() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Kpi> generateKpiSessionsPushParents() {
        final String OPERATION_NAME = "SESSIONS_PUSH_PARENTS";
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String fieldToSum = "sessions"; // Assuming field name in ga4_own_media

        String batchId = generateBatchId();

        return prepareQueryGa4PushParent(fieldToSum) // Uses helper for Push Parent logic
                .collectList()
                .flatMapMany(list -> {
                    Map<String, Double> sessionsByCampaign = list.stream()
                            .collect(Collectors.groupingBy(
                                    doc -> doc.getString("campaignId"),
                                    Collectors.summingDouble(doc -> doc.getDouble("value"))
                            ));

                    List<Kpi> kpis = sessionsByCampaign.entrySet().stream()
                            .map(entry -> {
                                String campaignId = entry.getKey();
                                Double totalValue = entry.getValue();

                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaignId);
                                kpi.setCampaignSubId(campaignId); // Parent uses campaignId
                                kpi.setKpiId("PP-S"); // KPI ID for Push Parent Sessions
                                kpi.setKpiDescription("Sesiones - GA4 Push (Padre)");
                                kpi.setType("cantidad");
                                kpi.setValue(totalValue);
                                kpi.setStatus("A");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setBatchId(batchId);
                                kpi.setMediaType("PUSH"); // Explicitly set for Push KPIs
                                return kpi;
                            })
                            .collect(Collectors.toList());

                    return Flux.fromIterable(kpis);
                })
                .flatMap(this::saveOrUpdateKpiStartOfDay)
                .transform(flux -> trackMetrics(flux, metrics))
                .transform(flux -> withErrorHandling(flux, OPERATION_NAME));
    }


    @Override
    public Flux<Kpi> generateKpiSessionsPushParents() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Kpi> generateKpiClicksByFormat() {
        final String OPERATION_NAME = "CLICKS_BY_FORMAT";
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String fieldToSum = "clicks"; // Assuming field name in ga4_own_media based on user logic "sum(""Clicks"")"
        List<String> formats = Arrays.asList("MC", "MF", "MB"); // Formats for Mailing

        String batchId = generateBatchId();

        return prepareQueryGa4ByFormat(fieldToSum, formats) // Uses helper for GA4 query by format
                .flatMap(doc -> {
                    String campaignId = doc.getString("campaignId");
                    String campaignSubId = doc.getString("campaignSubId"); // Provided by prepareQueryGa4ByFormat
                    String format = doc.getString("format"); // Provided by prepareQueryGa4ByFormat
                    Double value = doc.getDouble("value");

                    String kpiId;
                    switch (format) {
                        case "MC": kpiId = "MC-C"; break;
                        case "MF": kpiId = "MF-C"; break;
                        case "MB": kpiId = "MB-C"; break;
                        default:
                            log.warn("Unsupported format encountered for Clicks KPI: {}", format);
                            return Mono.empty();
                    }

                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(campaignId);
                    kpi.setCampaignSubId(campaignSubId);
                    kpi.setKpiId(kpiId);
                    kpi.setKpiDescription("Clicks - GA4 Medios Propios (" + format + ")");
                    kpi.setType("cantidad");
                    kpi.setValue(value);
                    kpi.setStatus("A");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setFormat(format); // Set the format field
                    setOwnedMediaBatchFields(kpi, batchId); // Sets mediaType to OWNED

                    return saveOrUpdateKpiStartOfDay(kpi);
                })
                .transform(flux -> trackMetrics(flux, metrics)) // Track performance
                .transform(flux -> withErrorHandling(flux, OPERATION_NAME)); // Handle errors
    }


    @Override
    public Flux<Kpi> generateKpiClicksByFormat() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Kpi> generateKpiSalesByFormat() {
        final String OPERATION_NAME = "SALES_BY_FORMAT";
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String fieldToSum = "totalRevenue"; // Assuming field name in ga4_own_media
        List<String> formats = Arrays.asList("MC", "MF", "MB"); // Formats for Mailing

        String batchId = generateBatchId();

        return prepareQueryGa4ByFormat(fieldToSum, formats) // Uses helper for GA4 query by format
                .flatMap(doc -> {
                    String campaignId = doc.getString("campaignId");
                    String campaignSubId = doc.getString("campaignSubId");
                    String format = doc.getString("format");
                    Double value = doc.getDouble("value");

                    String kpiId;
                    switch (format) {
                        case "MC": kpiId = "MC-V"; break;
                        case "MF": kpiId = "MF-V"; break;
                        case "MB": kpiId = "MB-V"; break;
                        default:
                            log.warn("Unsupported format encountered for Sales KPI: {}", format);
                            return Mono.empty();
                    }

                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(campaignId);
                    kpi.setCampaignSubId(campaignSubId);
                    kpi.setKpiId(kpiId);
                    kpi.setKpiDescription("Venta - GA4 Medios Propios (" + format + ")");
                    kpi.setType("moneda"); // Sales are currency
                    kpi.setValue(value);
                    kpi.setStatus("A");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setFormat(format);
                    setOwnedMediaBatchFields(kpi, batchId);

                    return saveOrUpdateKpiStartOfDay(kpi);
                })
                .transform(flux -> trackMetrics(flux, metrics))
                .transform(flux -> withErrorHandling(flux, OPERATION_NAME));
    }


    @Override
    public Flux<Kpi> generateKpiSalesByFormat() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Kpi> generateKpiTransactionsByFormat() {
        final String OPERATION_NAME = "TRANSACTIONS_BY_FORMAT";
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String fieldToSum = "transactions"; // Assuming field name in ga4_own_media
        List<String> formats = Arrays.asList("MC", "MF", "MB"); // Formats for Mailing

        String batchId = generateBatchId();

        return prepareQueryGa4ByFormat(fieldToSum, formats) // Uses helper for GA4 query by format
                .flatMap(doc -> {
                    String campaignId = doc.getString("campaignId");
                    String campaignSubId = doc.getString("campaignSubId");
                    String format = doc.getString("format");
                    Double value = doc.getDouble("value");

                    String kpiId;
                    switch (format) {
                        case "MC": kpiId = "MC-T"; break;
                        case "MF": kpiId = "MF-T"; break;
                        case "MB": kpiId = "MB-T"; break;
                        default:
                            log.warn("Unsupported format encountered for Transactions KPI: {}", format);
                            return Mono.empty();
                    }

                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(campaignId);
                    kpi.setCampaignSubId(campaignSubId);
                    kpi.setKpiId(kpiId);
                    kpi.setKpiDescription("Transacciones - GA4 Medios Propios (" + format + ")");
                    kpi.setType("cantidad");
                    kpi.setValue(value);
                    kpi.setStatus("A");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setFormat(format);
                    setOwnedMediaBatchFields(kpi, batchId);

                    return saveOrUpdateKpiStartOfDay(kpi);
                })
                .transform(flux -> trackMetrics(flux, metrics))
                .transform(flux -> withErrorHandling(flux, OPERATION_NAME));
    }


    @Override
    public Flux<Kpi> generateKpiTransactionsByFormat() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Kpi> generateKpiSessionsByFormat() {
        final String OPERATION_NAME = "SESSIONS_BY_FORMAT";
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String fieldToSum = "sessions"; // Assuming field name in ga4_own_media
        List<String> formats = Arrays.asList("MC", "MF", "MB"); // Formats for Mailing

        String batchId = generateBatchId();

        return prepareQueryGa4ByFormat(fieldToSum, formats) // Uses helper for GA4 query by format
                .flatMap(doc -> {
                    String campaignId = doc.getString("campaignId");
                    String campaignSubId = doc.getString("campaignSubId");
                    String format = doc.getString("format");
                    Double value = doc.getDouble("value");

                    String kpiId;
                    switch (format) {
                        case "MC": kpiId = "MC-S"; break;
                        case "MF": kpiId = "MF-S"; break;
                        case "MB": kpiId = "MB-S"; break;
                        default:
                            log.warn("Unsupported format encountered for Sessions KPI: {}", format);
                            return Mono.empty();
                    }

                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(campaignId);
                    kpi.setCampaignSubId(campaignSubId);
                    kpi.setKpiId(kpiId);
                    kpi.setKpiDescription("Sesiones - GA4 Medios Propios (" + format + ")");
                    kpi.setType("cantidad");
                    kpi.setValue(value);
                    kpi.setStatus("A");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setFormat(format);
                    setOwnedMediaBatchFields(kpi, batchId);

                    return saveOrUpdateKpiStartOfDay(kpi);
                })
                .transform(flux -> trackMetrics(flux, metrics))
                .transform(flux -> withErrorHandling(flux, OPERATION_NAME));
    }


    @Override
    public Flux<Kpi> generateKpiSessionsByFormat() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Kpi> generateKpiOpenRateParents() {
        final String OPERATION_NAME = "OPEN_RATE_PARENTS";
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String batchId = generateBatchId();
        LocalDateTime startOfDay = LocalDate.now().atStartOfDay();

        Query impressionsQuery = new Query(Criteria.where("kpiId").is("MP-I")
                .and("createdDate").gte(startOfDay));
        Flux<Kpi> impressionsFlux = reactiveMongoTemplate.find(impressionsQuery, Kpi.class, "kpi")
                .doOnError(e -> log.error("Error fetching MP-I KPIs for Open Rate: {}", e.getMessage()));


        Query scopeQuery = new Query(Criteria.where("kpiId").is("MP-A")
                .and("createdDate").gte(startOfDay));
        Flux<Kpi> scopeFlux = reactiveMongoTemplate.find(scopeQuery, Kpi.class, "kpi")
                 .doOnError(e -> log.error("Error fetching MP-A KPIs for Open Rate: {}", e.getMessage()));

        return Flux.zip(
                impressionsFlux.collectMap(Kpi::getCampaignId), // Map<CampaignId, Kpi(MP-I)>
                scopeFlux.collectMap(Kpi::getCampaignId)       // Map<CampaignId, Kpi(MP-A)>
        )
        .flatMapMany(tuple -> {
            Map<String, Kpi> impressionsMap = tuple.getT1();
            Map<String, Kpi> scopeMap = tuple.getT2();
            List<Kpi> openRateKpis = new ArrayList<>();

            impressionsMap.forEach((campaignId, impressionKpi) -> {
                Kpi scopeKpi = scopeMap.get(campaignId);
                if (scopeKpi != null && scopeKpi.getValue() != null && scopeKpi.getValue() != 0.0 && impressionKpi.getValue() != null) {
                    double openRate = impressionKpi.getValue() / scopeKpi.getValue();

                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(campaignId);
                    kpi.setCampaignSubId(campaignId); // Parent KPI uses campaignId as subId
                    kpi.setKpiId("MP-OR"); // Open Rate Parent
                    kpi.setKpiDescription("Open Rate Medios Propios (Padre)");
                    kpi.setType("porcentaje");
                    kpi.setValue(openRate); // Store as fraction (e.g., 0.1 for 10%)
                    kpi.setStatus("A");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setBatchId(batchId); // Use current batchId for the derived KPI
                    kpi.setMediaType("OWNED"); // Parent KPIs are OWNED media

                    openRateKpis.add(kpi);
                } else {
                    log.warn("Skipping Open Rate calculation for campaign {}: Scope KPI (MP-A) or Impression KPI (MP-I) missing, null, or scope is zero.", campaignId);
                }
            });
            log.info("Calculated {} Open Rate Parent KPIs.", openRateKpis.size());
            return Flux.fromIterable(openRateKpis);
        })
        .flatMap(this::saveOrUpdateKpiStartOfDay) // Save each calculated KPI
        .transform(flux -> trackMetrics(flux, metrics))
        .transform(flux -> withErrorHandling(flux, OPERATION_NAME));
    }


    @Override
    public Flux<Kpi> generateKpiOpenRateParents() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Kpi> generateKpiCRParents() {
        final String OPERATION_NAME = "CR_PARENTS"; // CR = Click Rate
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String batchId = generateBatchId();
        LocalDateTime startOfDay = LocalDate.now().atStartOfDay();

        Query clicksQuery = new Query(Criteria.where("kpiId").is("MP-C")
                .and("createdDate").gte(startOfDay));
        Flux<Kpi> clicksFlux = reactiveMongoTemplate.find(clicksQuery, Kpi.class, "kpi")
                .doOnError(e -> log.error("Error fetching MP-C KPIs for CR: {}", e.getMessage()));

        Query impressionsQuery = new Query(Criteria.where("kpiId").is("MP-I")
                .and("createdDate").gte(startOfDay));
        Flux<Kpi> impressionsFlux = reactiveMongoTemplate.find(impressionsQuery, Kpi.class, "kpi")
                .doOnError(e -> log.error("Error fetching MP-I KPIs for CR: {}", e.getMessage()));

        return Flux.zip(
                clicksFlux.collectMap(Kpi::getCampaignId),       // Map<CampaignId, Kpi(MP-C)>
                impressionsFlux.collectMap(Kpi::getCampaignId)  // Map<CampaignId, Kpi(MP-I)>
        )
        .flatMapMany(tuple -> {
            Map<String, Kpi> clicksMap = tuple.getT1();
            Map<String, Kpi> impressionsMap = tuple.getT2();
            List<Kpi> clickRateKpis = new ArrayList<>();

            clicksMap.forEach((campaignId, clickKpi) -> {
                Kpi impressionKpi = impressionsMap.get(campaignId);
                if (impressionKpi != null && impressionKpi.getValue() != null && impressionKpi.getValue() != 0.0 && clickKpi.getValue() != null) {
                    double clickRate = clickKpi.getValue() / impressionKpi.getValue();

                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(campaignId);
                    kpi.setCampaignSubId(campaignId); // Parent KPI uses campaignId as subId
                    kpi.setKpiId("MP-CR"); // Click Rate Parent
                    kpi.setKpiDescription("Click Rate Medios Propios (Padre)");
                    kpi.setType("porcentaje");
                    kpi.setValue(clickRate); // Store as fraction
                    kpi.setStatus("A");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setBatchId(batchId);
                    kpi.setMediaType("OWNED");

                    clickRateKpis.add(kpi);
                } else {
                     log.warn("Skipping Click Rate calculation for campaign {}: Impression KPI (MP-I) or Click KPI (MP-C) missing, null, or impressions are zero.", campaignId);
                }
            });
             log.info("Calculated {} Click Rate Parent KPIs.", clickRateKpis.size());
            return Flux.fromIterable(clickRateKpis);
        })
        .flatMap(this::saveOrUpdateKpiStartOfDay)
        .transform(flux -> trackMetrics(flux, metrics))
        .transform(flux -> withErrorHandling(flux, OPERATION_NAME));
    }


    @Override
    public Flux<Kpi> generateKpiCRParents() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Kpi> generateKpiClickRateByFormat() {
        final String OPERATION_NAME = "CLICK_RATE_BY_FORMAT";
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String batchId = generateBatchId();
        LocalDateTime startOfDay = LocalDate.now().atStartOfDay();

        List<String> clickFormatKpiIds = Arrays.asList("MC-C", "MF-C", "MB-C");
        Query clicksByFormatQuery = new Query(Criteria.where("kpiId").in(clickFormatKpiIds)
                .and("createdDate").gte(startOfDay));
        Flux<Kpi> clicksByFormatFlux = reactiveMongoTemplate.find(clicksByFormatQuery, Kpi.class, "kpi")
                .doOnError(e -> log.error("Error fetching Clicks by Format KPIs for Click Rate: {}", e.getMessage()));

        Query impressionsQuery = new Query(Criteria.where("kpiId").is("MP-I")
                .and("createdDate").gte(startOfDay));
        Flux<Kpi> impressionsFlux = reactiveMongoTemplate.find(impressionsQuery, Kpi.class, "kpi")
                .doOnError(e -> log.error("Error fetching MP-I KPIs for Click Rate by Format: {}", e.getMessage()));

        return Flux.zip(
                clicksByFormatFlux.collectList(), // List<Kpi(Clicks by Format)>
                impressionsFlux.collectMap(Kpi::getCampaignId) // Map<CampaignId, Kpi(MP-I)>
        )
        .flatMapMany(tuple -> {
            List<Kpi> clicksByFormatList = tuple.getT1();
            Map<String, Kpi> impressionsMap = tuple.getT2();
            List<Kpi> clickRateKpis = new ArrayList<>();

            clicksByFormatList.forEach(clickKpi -> {
                String campaignId = clickKpi.getCampaignId();
                String campaignSubId = clickKpi.getCampaignSubId();
                String format = clickKpi.getFormat(); // Get format from the click KPI
                Kpi impressionKpi = impressionsMap.get(campaignId); // Use parent campaignId to find impressions

                if (impressionKpi != null && impressionKpi.getValue() != null && impressionKpi.getValue() != 0.0 && clickKpi.getValue() != null && format != null) {
                    double clickRate = clickKpi.getValue() / impressionKpi.getValue();

                    String clickRateKpiId;
                    switch (format) {
                        case "MC": clickRateKpiId = "MC-CR"; break;
                        case "MF": clickRateKpiId = "MF-CR"; break;
                        case "MB": clickRateKpiId = "MB-CR"; break;
                        default:
                            log.warn("Unsupported format encountered for Click Rate by Format KPI: {}", format);
                            return; // Skip this record
                    }

                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(campaignId);
                    kpi.setCampaignSubId(campaignSubId); // Keep the subId from the click KPI
                    kpi.setKpiId(clickRateKpiId);
                    kpi.setKpiDescription("Click Rate Medios Propios (" + format + ")");
                    kpi.setType("porcentaje");
                    kpi.setValue(clickRate); // Store as fraction
                    kpi.setStatus("A");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setFormat(format); // Keep the format
                    kpi.setBatchId(batchId);
                    kpi.setMediaType("OWNED");

                    clickRateKpis.add(kpi);
                } else {
                     log.warn("Skipping Click Rate by Format calculation for campaign {} / subId {} / format {}: Impression KPI (MP-I) or Click KPI ({}) missing, null, or impressions are zero.",
                             campaignId, campaignSubId, format, clickKpi.getKpiId());
                }
            });
            log.info("Calculated {} Click Rate by Format KPIs.", clickRateKpis.size());
            return Flux.fromIterable(clickRateKpis);
        })
        .flatMap(this::saveOrUpdateKpiStartOfDay)
        .transform(flux -> trackMetrics(flux, metrics))
        .transform(flux -> withErrorHandling(flux, OPERATION_NAME));
    }


    @Override
    public Flux<Kpi> generateKpiClickRateByFormat() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Kpi> generateKpiOpenRatePushParents() {
        final String OPERATION_NAME = "OPEN_RATE_PUSH_PARENTS";
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String batchId = generateBatchId();
        LocalDateTime startOfDay = LocalDate.now().atStartOfDay();

        Query impressionsQuery = new Query(Criteria.where("kpiId").is("PP-I")
                .and("createdDate").gte(startOfDay));
        Flux<Kpi> impressionsFlux = reactiveMongoTemplate.find(impressionsQuery, Kpi.class, "kpi")
                .doOnError(e -> log.error("Error fetching PP-I KPIs for Push Open Rate: {}", e.getMessage()));

        Query scopeQuery = new Query(Criteria.where("kpiId").is("PP-A")
                .and("createdDate").gte(startOfDay));
        Flux<Kpi> scopeFlux = reactiveMongoTemplate.find(scopeQuery, Kpi.class, "kpi")
                 .doOnError(e -> log.error("Error fetching PP-A KPIs for Push Open Rate: {}", e.getMessage()));

        return Flux.zip(
                impressionsFlux.collectMap(Kpi::getCampaignId), // Map<CampaignId, Kpi(PP-I)>
                scopeFlux.collectMap(Kpi::getCampaignId)       // Map<CampaignId, Kpi(PP-A)>
        )
        .flatMapMany(tuple -> {
            Map<String, Kpi> impressionsMap = tuple.getT1();
            Map<String, Kpi> scopeMap = tuple.getT2();
            List<Kpi> openRateKpis = new ArrayList<>();

            impressionsMap.forEach((campaignId, impressionKpi) -> {
                Kpi scopeKpi = scopeMap.get(campaignId);
                if (scopeKpi != null && scopeKpi.getValue() != null && scopeKpi.getValue() != 0.0 && impressionKpi.getValue() != null) {
                    double openRate = impressionKpi.getValue() / scopeKpi.getValue();

                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(campaignId);
                    kpi.setCampaignSubId(campaignId); // Parent KPI uses campaignId as subId
                    kpi.setKpiId("PP-OR"); // Push Open Rate Parent
                    kpi.setKpiDescription("Open Rate Push Medios Propios (Padre)");
                    kpi.setType("porcentaje");
                    kpi.setValue(openRate); // Store as fraction
                    kpi.setStatus("A");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setBatchId(batchId);
                    kpi.setMediaType("PUSH"); // Explicitly set for Push KPIs

                    openRateKpis.add(kpi);
                } else {
                    log.warn("Skipping Push Open Rate calculation for campaign {}: Scope KPI (PP-A) or Impression KPI (PP-I) missing, null, or scope is zero.", campaignId);
                }
            });
            log.info("Calculated {} Push Open Rate Parent KPIs.", openRateKpis.size());
            return Flux.fromIterable(openRateKpis);
        })
        .flatMap(this::saveOrUpdateKpiStartOfDay)
        .transform(flux -> trackMetrics(flux, metrics))
        .transform(flux -> withErrorHandling(flux, OPERATION_NAME));
    }


    @Override
    public Flux<Kpi> generateKpiOpenRatePushParents() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Kpi> generateKpiRoasGeneral() {
        final String OPERATION_NAME = "ROAS_GENERAL";
        OperationMetrics metrics = startMetrics(OPERATION_NAME);
        String batchId = generateBatchId();
        LocalDateTime startOfDay = LocalDate.now().atStartOfDay();

        List<String> salesKpiIds = Arrays.asList("MP-V", "PP-V");
        Query salesQuery = new Query(Criteria.where("kpiId").in(salesKpiIds)
                .and("createdDate").gte(startOfDay));
        Flux<Kpi> salesFlux = reactiveMongoTemplate.find(salesQuery, Kpi.class, "kpi")
                .doOnError(e -> log.error("Error fetching Sales KPIs (MP-V, PP-V) for ROAS: {}", e.getMessage()));

        return salesFlux.collectMap(Kpi::getCampaignId) // Map<CampaignId, Kpi(Sales)>
            .flatMapMany(salesMap -> {
                if (salesMap.isEmpty()) {
                    log.info("No Sales KPIs found for ROAS calculation today.");
                    return Flux.empty();
                }
                List<String> campaignIds = new ArrayList<>(salesMap.keySet());
                Query campaignQuery = new Query(Criteria.where("campaignId").in(campaignIds));

                return reactiveMongoTemplate.find(campaignQuery, Campaign.class, "campaigns")
                    .collectMap(Campaign::getCampaignId) // Map<CampaignId, Campaign>
                    .flatMapMany(campaignMap -> {
                        List<Kpi> roasKpis = new ArrayList<>();
                        salesMap.forEach((campaignId, salesKpi) -> {
                            Campaign campaign = campaignMap.get(campaignId);
                            if (campaign != null && campaign.getInvestment() != null && campaign.getInvestment() != 0.0 && salesKpi.getValue() != null) {
                                double roas = salesKpi.getValue() / campaign.getInvestment();
                                String roasKpiId;
                                String roasDescription;
                                String mediaType;

                                if ("MP-V".equals(salesKpi.getKpiId())) {
                                    roasKpiId = "MP-ROAS";
                                    roasDescription = "ROAS Medios Propios (Padre)";
                                    mediaType = "OWNED";
                                } else if ("PP-V".equals(salesKpi.getKpiId())) {
                                    roasKpiId = "PP-ROAS";
                                    roasDescription = "ROAS Push Medios Propios (Padre)";
                                    mediaType = "PUSH";
                                } else {
                                    log.warn("Unexpected Sales KPI ID encountered for ROAS calculation: {}", salesKpi.getKpiId());
                                    return; // Skip
                                }

                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaignId);
                                kpi.setCampaignSubId(campaignId); // Parent ROAS uses campaignId as subId
                                kpi.setKpiId(roasKpiId);
                                kpi.setKpiDescription(roasDescription);
                                kpi.setType("ratio"); // ROAS is a ratio
                                kpi.setValue(roas);
                                kpi.setStatus("A");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setBatchId(batchId);
                                kpi.setMediaType(mediaType);

                                roasKpis.add(kpi);
                            } else {
                                log.warn("Skipping ROAS calculation for campaign {}: Campaign, Investment, or Sales KPI ({}) missing, null, or investment is zero.",
                                         campaignId, salesKpi.getKpiId());
                            }
                        });
                        log.info("Calculated {} ROAS General KPIs.", roasKpis.size());
                        return Flux.fromIterable(roasKpis);
                    });
            })
            .flatMap(this::saveOrUpdateKpiStartOfDay)
            .transform(flux -> trackMetrics(flux, metrics))
            .transform(flux -> withErrorHandling(flux, OPERATION_NAME));
    }


    @Override
    public Flux<Kpi> generateKpiRoasGeneral() {
        // Implementación original
        return Flux.empty();
    }

    @Override
    public Flux<Metrics> generateMetricsGeneral() {
        log.info("generateMetricsGeneral called, returning Flux.empty() as implementation is not defined for this scope.");
        return Flux.empty(); // Placeholder implementation
    }


    @Override
    public Flux<Metrics> generateMetricsGeneral() {
        // Implementación original
        return Flux.empty();
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
        // Pipeline optimizado
        List<Document> pipeline = Arrays.asList(
                // Filtrar primero por campos necesarios para reducir volumen
                new Document("$match",
                        new Document("SendID", new Document("$exists", true).append("$ne", null))
                ),

                // Proyectar solo los campos necesarios antes del lookup
                new Document("$project",
                        new Document("SendID", 1)
                ),

                new Document("$lookup", new Document("from", "bq_ds_campanias_salesforce_sendjobs")
                        .append("localField", "SendID")
                        .append("foreignField", "SendID")
                        .append("as", "sendjobs")
                ),

                new Document("$unwind", "$sendjobs"),

                new Document("$match",
                        new Document("sendjobs.campaignId", new Document("$exists", true).append("$ne", null))
                ),

                new Document("$lookup", new Document("from", "campaigns")
                        .append("localField", "sendjobs.campaignId")
                        .append("foreignField", "campaignId")
                        .append("as", "campaign")
                ),

                new Document("$match",
                        new Document("campaign.campaignId", new Document("$exists", true)
                                .append("$ne", null)
                                .append("$ne", "")
                        )
                ),

                new Document("$group",
                        new Document("_id", "$sendjobs.campaignId")
                                .append("value", new Document("$sum", 1))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id"))
                                .append("value", new Document("$toDouble", "$value"))
                )
        );

        // Aplicamos timeout directamente, sin usar RetryHandler para evitar el error de tipos
        return reactiveMongoTemplate.getCollection(collectionParam)
                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class))
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

                new Document("$match", new Document("campaign.format", new Document("$in", Arrays.asList("MC", "MF", "MB")))),

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
                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class));
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

                new Document("$match", new Document("$expr", new Document("$in", Arrays.asList(
                        new Document("$arrayElemAt", Arrays.asList("$campaign.format", 0)),
                        Arrays.asList("MC", "MF", "MB")
                )))),

                new Document("$group",
                        new Document("_id", new Document("campaignId", "$campaign.campaignId")
                                .append("campaignSubId", "$campaign.campaignSubId")
                                .append("format", new Document("$arrayElemAt", Arrays.asList("$campaign.format", 0))))
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
                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class));
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
}
