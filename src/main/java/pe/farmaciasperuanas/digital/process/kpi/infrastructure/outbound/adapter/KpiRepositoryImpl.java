package pe.farmaciasperuanas.digital.process.kpi.infrastructure.outbound.adapter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
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

import org.bson.Document;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
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
 * <li>Apr 12, 2025 Implementación mejorada para KPIs específicos.</li>
 * </ul>
 * @version 1.0
 */
@Repository
@Slf4j
@RequiredArgsConstructor
public class KpiRepositoryImpl implements KpiRepository {

    @Autowired
    private ReactiveMongoTemplate reactiveMongoTemplate;

    private static final String MEDIO_PROPIO = "MEDIO_PROPIO";
    private static final String MEDIO_PAGADO = "MEDIO_PAGADO";

    // Constantes para formatos
    private static final String FORMAT_MP = "MP";
    private static final String FORMAT_MC = "MC";
    private static final String FORMAT_MF = "MF";
    private static final String FORMAT_MB = "MB";
    private static final String FORMAT_PA = "PA";
    private static final String FORMAT_PW = "PW";

    // Formatos agrupados
    private static final List<String> EMAIL_FORMATS = Arrays.asList(FORMAT_MC, FORMAT_MF, FORMAT_MB);
    private static final List<String> PUSH_FORMATS = Arrays.asList(FORMAT_PA, FORMAT_PW);

    /**
     * Genera un ID de lote único
     * @return String con el ID de lote
     */
    private String generateBatchId() {
        return "BATCH_" + UUID.randomUUID().toString();
    }

    /**
     * Implementación del método para generar KPIs de impresiones para todos los formatos
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    @Override
    public Flux<Kpi> generateKpiImpressions() {
        String batchId = generateBatchId();
        log.info("Generando KPIs de impresiones para todos los formatos. Batch ID: {}", batchId);

        return Flux.concat(
                generateKpiImpressionsMailingParent(batchId),
                generateKpiImpressionsPushApp(batchId),
                generateKpiImpressionsPushWeb(batchId)
        );
    }

    /**
     * Genera KPIs de impresiones (aperturas) para el formato Mailing Padre (MP)
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiImpressionsMailingParent(String batchId) {
        log.info("Generando KPI de impresiones (aperturas) para Mailing Padre (MP)");

        // Realizar agregación a través de ReactiveMongoTemplate usando SpringData
        Aggregation aggregation = Aggregation.newAggregation(
                // Lookup para unir con sendjobs
                Aggregation.lookup("bq_ds_campanias_salesforce_sendjobs", "SendID", "SendID", "sendjobs"),
                // Desplegar el array de sendjobs
                Aggregation.unwind("sendjobs"),
                // Filtrar documentos donde campaignId existe
                Aggregation.match(Criteria.where("sendjobs.campaignId").exists(true).ne(null)),
                // Lookup para unir con campaigns
                Aggregation.lookup("campaigns", "sendjobs.campaignId", "campaignId", "campaign"),
                // Desplegar el array de campaign
                Aggregation.unwind("campaign"),
                // Filtrar campañas válidas
                Aggregation.match(Criteria.where("campaign.campaignId").exists(true).ne(null).ne("")),
                // Agrupar por campaignId y contar
                Aggregation.group("campaign.campaignId")
                        .first("campaign.providerId").as("providerId")
                        .first("campaign.name").as("campaignName")
                        .count().as("value"),
                // Proyectar los campos finales
                Aggregation.project()
                        .andExpression("_id").as("campaignId")
                        .andExpression("providerId").as("providerId")
                        .andExpression("campaignName").as("campaignName")
                        .andExpression("value").as("value")
        );

        return reactiveMongoTemplate.aggregate(
                        aggregation,
                        "bq_ds_campanias_salesforce_opens",
                        ImpressionsResult.class) // Clase auxiliar para mapear el resultado
                .map(result -> {
                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(result.getCampaignId());
                    kpi.setCampaignSubId(result.getCampaignId());
                    kpi.setKpiId("MP-I");
                    kpi.setKpiDescription("Impresiones (Aperturas)");
                    kpi.setValue(Double.valueOf(result.getValue()));
                    kpi.setType("cantidad");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setStatus("A");
                    kpi.setFormat(FORMAT_MP);
                    kpi.setBatchId(batchId);
                    kpi.setTypeMedia(MEDIO_PROPIO);
                    return kpi;
                })
                .flatMap(this::saveKpi);
    }

    /**
     * Genera KPIs de impresiones (aperturas) para el formato Push App (PA)
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiImpressionsPushApp(String batchId) {
        log.info("Generando KPI de impresiones (aperturas) para Push App (PA)");

        // Usar fechas para filtrar últimos 12 meses (31536000000L = 365 días en milisegundos)
        java.util.Date startDate = new java.util.Date(new java.util.Date().getTime() - 31536000000L);

        // Esta agregación ahora se asemeja más al query de BigQuery pero utilizando campaignId directamente
        Aggregation aggregation = Aggregation.newAggregation(
                // Match inicial para fechas y estado (similar a WHERE en SQL)
                Aggregation.match(new Criteria().andOperator(
                        Criteria.where("FechaProceso").gte(startDate),
                        Criteria.where("DateTimeSend").gte(startDate),
                        Criteria.where("Status").is("Success"),
                        Criteria.where("OpenDate").exists(true).ne(null)
                )),
                // Lookup para unir con tabla equivalente a tabla_fape_connect
                Aggregation.lookup("campaigns", "campaignId", "campaignId", "campaign"),
                // Desplegar el array de campaign
                Aggregation.unwind("campaign"),
                // Filtrar para Push App
                Aggregation.match(Criteria.where("campaign.format").is(FORMAT_PA)),
                // Agrupar por campaignId para contar
                Aggregation.group("campaign.campaignId")
                        .first("campaign.providerId").as("providerId")
                        .first("campaign.name").as("campaignName")
                        .count().as("value"),
                // Proyectar los campos finales
                Aggregation.project()
                        .andExpression("_id").as("campaignId")
                        .andExpression("providerId").as("providerId")
                        .andExpression("campaignName").as("campaignName")
                        .andExpression("value").as("value")
        );

        return reactiveMongoTemplate.aggregate(
                        aggregation,
                        "bq_ds_campanias_salesforce_push",
                        ImpressionsResult.class)
                .map(result -> {
                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(result.getCampaignId());
                    kpi.setCampaignSubId(result.getCampaignId());
                    kpi.setKpiId("PA-I");
                    kpi.setKpiDescription("Impresiones (Aperturas)");
                    kpi.setValue(Double.valueOf(result.getValue()));
                    kpi.setType("cantidad");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setStatus("A");
                    kpi.setFormat(FORMAT_PA);
                    kpi.setBatchId(batchId);
                    kpi.setTypeMedia(MEDIO_PROPIO);
                    return kpi;
                })
                .flatMap(this::saveKpi);
    }
    /**
     * Genera KPIs de impresiones (aperturas) para el formato Push Web (PW)
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiImpressionsPushWeb(String batchId) {
        log.info("Generando KPI de impresiones (aperturas) para Push Web (PW)");

        // Usar fechas para filtrar últimos 12 meses (31536000000L = 365 días en milisegundos)
        java.util.Date startDate = new java.util.Date(new java.util.Date().getTime() - 31536000000L);

        // Esta agregación ahora se asemeja más al query de BigQuery pero utilizando campaignId directamente
        Aggregation aggregation = Aggregation.newAggregation(
                // Match inicial para fechas y estado (similar a WHERE en SQL)
                Aggregation.match(new Criteria().andOperator(
                        Criteria.where("FechaProceso").gte(startDate),
                        Criteria.where("DateTimeSend").gte(startDate),
                        Criteria.where("Status").is("Success"),
                        Criteria.where("OpenDate").exists(true).ne(null)
                )),
                // Lookup para unir con tabla equivalente a tabla_fape_connect
                Aggregation.lookup("campaigns", "campaignId", "campaignId", "campaign"),
                // Desplegar el array de campaign
                Aggregation.unwind("campaign"),
                // Filtrar para Push App
                Aggregation.match(Criteria.where("campaign.format").is(FORMAT_PA)),
                // Agrupar por campaignId para contar
                Aggregation.group("campaign.campaignId")
                        .first("campaign.providerId").as("providerId")
                        .first("campaign.name").as("campaignName")
                        .count().as("value"),
                // Proyectar los campos finales
                Aggregation.project()
                        .andExpression("_id").as("campaignId")
                        .andExpression("providerId").as("providerId")
                        .andExpression("campaignName").as("campaignName")
                        .andExpression("value").as("value")
        );

        return reactiveMongoTemplate.aggregate(
                        aggregation,
                        "bq_ds_campanias_salesforce_push",
                        ImpressionsResult.class)
                .map(result -> {
                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(result.getCampaignId());
                    kpi.setCampaignSubId(result.getCampaignId());
                    kpi.setKpiId("PA-I");
                    kpi.setKpiDescription("Impresiones (Aperturas)");
                    kpi.setValue(Double.valueOf(result.getValue()));
                    kpi.setType("cantidad");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setStatus("A");
                    kpi.setFormat(FORMAT_PA);
                    kpi.setBatchId(batchId);
                    kpi.setTypeMedia(MEDIO_PROPIO);
                    return kpi;
                })
                .flatMap(this::saveKpi);
    }

    // Clase auxiliar para mapear resultados de impresiones
    private static class ImpressionsResult {
        private String campaignId;
        private String providerId;
        private String campaignName;
        private int value;

        public String getCampaignId() {
            return campaignId;
        }

        public void setCampaignId(String campaignId) {
            this.campaignId = campaignId;
        }

        public String getProviderId() {
            return providerId;
        }

        public void setProviderId(String providerId) {
            this.providerId = providerId;
        }

        public String getCampaignName() {
            return campaignName;
        }

        public void setCampaignName(String campaignName) {
            this.campaignName = campaignName;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }
    }
    /**
     * Implementación del método para generar KPIs de alcance para todos los formatos
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    @Override
    public Flux<Kpi> generateKpiScope() {
        String batchId = generateBatchId();
        log.info("Generando KPIs de alcance para todos los formatos. Batch ID: {}", batchId);

        return Flux.concat(
                generateKpiScopeMailingParent(batchId),
                generateKpiScopePushApp(batchId),
                generateKpiScopePushWeb(batchId)
        );
    }

    /**
     * Genera KPIs de alcance (envíos) para el formato Mailing Padre (MP)
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiScopeMailingParent(String batchId) {
        log.info("Generando KPI de alcance (envíos) para Mailing Padre (MP)");

        Aggregation aggregation = Aggregation.newAggregation(
                // Lookup para unir con sendjobs
                Aggregation.lookup("bq_ds_campanias_salesforce_sendjobs", "SendID", "SendID", "sendjobs"),
                // Desplegar el array de sendjobs
                Aggregation.unwind("sendjobs"),
                // Filtrar documentos donde campaignId existe
                Aggregation.match(Criteria.where("sendjobs.campaignId").exists(true).ne(null)),
                // Lookup para unir con campaigns
                Aggregation.lookup("campaigns", "sendjobs.campaignId", "campaignId", "campaign"),
                // Desplegar el array de campaign
                Aggregation.unwind("campaign"),
                // Filtrar campañas válidas
                Aggregation.match(Criteria.where("campaign.campaignId").exists(true).ne(null).ne("")),
                // Agrupar por campaignId y contar
                Aggregation.group("campaign.campaignId")
                        .first("campaign.providerId").as("providerId")
                        .first("campaign.name").as("campaignName")
                        .count().as("value"),
                // Proyectar los campos finales
                Aggregation.project()
                        .andExpression("_id").as("campaignId")
                        .andExpression("providerId").as("providerId")
                        .andExpression("campaignName").as("campaignName")
                        .andExpression("value").as("value")
        );

        return reactiveMongoTemplate.aggregate(
                        aggregation,
                        "bq_ds_campanias_salesforce_sents",
                        ScopeResult.class)
                .map(result -> {
                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(result.getCampaignId());
                    kpi.setCampaignSubId(result.getCampaignId());
                    kpi.setKpiId("MP-A");
                    kpi.setKpiDescription("Alcance (Envíos)");
                    kpi.setValue(Double.valueOf(result.getValue()));
                    kpi.setType("cantidad");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setStatus("A");
                    kpi.setFormat(FORMAT_MP);
                    kpi.setBatchId(batchId);
                    kpi.setTypeMedia(MEDIO_PROPIO);
                    return kpi;
                })
                .flatMap(this::saveKpi);
    }

    /**
     * Genera KPIs de alcance (envíos) para el formato Push App (PA)
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiScopePushApp(String batchId) {
        log.info("Generando KPI de alcance (envíos) para Push App (PA)");

        // Usar fechas para filtrar últimos 12 meses
        java.util.Date startDate = new java.util.Date(new java.util.Date().getTime() - 31536000000L);

        // Construir criteria por separado para cada condición
        Criteria formatCriteria = Criteria.where("campaign.format").is(FORMAT_PA);
        Criteria validFieldsCriteria = new Criteria().andOperator(
                Criteria.where("FechaProceso").exists(true).ne(null),
                Criteria.where("DateTimeSend").exists(true).ne(null),
                Criteria.where("Status").is("Success")
        );
        Criteria dateCriteria = new Criteria().andOperator(
                Criteria.where("FechaProceso").gte(startDate),
                Criteria.where("DateTimeSend").gte(startDate)
        );

        // Combinar criterias con andOperator
        Criteria combinedCriteria = new Criteria().andOperator(
                formatCriteria,
                validFieldsCriteria,
                dateCriteria
        );

        Aggregation aggregation = Aggregation.newAggregation(
                // Lookup para unir con campaigns
                Aggregation.lookup("campaigns", "campaignId", "campaignId", "campaign"),
                // Desplegar el array de campaign
                Aggregation.unwind("campaign"),
                // Aplicar todos los criterios combinados
                Aggregation.match(combinedCriteria),
                // Agrupar por campaignId y contar
                Aggregation.group("campaign.campaignId")
                        .first("campaign.providerId").as("providerId")
                        .first("campaign.name").as("campaignName")
                        .count().as("value"),
                // Proyectar los campos finales
                Aggregation.project()
                        .andExpression("_id").as("campaignId")
                        .andExpression("providerId").as("providerId")
                        .andExpression("campaignName").as("campaignName")
                        .andExpression("value").as("value")
        );

        return reactiveMongoTemplate.aggregate(
                        aggregation,
                        "bq_ds_campanias_salesforce_push",
                        ScopeResult.class)
                .map(result -> {
                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(result.getCampaignId());
                    kpi.setCampaignSubId(result.getCampaignId());
                    kpi.setKpiId("PA-A");
                    kpi.setKpiDescription("Alcance (Envíos)");
                    kpi.setValue(Double.valueOf(result.getValue()));
                    kpi.setType("cantidad");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setStatus("A");
                    kpi.setFormat(FORMAT_PA);
                    kpi.setBatchId(batchId);
                    kpi.setTypeMedia(MEDIO_PROPIO);
                    return kpi;
                })
                .flatMap(this::saveKpi);
    }

    /**
     * Genera KPIs de alcance (envíos) para el formato Push Web (PW)
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiScopePushWeb(String batchId) {
        log.info("Generando KPI de alcance (envíos) para Push Web (PW)");

        // Usar fechas para filtrar últimos 12 meses
        java.util.Date startDate = new java.util.Date(new java.util.Date().getTime() - 31536000000L);

        // Construir criteria por separado para cada condición
        Criteria formatCriteria = Criteria.where("campaign.format").is(FORMAT_PW);
        Criteria validFieldsCriteria = new Criteria().andOperator(
                Criteria.where("FechaProceso").exists(true).ne(null),
                Criteria.where("DateTimeSend").exists(true).ne(null),
                Criteria.where("Status").is("Success")
        );
        Criteria dateCriteria = new Criteria().andOperator(
                Criteria.where("FechaProceso").gte(startDate),
                Criteria.where("DateTimeSend").gte(startDate)
        );

        // Combinar criterias con andOperator
        Criteria combinedCriteria = new Criteria().andOperator(
                formatCriteria,
                validFieldsCriteria,
                dateCriteria
        );

        Aggregation aggregation = Aggregation.newAggregation(
                // Lookup para unir con campaigns
                Aggregation.lookup("campaigns", "campaignId", "campaignId", "campaign"),
                // Desplegar el array de campaign
                Aggregation.unwind("campaign"),
                // Aplicar todos los criterios combinados
                Aggregation.match(combinedCriteria),
                // Agrupar por campaignId y contar
                Aggregation.group("campaign.campaignId")
                        .first("campaign.providerId").as("providerId")
                        .first("campaign.name").as("campaignName")
                        .count().as("value"),
                // Proyectar los campos finales
                Aggregation.project()
                        .andExpression("_id").as("campaignId")
                        .andExpression("providerId").as("providerId")
                        .andExpression("campaignName").as("campaignName")
                        .andExpression("value").as("value")
        );

        return reactiveMongoTemplate.aggregate(
                        aggregation,
                        "bq_ds_campanias_salesforce_push",
                        ScopeResult.class)
                .map(result -> {
                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(result.getCampaignId());
                    kpi.setCampaignSubId(result.getCampaignId());
                    kpi.setKpiId("PW-A");
                    kpi.setKpiDescription("Alcance (Envíos)");
                    kpi.setValue(Double.valueOf(result.getValue()));
                    kpi.setType("cantidad");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setStatus("A");
                    kpi.setFormat(FORMAT_PW);
                    kpi.setBatchId(batchId);
                    kpi.setTypeMedia(MEDIO_PROPIO);
                    return kpi;
                })
                .flatMap(this::saveKpi);
    }

    /**
     * Guarda un KPI en la base de datos
     * @param kpi KPI a guardar
     * @return Mono<Kpi> KPI guardado
     */
    private Mono<Kpi> saveKpi(Kpi kpi) {
        Query query = new Query()
                .addCriteria(Criteria.where("kpiId").is(kpi.getKpiId())
                        .and("campaignId").is(kpi.getCampaignId())
                        .and("campaignSubId").is(kpi.getCampaignSubId()));

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
                .set("updatedDate", kpi.getUpdatedDate())
                .set("format", kpi.getFormat())
                .set("batchId", kpi.getBatchId())
                .set("typeMedia", kpi.getTypeMedia());

        return reactiveMongoTemplate.upsert(query, update, Kpi.class)
                .thenReturn(kpi);
    }

    // Clase auxiliar para mapear resultados de alcance (scope)
    private static class ScopeResult {
        private String campaignId;
        private String providerId;
        private String campaignName;
        private int value;

        public String getCampaignId() {
            return campaignId;
        }

        public void setCampaignId(String campaignId) {
            this.campaignId = campaignId;
        }

        public String getProviderId() {
            return providerId;
        }

        public void setProviderId(String providerId) {
            this.providerId = providerId;
        }

        public String getCampaignName() {
            return campaignName;
        }

        public void setCampaignName(String campaignName) {
            this.campaignName = campaignName;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }
    }











































































    /**
     * Método principal para generar todas las métricas secuencialmente
     */
    public Mono<Void> generateAllMetrics() {
        System.out.println("Iniciando proceso de generación de todas las métricas...");

        // Ejecutamos cada método de generación de métricas en secuencia
        return generateMetricsGeneral()
                .thenMany(generateInvestmentMetrics())
                .doOnComplete(() -> System.out.println("Proceso de generación de todas las métricas completado exitosamente"))
                .doOnError(error -> System.err.println("Error en generateAllMetrics: " + error.getMessage()))
                .then();
    }

    /**
     * Genera métricas generales por proveedor para el día actual
     */

    public Flux<Metrics> generateMetricsGeneral() {
        System.out.println("Iniciando cálculo de totalSales por proveedor para la fecha actual...");

        // Obtenemos la fecha actual en el formato correcto
        LocalDate today = LocalDate.now();
        LocalDateTime startOfDay = today.atStartOfDay();
        LocalDateTime endOfDay = today.atTime(23, 59, 59);

        System.out.println("Filtrando KPIs entre: " + startOfDay + " y " + endOfDay);

        // Primero obtenemos todos los providers
        return reactiveMongoTemplate.findAll(Provider.class)
                .flatMap(provider -> {
                    // Extraemos los datos del provider
                    String providerId = provider.getProviderId();
                    String providerName = provider.getName();

                    System.out.println("Procesando proveedor: " + providerId + " - " + providerName);

                    // Criterio para buscar campañas relacionadas con este providerId
                    // Añadimos el filtro para excluir campañas con status "Finalizado"
                    Query campaignsQuery = Query.query(
                            Criteria.where("providerId").is(providerId)
                                    .and("status").ne("Finalizado")
                    );

                    return reactiveMongoTemplate.find(campaignsQuery, Campaign.class)
                            .collectList()
                            .flatMap(campaigns -> {
                                if (campaigns.isEmpty()) {
                                    System.out.println("No se encontraron campañas activas para el proveedor: " + providerId);
                                    return createEmptyMetrics(providerId);
                                }

                                // Extraemos los campaignId de las campañas encontradas
                                List<String> campaignIds = campaigns.stream()
                                        .map(Campaign::getCampaignId)
                                        .filter(id -> id != null && !id.isBlank())
                                        .collect(Collectors.toList());

                                System.out.println("Proveedor: " + providerId + " | Campañas activas encontradas: " +
                                        campaigns.size() + " | IDs válidos: " + campaignIds.size());

                                if (campaignIds.isEmpty()) {
                                    System.out.println("No hay campaignIds válidos para el proveedor: " + providerId);
                                    return createEmptyMetrics(providerId);
                                }

                                // Criterio para KPIs por campaignIds, fecha y tipos específicos de KPI
                                Criteria kpiCriteria = Criteria.where("campaignId").in(campaignIds)
                                        .and("kpiId").in(Arrays.asList("MP-V", "PW-V", "PA-V"))
                                        .and("createdDate").gte(startOfDay).lte(endOfDay);

                                // Agregación para sumar los valores de KPI
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
                                            return createMetricsObject(providerId, totalSales);
                                        })
                                        .switchIfEmpty(createEmptyMetrics(providerId));
                            });
                })
                .doOnNext(metrics -> {
                    System.out.println("Calculado - Proveedor ID: " + metrics.getProviderId() +
                            " | Total Ventas del día: " + metrics.getTotalSales());
                })
                .flatMap(metrics -> {
                    LocalDateTime now = LocalDateTime.now();
                    LocalDateTime startOfDayx = now.toLocalDate().atStartOfDay();
                    LocalDateTime endOfDayx = now.toLocalDate().atTime(23, 59, 59);

                    return findMetricsByProviderIdAndDate(metrics.getProviderId(), startOfDayx, endOfDayx)
                            .flatMap(existingMetric -> {
                                existingMetric.setTotalSales(metrics.getTotalSales());
                                existingMetric.setUpdatedDate(now);
                                return reactiveMongoTemplate.save(existingMetric);
                            })
                            .switchIfEmpty(
                                    Mono.fromCallable(() -> {
                                        metrics.setCreatedUser("-");
                                        metrics.setCreatedDate(now);
                                        metrics.setUpdatedDate(now);
                                        return metrics;
                                    }).flatMap(reactiveMongoTemplate::save)
                            );
                })
                .doOnComplete(() -> System.out.println("Proceso de cálculo de métricas generales completado"))
                .doOnError(error -> System.err.println("Error en generateMetricsGeneral: " + error.getMessage()));
    }

    /**
     * Obtiene métricas de inversión para el período actual
     */
    public Flux<Metrics> generateInvestmentMetrics() {
        System.out.println("Iniciando cálculo de métricas de inversión...");

        List<Document> pipeline = Arrays.asList(
                new Document("$lookup",
                        new Document("from", "campaigns")
                                .append("let",
                                        new Document("providerId", "$providerId")
                                )
                                .append("pipeline", Arrays.asList(
                                        new Document("$match",
                                                new Document("$expr",
                                                        new Document("$and", Arrays.asList(
                                                                new Document("$eq", Arrays.asList(
                                                                        new Document("$toString", "$providerId"),
                                                                        new Document("$toString", "$$providerId")
                                                                )),
                                                                new Document("$lte", Arrays.asList("$startDate", new Date())),
                                                                new Document("$gte", Arrays.asList("$endDate", new Date()))
                                                        ))
                                                )
                                        ),
                                        new Document("$group",
                                                new Document("_id", "$providerId")
                                                        .append("totalInvestmentPeriod", new Document("$sum", "$investment"))
                                        )
                                ))
                                .append("as", "investmentData")
                ),
                new Document("$addFields",
                        new Document("totalInvestmentPeriod",
                                new Document("$ifNull", Arrays.asList(
                                        new Document("$arrayElemAt", Arrays.asList("$investmentData.totalInvestmentPeriod", 0)), 0))
                        )
                ),
                new Document("$project",
                        new Document("_id", 0)
                                .append("providerId", new Document("$toString", "$providerId"))
                                .append("totalInvestmentPeriod", new Document("$toDouble", "$totalInvestmentPeriod"))
                )
        );

        return reactiveMongoTemplate.getCollection("providers")
                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class))
                .flatMap(this::saveOrUpdateInvestmentMetric)
                .doOnComplete(() -> System.out.println("Proceso de cálculo de métricas de inversión completado"))
                .doOnError(error -> System.err.println("Error en generateInvestmentMetrics: " + error.getMessage()));
    }


    /**
     * Extrae el valor totalSales del resultado de la agregación
     */
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

    /**
     * Crea un objeto Metrics vacío para un proveedor
     */
    private Mono<Metrics> createEmptyMetrics(String providerId) {
        Metrics metrics = Metrics.builder()
                .providerId(providerId)
                .totalSales(0.0)
                .build();
        return Mono.just(metrics);
    }

    /**
     * Crea un objeto Metrics con los valores calculados
     */
    private Metrics createMetricsObject(String providerId, Double totalSales) {
        return Metrics.builder()
                .providerId(providerId)
                .totalSales(totalSales)
                .build();
    }

    /**
     * Busca métricas existentes por providerId y fecha
     */
    private Mono<Metrics> findMetricsByProviderIdAndDate(String providerId, LocalDateTime startDate, LocalDateTime endDate) {
        Query query = new Query();
        query.addCriteria(Criteria.where("providerId").is(providerId)
                .and("createdDate").gte(startDate).lte(endDate));
        return reactiveMongoTemplate.findOne(query, Metrics.class);
    }

    /**
     * Guarda o actualiza métricas de inversión
     */
    private Mono<Metrics> saveOrUpdateInvestmentMetric(Document document) {
        String providerId = document.getString("providerId");
        Double totalInvestmentPeriod = document.getDouble("totalInvestmentPeriod");
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime startOfDay = now.toLocalDate().atStartOfDay();
        LocalDateTime endOfDay = now.toLocalDate().atTime(23, 59, 59);

        return findMetricsByProviderIdAndDate(providerId, startOfDay, endOfDay)
                .flatMap(existingMetric -> {
                    existingMetric.setTotalInvestmentPeriod(totalInvestmentPeriod);
                    existingMetric.setUpdatedDate(now);
                    return reactiveMongoTemplate.save(existingMetric);
                })
                .switchIfEmpty(
                        Mono.fromCallable(() -> {
                            Metrics newMetrics = new Metrics();
                            newMetrics.setProviderId(providerId);
                            newMetrics.setTotalInvestmentPeriod(totalInvestmentPeriod);
                            newMetrics.setCreatedUser("-");
                            newMetrics.setCreatedDate(now);
                            newMetrics.setUpdatedDate(now);
                            return newMetrics;
                        }).flatMap(reactiveMongoTemplate::save)
                );
    }

    /**
     * Guarda o actualiza métricas de ventas
     */
    private Mono<Metrics> saveOrUpdateSalesMetric(Document document) {
        String providerId = document.getString("providerId");
        Double totalSales = document.getDouble("totalSales");
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime startOfDay = now.toLocalDate().atStartOfDay();
        LocalDateTime endOfDay = now.toLocalDate().atTime(23, 59, 59);

        return findMetricsByProviderIdAndDate(providerId, startOfDay, endOfDay)
                .flatMap(existingMetric -> {
                    existingMetric.setTotalSales(totalSales);
                    existingMetric.setUpdatedDate(now);
                    return reactiveMongoTemplate.save(existingMetric);
                })
                .switchIfEmpty(
                        Mono.fromCallable(() -> {
                            Metrics newMetrics = new Metrics();
                            newMetrics.setProviderId(providerId);
                            newMetrics.setTotalSales(totalSales);
                            newMetrics.setCreatedUser("-");
                            newMetrics.setCreatedDate(now);
                            newMetrics.setUpdatedDate(now);
                            return newMetrics;
                        }).flatMap(reactiveMongoTemplate::save)
                );
    }
}