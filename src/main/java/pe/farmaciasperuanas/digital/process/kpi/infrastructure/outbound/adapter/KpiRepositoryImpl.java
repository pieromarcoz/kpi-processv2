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
import pe.farmaciasperuanas.digital.process.kpi.domain.model.CampaignMedium;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.Provider;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.KpiRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.bson.Document;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
 *
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
     *
     * @return String con el ID de lote
     */
    private String generateBatchId() {
        return "BATCH_" + UUID.randomUUID().toString();
    }

    /**
     * Implementación del método para generar KPIs de impresiones para todos los formatos
     *
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
     * Implementación del método para generar KPIs de alcance para todos los formatos
     *
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
     * Genera KPIs genericos para Push Web
     */
    @Override
    public Flux<Kpi> generatePushWebKpis() {
        String batchId = generateBatchId();
        return Flux.concat(
                generateKpiPushWebClicks(batchId),    // PW-C
                generateKpiPushWebOpenRate(batchId)   // PW-OR
        );
    }

    /**
     * Calcula clicks para un formato específico
     */
    private Mono<Double> calculateClicksByFormat(String campaignId, String format) {
        log.info("Calculando clicks para campaña: {} y formato: {}", campaignId, format);

        Aggregation aggregation = Aggregation.newAggregation(
                // Match para filtrar por campaignId
                Aggregation.match(Criteria.where("campaignId").is(campaignId)),

                // Group para contar clicks
                Aggregation.group()
                        .count().as("clickCount")
        );

        return reactiveMongoTemplate.aggregate(
                        aggregation,
                        format.equals(FORMAT_PW) ? "bq_ds_campanias_salesforce_push" : "bq_ds_campanias_salesforce_clicks",
                        Document.class)
                .next()
                .map(result -> {
                    Object clickCount = result.get("clickCount");
                    if (clickCount instanceof Number) {
                        return ((Number) clickCount).doubleValue();
                    }
                    return 0.0;
                })
                .defaultIfEmpty(0.0)
                .doOnNext(clicks ->
                        log.info("Clicks calculados para campaña {} y formato {}: {}",
                                campaignId, format, clicks));
    }

    /**
     * Genera KPI de clicks para Push Web
     */
    private Flux<Kpi> generateKpiPushWebClicks(String batchId) {
        log.info("Generando KPI de clicks para Push Web (PW)");

        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").ne("Finalizado")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_PW))),
                        Campaign.class
                )
                .flatMap(campaign -> {
                    // Obtener los clicks
                    return calculateClicksByFormat(campaign.getCampaignId(), FORMAT_PW)
                            .flatMap(clicks -> {
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("PW-C");
                                kpi.setKpiDescription("Clicks Push Web");
                                kpi.setValue(clicks);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_PW);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPI de Open Rate para Push Web
     */
    private Flux<Kpi> generateKpiPushWebOpenRate(String batchId) {
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("kpiId").in("PW-I", "PW-A")
                                .and("status").is("A")),
                        Kpi.class,
                        "kpi_v2"
                )
                .collectMultimap(Kpi::getCampaignId)
                .flatMapMany(kpisByCampaign -> {
                    List<Kpi> rateKpis = new ArrayList<>();

                    kpisByCampaign.forEach((campaignId, kpis) -> {
                        Double impressions = kpis.stream()
                                .filter(k -> k.getKpiId().equals("PW-I"))
                                .map(Kpi::getValue)
                                .findFirst()
                                .orElse(0.0);

                        Double scope = kpis.stream()
                                .filter(k -> k.getKpiId().equals("PW-A"))
                                .map(Kpi::getValue)
                                .findFirst()
                                .orElse(0.0);

                        if (scope > 0) {
                            double openRate = impressions / scope;
                            Kpi rateKpi = new Kpi();
                            rateKpi.setCampaignId(campaignId);
                            rateKpi.setCampaignSubId(campaignId);
                            rateKpi.setKpiId("PW-OR");
                            rateKpi.setKpiDescription("Open Rate Push Web");
                            rateKpi.setValue(openRate);
                            rateKpi.setType("porcentaje");
                            rateKpi.setCreatedUser("-");
                            rateKpi.setCreatedDate(LocalDateTime.now());
                            rateKpi.setUpdatedDate(LocalDateTime.now());
                            rateKpi.setStatus("A");
                            rateKpi.setFormat(FORMAT_PW);
                            rateKpi.setBatchId(batchId);
                            rateKpi.setTypeMedia(MEDIO_PROPIO);
                            rateKpis.add(rateKpi);
                        }
                    });

                    return Flux.fromIterable(rateKpis);
                })
                .flatMap(this::saveKpi);
    }

    /**
     * Versión optimizada para generar KPI de impresiones (aperturas) para Mailing Padre (MP)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiImpressionsMailingParent(String batchId) {
        log.info("Generando KPI de impresiones (aperturas) para Mailing Padre (MP) - Versión optimizada");

        // Primero obtenemos las campañas activas para reducir el scope de búsqueda
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").is("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_MP))),
                        Campaign.class
                )
                .collectList()
                .flatMapMany(campaigns -> {
                    if (campaigns.isEmpty()) {
                        log.info("No se encontraron campañas activas para Mailing Padre (MP)");
                        return Flux.empty();
                    }

                    log.info("Se encontraron {} campañas activas para MP-I", campaigns.size());

                    // Obtenemos primero los proveedores únicos
                    Set<String> providerIds = campaigns.stream()
                            .map(Campaign::getProviderId)
                            .filter(id -> id != null && !id.isEmpty())
                            .collect(Collectors.toSet());

                    log.info("Procesando KPIs para {} proveedores", providerIds.size());

                    List<Flux<Kpi>> kpiFluxes = new ArrayList<>();

                    // Procesamos cada proveedor individualmente
                    for (String providerId : providerIds) {
                        log.info("Procesando KPI para proveedor: {}", providerId);

                        // Filtramos las campañas solo para este proveedor
                        List<Campaign> providerCampaigns = campaigns.stream()
                                .filter(c -> providerId.equals(c.getProviderId()))
                                .collect(Collectors.toList());

                        if (providerCampaigns.isEmpty()) {
                            log.info("No hay campañas para el proveedor: {}", providerId);
                            continue;
                        }

                        log.info("Proveedor: {}, Número de campañas: {}", providerId, providerCampaigns.size());

                        // Este agregador debe replicar exactamente la consulta SQL:
                        // SELECT COUNT(DISTINCT a.EmailAddress)
                        // FROM bq_ds_campanias_salesforce_opens a
                        // INNER JOIN bq_ds_campanias_salesforce_sendjobs b ON a.SendID = b.SendID
                        // INNER JOIN Campaigns c ON c.CampaignId = b.CampaignId
                        // WHERE c.ProveedorId = 'ID';

                        Aggregation aggregation = Aggregation.newAggregation(
                                // Stage 1: Lookup para unir opens con sendjobs (INNER JOIN a con b)
                                Aggregation.lookup("bq_ds_campanias_salesforce_sendjobs", "SendID", "SendID", "sendjobs"),
                                // Stage 2: Desenrollar el array resultante (equivalente a INNER JOIN)
                                Aggregation.unwind("sendjobs"),

                                // Stage 3: Filtro para asegurarnos que sendjobs.campaignId existe
                                Aggregation.match(Criteria.where("sendjobs.campaignId").ne(null)),

                                // Stage 4: Lookup para unir con campañas (INNER JOIN con c)
                                // Esto es un poco complejo en MongoDB ya que necesitamos hacer otro lookup
                                Aggregation.lookup("campaigns", "sendjobs.campaignId", "campaignId", "campaign_info"),
                                // Stage 5: Desenrollar el array de campaign_info
                                Aggregation.unwind("campaign_info"),

//                                // Stage 6: Filtro por providerId (WHERE c.ProveedorId = 'ID')
//                                Aggregation.match(Criteria.where("campaign_info.providerId").is(providerId)),

                                // Stage 7: Agrupamos por EmailAddress para obtener registros únicos
                                // (COUNT DISTINCT)
                                Aggregation.group("EmailAddress"),

                                // Stage 8: Contamos los grupos para obtener el conteo total
                                Aggregation.group().count().as("count")
                        );

                        // Ejecutamos la agregación
                        Flux<Kpi> providerKpiFlux = reactiveMongoTemplate.aggregate(
                                        aggregation,
                                        "bq_ds_campanias_salesforce_opens",
                                        Document.class)
                                .doOnNext(result -> {
                                    Integer count = result.getInteger("count");
                                    log.info("Proveedor: {}, Conteo de EmailAddress únicos: {}", providerId, count);
                                })
                                .flatMap(result -> {
                                    // Obtenemos el conteo de direcciones únicas
                                    Integer count = result.getInteger("count");
                                    double value = count != null ? count.doubleValue() : 0.0;

                                    // Creamos un KPI para cada campaña de este proveedor con el mismo valor
                                    return Flux.fromIterable(providerCampaigns)
                                            .map(campaign -> {
                                                Kpi kpi = new Kpi();
                                                kpi.setCampaignId(campaign.getCampaignId());
                                                kpi.setCampaignSubId(campaign.getCampaignSubId());
                                                kpi.setKpiId("MP-I");
                                                kpi.setKpiDescription("Impresiones (Aperturas)");
                                                kpi.setValue(value);
                                                kpi.setType("cantidad");
                                                kpi.setCreatedUser("-");
                                                kpi.setCreatedDate(LocalDateTime.now());
                                                kpi.setUpdatedDate(LocalDateTime.now());
                                                kpi.setStatus("A");
                                                kpi.setFormat(FORMAT_MP);
                                                kpi.setBatchId(batchId);
                                                kpi.setTypeMedia(MEDIO_PROPIO);
                                                kpi.setProviderId(providerId);

                                                log.debug("Generando KPI para campaña: {}", campaign.getCampaignId());
                                                return kpi;
                                            });
                                })
                                // Si no hay resultados, creamos KPIs con valor 0
                                .switchIfEmpty(Flux.defer(() -> {
                                    log.info("No se encontraron aperturas para el proveedor: {}", providerId);
                                    return Flux.fromIterable(providerCampaigns)
                                            .map(campaign -> {
                                                Kpi kpi = new Kpi();
                                                kpi.setCampaignId(campaign.getCampaignId());
                                                kpi.setCampaignSubId(campaign.getCampaignSubId());
                                                kpi.setKpiId("MP-I");
                                                kpi.setKpiDescription("Impresiones (Aperturas)");
                                                kpi.setValue(0.0);
                                                kpi.setType("cantidad");
                                                kpi.setCreatedUser("-");
                                                kpi.setCreatedDate(LocalDateTime.now());
                                                kpi.setUpdatedDate(LocalDateTime.now());
                                                kpi.setStatus("A");
                                                kpi.setFormat(FORMAT_MP);
                                                kpi.setBatchId(batchId);
                                                kpi.setTypeMedia(MEDIO_PROPIO);
                                                kpi.setProviderId(providerId);
                                                return kpi;
                                            });
                                }))
                                .flatMap(this::saveKpi)
                                .onErrorResume(e -> {
                                    log.error("Error al procesar KPI de impresiones MP para proveedor {}: {}",
                                            providerId, e.getMessage(), e);
                                    return Flux.empty();
                                });

                        kpiFluxes.add(providerKpiFlux);
                    }

                    return kpiFluxes.isEmpty() ? Flux.empty() : Flux.merge(kpiFluxes);
                })
                .onErrorResume(e -> {
                    log.error("Error al buscar campañas para KPI de impresiones MP: {}", e.getMessage(), e);
                    return Flux.empty();
                });
    }

    /**
     * Versión optimizada para generar KPI de impresiones (aperturas) para Push App (PA)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiImpressionsPushApp(String batchId) {
        log.info("Generando KPI de impresiones (aperturas) para Push App (PA) - Versión optimizada");

        // Usar fechas para filtrar últimos 12 meses
        java.util.Date startDate = new java.util.Date(new java.util.Date().getTime() - 31536000000L);

        // Primero obtenemos las campañas activas para reducir el scope de búsqueda
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").is("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_PA))),
                        Campaign.class
                )
                .collectList()
                .flatMapMany(campaigns -> {
                    if (campaigns.isEmpty()) {
                        log.info("No se encontraron campañas activas para Push App (PA)");
                        return Flux.empty();
                    }

                    // Extraer los campaignIds para usarlos en un $in
                    List<String> campaignIds = campaigns.stream()
                            .map(Campaign::getCampaignId)
                            .filter(id -> id != null && !id.isEmpty())
                            .collect(Collectors.toList());

                    log.info("Procesando {} campañas activas para PA-I", campaignIds.size());

                    // Construimos un mapa de campaignId a detalles de campaña para uso posterior
                    Map<String, Campaign> campaignMap = campaigns.stream()
                            .collect(Collectors.toMap(
                                    Campaign::getCampaignId,
                                    campaign -> campaign,
                                    (c1, c2) -> c1 // En caso de duplicados quedarse con el primero
                            ));

                    // Realizar agregación con un match inicial para limitar los registros procesados
                    Aggregation aggregation = Aggregation.newAggregation(
                            // Match inicial con todos los filtros estáticos y el IN de campaignIds
                            Aggregation.match(new Criteria().andOperator(
//                                    Criteria.where("FechaProceso").gte(startDate),
//                                    Criteria.where("DateTimeSend").gte(startDate),
//                                    Criteria.where("Status").is("Success"),
//                                    Criteria.where("OpenDate").exists(true).ne(null),
                                    Criteria.where("campaignId").in(campaignIds)
                            )),

                            // Agrupar directamente por campaignId sin necesidad de lookup
                            Aggregation.group("campaignId")
                                    .count().as("value"),

                            // Proyectar los campos finales
                            Aggregation.project()
                                    .andExpression("_id").as("campaignId")
                                    .andExpression("value").as("value")
                    );

                    return reactiveMongoTemplate.aggregate(
                                    aggregation,
                                    "bq_ds_campanias_salesforce_push",
                                    Document.class)
                            .map(result -> {
                                String campaignId = result.getString("campaignId");
                                Integer value = result.getInteger("value");
                                Campaign campaign = campaignMap.get(campaignId);

                                if (campaign == null) {
                                    log.warn("No se encontró información de campaña para campaignId: {}", campaignId);
                                    return null;
                                }

                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaignId);
                                kpi.setCampaignSubId(campaignId);
                                kpi.setKpiId("PA-I");
                                kpi.setKpiDescription("Impresiones (Aperturas)");
                                kpi.setValue(value != null ? value.doubleValue() : 0.0);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_PA);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                return kpi;
                            })
                            .filter(Objects::nonNull)
                            .flatMap(this::saveKpi);
                });
    }

    /**
     * Versión optimizada para generar KPI de impresiones (aperturas) para Push Web (PW)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiImpressionsPushWeb(String batchId) {
        log.info("Generando KPI de impresiones (aperturas) para Push Web (PW) - Versión optimizada");

        // Usar fechas para filtrar últimos 12 meses
        java.util.Date startDate = new java.util.Date(new java.util.Date().getTime() - 31536000000L);

        // Primero obtenemos las campañas activas para reducir el scope de búsqueda
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").is("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_PW))),
                        Campaign.class
                )
                .collectList()
                .flatMapMany(campaigns -> {
                    if (campaigns.isEmpty()) {
                        log.info("No se encontraron campañas activas para Push Web (PW)");
                        return Flux.empty();
                    }

                    // Extraer los campaignIds para usarlos en un $in
                    List<String> campaignIds = campaigns.stream()
                            .map(Campaign::getCampaignId)
                            .filter(id -> id != null && !id.isEmpty())
                            .collect(Collectors.toList());

                    log.info("Procesando {} campañas activas para PW-I", campaignIds.size());

                    // Construimos un mapa de campaignId a detalles de campaña para uso posterior
                    Map<String, Campaign> campaignMap = campaigns.stream()
                            .collect(Collectors.toMap(
                                    Campaign::getCampaignId,
                                    campaign -> campaign,
                                    (c1, c2) -> c1 // En caso de duplicados quedarse con el primero
                            ));

                    // Realizar agregación con un match inicial para limitar los registros procesados
                    Aggregation aggregation = Aggregation.newAggregation(
                            // Match inicial con todos los filtros estáticos y el IN de campaignIds
                            Aggregation.match(new Criteria().andOperator(
//                                    Criteria.where("FechaProceso").gte(startDate),
//                                    Criteria.where("DateTimeSend").gte(startDate),
//                                    Criteria.where("Status").is("Success"),
//                                    Criteria.where("OpenDate").exists(true).ne(null),
                                    Criteria.where("campaignId").in(campaignIds)
                            )),

                            // Agrupar directamente por campaignId sin necesidad de lookup
                            Aggregation.group("campaignId")
                                    .count().as("value"),

                            // Proyectar los campos finales
                            Aggregation.project()
                                    .andExpression("_id").as("campaignId")
                                    .andExpression("value").as("value")
                    );

                    return reactiveMongoTemplate.aggregate(
                                    aggregation,
                                    "bq_ds_campanias_salesforce_push",
                                    Document.class)
                            .map(result -> {
                                String campaignId = result.getString("campaignId");
                                Integer value = result.getInteger("value");
                                Campaign campaign = campaignMap.get(campaignId);

                                if (campaign == null) {
                                    log.warn("No se encontró información de campaña para campaignId: {}", campaignId);
                                    return null;
                                }

                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaignId);
                                kpi.setCampaignSubId(campaignId);
                                kpi.setKpiId("PW-I");
                                kpi.setKpiDescription("Impresiones (Aperturas)");
                                kpi.setValue(value != null ? value.doubleValue() : 0.0);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_PW);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                return kpi;
                            })
                            .filter(Objects::nonNull)
                            .flatMap(this::saveKpi);
                });
    }

    /**
     * Versión Ultra-optimizada para generar KPI de alcance (envíos) para Mailing Padre (MP)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiScopeMailingParent(String batchId) {
        log.info("Generando KPI de alcance (envíos) para Mailing Padre (MP) siguiendo el query exacto de BigQuery");

        // Convertir batchId a String si no lo es y hacerlo final para su uso en lambdas
        final String finalBatchId = String.valueOf(batchId);

        // Filtrar por campañas activas y del último año
        LocalDateTime oneYearAgo = LocalDateTime.now().minusYears(1);

        try {
            log.info("Consultando campañas activas con formato MP y actualizadas después de {}", oneYearAgo);
            Query query = new Query();
            query.addCriteria(Criteria.where("status").in("En proceso"));
            query.addCriteria(Criteria.where("media").elemMatch(Criteria.where("format").is(String.valueOf(FORMAT_MP))));
            query.addCriteria(Criteria.where("updatedDate").gte(oneYearAgo));
            query.limit(100);

            return reactiveMongoTemplate.find(query, Campaign.class)
                    .collectList()
                    .flatMapMany(campaigns -> {
                        if (campaigns.isEmpty()) {
                            log.info("No se encontraron campañas activas para Mailing Padre (MP)");
                            return Flux.empty();
                        }

                        // Agrupar campañas por providerId
                        Map<String, List<Campaign>> campaignsByProvider = campaigns.stream()
                                .filter(c -> c.getProviderId() != null && !c.getProviderId().isEmpty())
                                .collect(Collectors.groupingBy(Campaign::getProviderId));

                        log.info("Se encontraron {} proveedores diferentes con campañas activas", campaignsByProvider.size());

                        // Procesar cada provider
                        return Flux.fromIterable(campaignsByProvider.entrySet())
                                .flatMap(entry -> {
                                    String providerId = entry.getKey();
                                    List<Campaign> providerCampaigns = entry.getValue();

                                    log.info("Procesando providerId: {} con {} campañas activas", providerId, providerCampaigns.size());

                                    // Implementar el query SQL proporcionado usando aggregation
                                    // Primero obtenemos los campaignIds para este provider
                                    List<String> campaignIds = providerCampaigns.stream()
                                            .map(Campaign::getCampaignId)
                                            .filter(id -> id != null && !id.isEmpty())
                                            .collect(Collectors.toList());

                                    if (campaignIds.isEmpty()) {
                                        log.info("No hay campaignIds válidos para providerId {}", providerId);
                                        return Flux.empty();
                                    }

                                    // Realizamos un aggregation que simula el JOIN de SQL
                                    return reactiveMongoTemplate.aggregate(
                                                    Aggregation.newAggregation(
                                                            // Iniciar desde sendjobs (equivalente a la tabla b en el SQL)
                                                            Aggregation.match(Criteria.where("campaignId").in(campaignIds)),
                                                            // Proyectar solo los campos que necesitamos
                                                            Aggregation.project("SendID", "campaignId"),
                                                            // Hacer un $lookup para unir con sents (equivalente a la tabla a en el SQL)
                                                            Aggregation.lookup("bq_ds_campanias_salesforce_sents", "SendID", "SendID", "sents"),
                                                            // Desenrollar los resultados para obtener todos los EmailAddress
                                                            Aggregation.unwind("sents"),
                                                            // Agrupar por EmailAddress para eliminar duplicados
                                                            Aggregation.group("sents.EmailAddress").count().as("count"),
                                                            // Contar el número total de EmailAddress únicos
                                                            Aggregation.group().count().as("distinctCount")
                                                    ),
                                                    "bq_ds_campanias_salesforce_sendjobs",
                                                    Document.class
                                            ).collectList()
                                            .map(list -> list.isEmpty() ? new Document("distinctCount", 0) : list.get(0))
                                            .flatMapMany(countResult -> {
                                                // Extraer valor del conteo y manejarlo de forma segura
                                                Object rawCount = countResult.get("distinctCount");
                                                final Double distinctCount = convertToDouble(rawCount);

                                                log.info("Resultado para providerId {}: {} EmailAddress únicos", providerId, distinctCount);

                                                // Crear KPIs para cada campaña del provider
                                                return Flux.fromIterable(providerCampaigns)
                                                        .flatMap(campaign -> {
                                                            String campaignId = campaign.getCampaignId();

                                                            log.info("Creando KPI MP-A para campaña {}, provider {}: valor={}",
                                                                    campaignId, providerId, distinctCount);

                                                            Kpi kpi = new Kpi();
                                                            kpi.setCampaignId(String.valueOf(campaignId));
                                                            kpi.setCampaignSubId(String.valueOf(campaignId));
                                                            kpi.setKpiId("MP-A");
                                                            kpi.setKpiDescription("Alcance (Envíos) - COUNT DISTINCT EmailAddress");
                                                            kpi.setValue(distinctCount);
                                                            kpi.setType("cantidad");
                                                            kpi.setCreatedUser("-");
                                                            kpi.setCreatedDate(LocalDateTime.now());
                                                            kpi.setUpdatedDate(LocalDateTime.now());
                                                            kpi.setStatus("A");
                                                            kpi.setFormat(String.valueOf(FORMAT_MP));
                                                            kpi.setBatchId(finalBatchId);
                                                            kpi.setTypeMedia(String.valueOf(MEDIO_PROPIO));
                                                            kpi.setProviderId(campaign.getProviderId());
                                                            return saveKpi(kpi);
                                                        });
                                            });
                                });
                    });
        } catch (Exception e) {
            log.error("Error en generateKpiScopeMailingParent: ", e);
            return Flux.error(e); // Es mejor propagar el error para manejo superior
        }
    }


    /**
     * Versión Ultra-optimizada para generar KPI de alcance (envíos) para Push App (PA)
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    /**
     * Versión Ultra-optimizada para generar KPI de alcance (envíos) para Push App (PA)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiScopePushApp(String batchId) {
        log.info("Generando KPI de alcance (envíos) para Push App (PA) - Versión ultra-optimizada");

        // Validar y normalizar batchId
        final String finalBatchId = (batchId != null) ? batchId : generateBatchId();

        // Primero obtenemos las campañas activas para reducir el scope de búsqueda
        // Limitando a solo las campañas recientes
        LocalDateTime oneYearAgo = LocalDateTime.now().minusYears(1);

        try {
            Query query = new Query();
            query.addCriteria(Criteria.where("status").in("En proceso"));
            query.addCriteria(Criteria.where("media").elemMatch(Criteria.where("format").is(FORMAT_PA)));
            query.addCriteria(Criteria.where("updatedDate").gte(oneYearAgo));
            query.limit(100); // Limitamos resultados para evitar sobrecarga

            return reactiveMongoTemplate.find(query, Campaign.class)
                    .collectList()
                    .flatMapMany(campaigns -> {
                        if (campaigns == null || campaigns.isEmpty()) {
                            log.info("No se encontraron campañas activas para Push App (PA)");
                            return Flux.empty();
                        }

                        // Extraer los campaignIds válidos para usarlos en un $in
                        List<String> campaignIds = campaigns.stream()
                                .filter(Objects::nonNull)
                                .map(Campaign::getCampaignId)
                                .filter(id -> id != null && !id.trim().isEmpty())
                                .collect(Collectors.toList());

                        if (campaignIds.isEmpty()) {
                            log.info("No se encontraron campaignIds válidos para PA-A");
                            return Flux.empty();
                        }

                        log.info("Procesando {} campañas activas para PA-A", campaignIds.size());

                        // Dividir en lotes si hay demasiados IDs para evitar límites de MongoDB
                        final int BATCH_SIZE = 50;
                        List<List<String>> campaignIdBatches = new ArrayList<>();

                        for (int i = 0; i < campaignIds.size(); i += BATCH_SIZE) {
                            campaignIdBatches.add(
                                    campaignIds.subList(i, Math.min(i + BATCH_SIZE, campaignIds.size()))
                            );
                        }

                        log.info("Dividiendo consulta en {} lotes para evitar límites de MongoDB", campaignIdBatches.size());

                        // Construimos un mapa de campaignId a detalles de campaña para uso posterior
                        Map<String, Campaign> campaignMap = campaigns.stream()
                                .filter(Objects::nonNull)
                                .filter(c -> c.getCampaignId() != null && !c.getCampaignId().trim().isEmpty())
                                .collect(Collectors.toMap(
                                        Campaign::getCampaignId,
                                        campaign -> campaign,
                                        (c1, c2) -> c1 // En caso de duplicados quedarse con el primero
                                ));

                        // Procesar cada lote de IDs
                        return Flux.fromIterable(campaignIdBatches)
                                .flatMap(batchCampaignIds -> {
                                    // Realizar agregación con un match inicial para limitar los registros procesados
                                    Aggregation aggregation = Aggregation.newAggregation(
                                            // Match inicial con el IN de campaignIds del lote actual
                                            Aggregation.match(Criteria.where("campaignId").in(batchCampaignIds)),
                                            // Agrupar directamente por campaignId sin necesidad de lookup
                                            Aggregation.group("campaignId")
                                                    .count().as("value"),
                                            // Proyectar los campos finales
                                            Aggregation.project()
                                                    .andExpression("_id").as("campaignId")
                                                    .andExpression("value").as("value")
                                    );

                                    return reactiveMongoTemplate.aggregate(
                                            aggregation,
                                            "bq_ds_campanias_salesforce_push",
                                            Document.class
                                    );
                                })
                                .map(result -> {
                                    // Extraer campaignId del resultado, asegurando que no sea nulo
                                    String campaignId = result.getString("campaignId");
                                    if (campaignId == null || campaignId.trim().isEmpty()) {
                                        log.warn("Se encontró un resultado con campaignId nulo o vacío");
                                        return null;
                                    }

                                    // Extraer y convertir el valor con manejo seguro de tipos
                                    Double value = convertToDouble(result.get("value"));

                                    // Obtener la campaña correspondiente
                                    Campaign campaign = campaignMap.get(campaignId);
                                    if (campaign == null) {
                                        log.warn("No se encontró información de campaña para campaignId: {}", campaignId);
                                        return null;
                                    }

                                    // Crear y configurar el KPI
                                    Kpi kpi = new Kpi();
                                    kpi.setCampaignId(campaignId);
                                    kpi.setCampaignSubId(campaignId);
                                    kpi.setKpiId("PA-A");
                                    kpi.setKpiDescription("Alcance (Envíos)");
                                    kpi.setValue(value);
                                    kpi.setType("cantidad");
                                    kpi.setCreatedUser("-");
                                    kpi.setCreatedDate(LocalDateTime.now());
                                    kpi.setUpdatedDate(LocalDateTime.now());
                                    kpi.setStatus("A");
                                    kpi.setFormat(FORMAT_PA);
                                    kpi.setBatchId(finalBatchId);
                                    kpi.setTypeMedia(MEDIO_PROPIO);
                                    kpi.setProviderId(campaign.getProviderId());
                                    return kpi;
                                })
                                .filter(Objects::nonNull)
                                .flatMap(this::saveKpi);
                    });
        } catch (Exception e) {
            log.error("Error en generateKpiScopePushApp: {}", e.getMessage(), e);
            return Flux.empty();
        }
    }

    /**
     * Versión Ultra-optimizada para generar KPI de alcance (envíos) para Push Web (PW)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiScopePushWeb(String batchId) {
        log.info("Generando KPI de alcance (envíos) para Push Web (PW) - Versión ultra-optimizada");

        // Validar y normalizar batchId
        final String finalBatchId = (batchId != null) ? batchId : generateBatchId();

        // Primero obtenemos las campañas activas para reducir el scope de búsqueda
        // Limitando a solo las campañas recientes
        LocalDateTime oneYearAgo = LocalDateTime.now().minusYears(1);

        try {
            Query query = new Query();
            query.addCriteria(Criteria.where("status").in("En proceso"));
            query.addCriteria(Criteria.where("media").elemMatch(Criteria.where("format").is(FORMAT_PW)));
            query.addCriteria(Criteria.where("updatedDate").gte(oneYearAgo));
            query.limit(100); // Limitamos resultados para evitar sobrecarga

            return reactiveMongoTemplate.find(query, Campaign.class)
                    .collectList()
                    .flatMapMany(campaigns -> {
                        if (campaigns == null || campaigns.isEmpty()) {
                            log.info("No se encontraron campañas activas para Push Web (PW)");
                            return Flux.empty();
                        }

                        // Extraer los campaignIds válidos para usarlos en un $in
                        List<String> campaignIds = campaigns.stream()
                                .filter(Objects::nonNull)
                                .map(Campaign::getCampaignId)
                                .filter(id -> id != null && !id.trim().isEmpty())
                                .collect(Collectors.toList());

                        if (campaignIds.isEmpty()) {
                            log.info("No se encontraron campaignIds válidos para PW-A");
                            return Flux.empty();
                        }

                        log.info("Procesando {} campañas activas para PW-A", campaignIds.size());

                        // Dividir en lotes si hay demasiados IDs para evitar límites de MongoDB
                        final int BATCH_SIZE = 50;
                        List<List<String>> campaignIdBatches = new ArrayList<>();

                        for (int i = 0; i < campaignIds.size(); i += BATCH_SIZE) {
                            campaignIdBatches.add(
                                    campaignIds.subList(i, Math.min(i + BATCH_SIZE, campaignIds.size()))
                            );
                        }

                        log.info("Dividiendo consulta en {} lotes para evitar límites de MongoDB", campaignIdBatches.size());

                        // Construimos un mapa de campaignId a detalles de campaña para uso posterior
                        Map<String, Campaign> campaignMap = campaigns.stream()
                                .filter(Objects::nonNull)
                                .filter(c -> c.getCampaignId() != null && !c.getCampaignId().trim().isEmpty())
                                .collect(Collectors.toMap(
                                        Campaign::getCampaignId,
                                        campaign -> campaign,
                                        (c1, c2) -> c1 // En caso de duplicados quedarse con el primero
                                ));

                        // Almacenamos los resultados por campaignId para acumularlos
                        Map<String, Double> aggregatedResults = new ConcurrentHashMap<>();

                        // Procesar cada lote de IDs
                        return Flux.fromIterable(campaignIdBatches)
                                .flatMap(batchCampaignIds -> {
                                    // Realizar agregación con un match inicial para limitar los registros procesados
                                    Aggregation aggregation = Aggregation.newAggregation(
                                            // Match inicial con el IN de campaignIds del lote actual
                                            Aggregation.match(Criteria.where("campaignId").in(batchCampaignIds)),
                                            // Agrupar directamente por campaignId sin necesidad de lookup
                                            Aggregation.group("campaignId")
                                                    .count().as("value"),
                                            // Proyectar los campos finales
                                            Aggregation.project()
                                                    .andExpression("_id").as("campaignId")
                                                    .andExpression("value").as("value")
                                    );

                                    return reactiveMongoTemplate.aggregate(
                                                    aggregation,
                                                    "bq_ds_campanias_salesforce_push",
                                                    Document.class
                                            )
                                            // Acumular los resultados por campaignId
                                            .doOnNext(result -> {
                                                // Extraer campaignId del resultado, asegurando que no sea nulo
                                                String campaignId = result.getString("campaignId");
                                                if (campaignId == null || campaignId.trim().isEmpty()) {
                                                    log.warn("Se encontró un resultado con campaignId nulo o vacío");
                                                    return;
                                                }

                                                // Extraer y convertir el valor con manejo seguro de tipos
                                                Double value = convertToDouble(result.get("value"));

                                                // Actualizar el mapa de resultados de forma atómica
                                                aggregatedResults.compute(campaignId, (k, v) -> (v == null ? 0.0 : v) + value);
                                            })
                                            // Completar el lote actual antes de procesar el siguiente
                                            .then(Mono.empty());
                                })
                                // Después de procesar todos los lotes, generar los KPIs
                                .thenMany(Flux.defer(() -> {
                                    // Convertir el mapa de resultados a KPIs
                                    return Flux.fromIterable(aggregatedResults.entrySet())
                                            .map(entry -> {
                                                String campaignId = entry.getKey();
                                                if (campaignId == null || campaignId.trim().isEmpty()) {
                                                    return null;
                                                }

                                                Double value = entry.getValue();
                                                if (value == null) {
                                                    value = 0.0;
                                                }

                                                Campaign campaign = campaignMap.get(campaignId);
                                                if (campaign == null) {
                                                    log.warn("No se encontró información de campaña para campaignId: {}", campaignId);
                                                    return null;
                                                }

                                                // Crear y configurar el KPI
                                                Kpi kpi = new Kpi();
                                                kpi.setCampaignId(campaignId);
                                                kpi.setCampaignSubId(campaignId);
                                                kpi.setKpiId("PW-A");
                                                kpi.setKpiDescription("Alcance (Envíos)");
                                                kpi.setValue(value);
                                                kpi.setType("cantidad");
                                                kpi.setCreatedUser("-");
                                                kpi.setCreatedDate(LocalDateTime.now());
                                                kpi.setUpdatedDate(LocalDateTime.now());
                                                kpi.setStatus("A");
                                                kpi.setFormat(FORMAT_PW);
                                                kpi.setBatchId(finalBatchId);
                                                kpi.setTypeMedia(MEDIO_PROPIO);

                                                return kpi;
                                            })
                                            .filter(Objects::nonNull)
                                            .flatMap(this::saveKpi);
                                }));
                    });
        } catch (Exception e) {
            log.error("Error en generateKpiScopePushWeb: {}", e.getMessage(), e);
            return Flux.empty();
        }
    }

    /**
     * Método de utilidad para convertir diferentes tipos de valores a Double de forma segura
     *
     * @param value Objeto a convertir a Double
     * @return Double convertido o 0.0 si hay error
     */
    private Double convertToDouble(Object value) {
        if (value == null) {
            return 0.0;
        }

        try {
            if (value instanceof Integer) {
                return ((Integer) value).doubleValue();
            } else if (value instanceof Long) {
                return ((Long) value).doubleValue();
            } else if (value instanceof Double) {
                return (Double) value;
            } else if (value instanceof Number) {
                return ((Number) value).doubleValue();
            } else if (value instanceof String) {
                String strValue = ((String) value).trim();
                if (strValue.isEmpty()) {
                    return 0.0;
                }
                // Eliminar caracteres extraños y reemplazar comas por puntos para mayor robustez
                strValue = strValue.replaceAll("[^\\d.,\\-]", "").replace(",", ".");
                return Double.parseDouble(strValue);
            } else {
                // Último recurso: convertir a String y luego a Double
                String strValue = value.toString().trim();
                if (strValue.isEmpty()) {
                    return 0.0;
                }
                // Eliminar caracteres extraños y reemplazar comas por puntos
                strValue = strValue.replaceAll("[^\\d.,\\-]", "").replace(",", ".");
                return Double.parseDouble(strValue);
            }
        } catch (NumberFormatException | NullPointerException e) {
            log.warn("No se pudo convertir el valor '{}' a Double: {}", value, e.getMessage());
            return 0.0;
        }
    }

    /**
     * Guarda un KPI en la base de datos
     *
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
                .set("typeMedia", kpi.getTypeMedia())
                .set("providerId", kpi.getProviderId());

        return reactiveMongoTemplate.upsert(query, update, Kpi.class)
                .thenReturn(kpi);
    }

    /**
     * Implementación del método para generar KPIs de Ventas para todos los formatos
     *
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    @Override
    public Flux<Kpi> generateKpiSales() {
        String batchId = generateBatchId();
        log.info("Generando KPIs de Ventas para todos los formatos. Batch ID: {}", batchId);

        // Ejecutamos todos los métodos de generación y concatenamos sus resultados
        return Flux.concat(
                generateKpiSalesMailingParent(batchId),
                generateKpiSalesMailingCabecera(batchId),
                generateKpiSalesMailingFeed(batchId),
                generateKpiSalesMailingBody(batchId),
                generateKpiSalesPushWeb(batchId),
                generateKpiSalesPushApp(batchId)
        );
    }

    /**
     * Genera KPIs de Ventas para el formato Mailing Padre (MP)
     * MP-V = MC-V + MF-V + MB-V
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiSalesMailingParent(String batchId) {
        log.info("Generando KPI de Ventas para Mailing Padre (MP)");

        // Primero obtenemos las campañas activas para el formato MP
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").in("En Proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_MP))),
                        Campaign.class
                )
                .flatMap(campaign -> {
                    log.info("Procesando campaña para MP-V: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Para MP-V necesitamos la suma de ventas de MC, MF y MB
                    // Primero obtenemos las ventas para cada formato
                    Mono<Double> salesMC = calculateSalesByProviderId(campaign.getProviderId(), FORMAT_MC);
                    Mono<Double> salesMF = calculateSalesByProviderId(campaign.getProviderId(), FORMAT_MF);
                    Mono<Double> salesMB = calculateSalesByProviderId(campaign.getProviderId(), FORMAT_MB);

                    // Combinamos los tres valores y los sumamos
                    return Mono.zip(salesMC, salesMF, salesMB)
                            .flatMap(tuple -> {
                                Double mcSales = tuple.getT1();
                                Double mfSales = tuple.getT2();
                                Double mbSales = tuple.getT3();

                                log.info("Componentes de ventas para MP-V: MC={}, MF={}, MB={}",
                                        mcSales, mfSales, mbSales);

                                // Calcular la suma total
                                double totalSales = mcSales + mfSales + mbSales;
                                log.info("Total de ventas calculado para MP-V: {}", totalSales);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MP-V");
                                kpi.setKpiDescription("Ventas Mailing Padre");
                                kpi.setValue(totalSales);
                                kpi.setType("monto");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MP);
                                kpi.setBatchId(batchId);
                                kpi.setProviderId(campaign.getProviderId());
                                kpi.setTypeMedia(MEDIO_PROPIO);


                                log.info("Guardando KPI MP-V para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Ventas para el formato Mailing Cabecera (MC)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiSalesMailingCabecera(String batchId) {
        log.info("Generando KPI de Ventas para Mailing Cabecera (MC)");

        // Obtener las campañas activas para el formato MC
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").in("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_MC))),
                        Campaign.class
                )
                .flatMap(campaign -> {
                    log.info("Procesando campaña para MC-V: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las ventas para esta campaña
                    return calculateSalesByProviderId(campaign.getProviderId(), FORMAT_MC)
                            .flatMap(sales -> {
                                log.info("Ventas calculadas para MC-V: {}", sales);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MCV");
                                kpi.setKpiDescription("Ventas Mailing Cabecera");
                                kpi.setValue(sales);
                                kpi.setType("monto");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MC);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());

                                log.info("Guardando KPI MC-V para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Ventas para el formato Mailing Feed (MF)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiSalesMailingFeed(String batchId) {
        log.info("Generando KPI de Ventas para Mailing Feed (MF)");

        // Obtener las campañas activas para el formato MF
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").in("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_MF))),
                        Campaign.class
                )
                .flatMap(campaign -> {
                    log.info("Procesando campaña para MF-V: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las ventas para esta campaña
                    return calculateSalesByProviderId(campaign.getProviderId(), FORMAT_MF)
                            .flatMap(sales -> {
                                log.info("Ventas calculadas para MF-V: {}", sales);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MFV");
                                kpi.setKpiDescription("Ventas Mailing Feed");
                                kpi.setValue(sales);
                                kpi.setType("monto");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MF);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());

                                log.info("Guardando KPI MF-V para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Ventas para el formato Mailing Body (MB)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiSalesMailingBody(String batchId) {
        log.info("Generando KPI de Ventas para Mailing Body (MB)");

        // Obtener las campañas activas para el formato MB
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").in("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_MB))),
                        Campaign.class
                )
                .flatMap(campaign -> {
                    log.info("Procesando campaña para MB-V: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las ventas para esta campaña
                    return calculateSalesByProviderId(campaign.getProviderId(), FORMAT_MB)
                            .flatMap(sales -> {
                                log.info("Ventas calculadas para MB-V: {}", sales);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MBV");
                                kpi.setKpiDescription("Ventas Mailing Body");
                                kpi.setValue(sales);
                                kpi.setType("monto");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MB);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());

                                log.info("Guardando KPI MB-V para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Ventas para el formato Push Web (PW)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiSalesPushWeb(String batchId) {
        log.info("Generando KPI de Ventas para Push Web (PW)");

        // Obtener las campañas activas para el formato PW
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").in("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_PW))),
                        Campaign.class
                )
                .flatMap(campaign -> {
                    log.info("Procesando campaña para PW-V: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las ventas para esta campaña
                    return calculateSalesByProviderId(campaign.getProviderId(), FORMAT_PW)
                            .flatMap(sales -> {
                                log.info("Ventas calculadas para PW-V: {}", sales);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("PW-V");
                                kpi.setKpiDescription("Ventas Push Web");
                                kpi.setValue(sales);
                                kpi.setType("monto");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_PW);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());

                                log.info("Guardando KPI PW-V para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Ventas para el formato Push App (PA)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiSalesPushApp(String batchId) {
        log.info("Generando KPI de Ventas para Push App (PA)");

        // Obtener las campañas activas para el formato PA
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").in("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_PA))),
                        Campaign.class
                )
                .flatMap(campaign -> {
                    log.info("Procesando campaña para PA-V: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las ventas para esta campaña
                    return calculateSalesByProviderId(campaign.getProviderId(), FORMAT_PA)
                            .flatMap(sales -> {
                                log.info("Ventas calculadas para PA-V: {}", sales);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("PA-V");
                                kpi.setKpiDescription("Ventas Push App");
                                kpi.setValue(sales);
                                kpi.setType("monto");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_PA);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());

                                log.info("Guardando KPI PA-V para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Calcula el total de ventas para un proveedor específico y un formato
     * Implementa exactamente el query:
     * SELECT sum(total_revenue) FROM ga4_own_media a INNER JOIN Campaigns c ON c.campaignId = a.campaignId WHERE c.ProveedorId = "ID"
     *
     * @param providerId ID del proveedor
     * @param format     Formato para el que se calculan las ventas
     * @return Mono<Double> Total de ventas
     */
    private Mono<Double> calculateSalesByProviderId(String providerId, String format) {
        log.info("Calculando ventas para proveedor: {} y formato: {}", providerId, format);

        // Primero obtenemos todos los campaignIds asociados con este providerId para el formato específico
        Query campaignsQuery = Query.query(
                Criteria.where("providerId").is(providerId)
                        .and("media").elemMatch(
                                Criteria.where("format").is(format)
                        )
        );

        log.info("Ejecutando consulta para obtener campañas con providerId: {} y formato: {}", providerId, format);

        return reactiveMongoTemplate.find(campaignsQuery, Campaign.class)
                .map(Campaign::getCampaignId)
                .filter(id -> id != null && !id.isEmpty())
                .collectList()
                .flatMap(campaignIds -> {
                    if (campaignIds.isEmpty()) {
                        log.warn("No se encontraron campañas para el proveedor: {} y formato: {}", providerId, format);
                        return Mono.just(0.0);
                    }

                    log.info("Campañas encontradas para proveedor {} y formato {}: {}", providerId, format, campaignIds.size());
                    log.debug("Lista de campaignIds: {}", campaignIds);

                    // Construir la agregación para calcular las ventas totales
                    // Esto implementa el JOIN y WHERE del SQL original
                    Aggregation aggregation = Aggregation.newAggregation(
                            // Filtrar por campaignIds (implementa el JOIN con WHERE)
                            Aggregation.match(Criteria.where("campaignId").in(campaignIds)),
                            // Calcular suma de total_revenue
                            Aggregation.group().sum("total_revenue").as("totalSales")
                    );

                    log.info("Ejecutando agregación para calcular ventas en ga4_own_media");

                    return reactiveMongoTemplate.aggregate(
                                    aggregation,
                                    "ga4_own_media",
                                    Document.class
                            )
                            .next()
                            .map(result -> {
                                // Extraer el total de ventas del resultado
                                Object totalSalesObj = result.get("totalSales");
                                log.info("Resultado de la agregación para proveedor {} y formato {}: totalSalesObj = {}, tipo = {}",
                                        providerId, format, totalSalesObj,
                                        totalSalesObj != null ? totalSalesObj.getClass().getName() : "null");

                                if (totalSalesObj == null) {
                                    log.warn("totalSalesObj es null para proveedor: {} y formato: {}", providerId, format);
                                    return 0.0;
                                }

                                // Convertir el resultado a Double de manera más robusta
                                try {
                                    if (totalSalesObj instanceof Double) {
                                        return (Double) totalSalesObj;
                                    } else if (totalSalesObj instanceof Integer) {
                                        return ((Integer) totalSalesObj).doubleValue();
                                    } else if (totalSalesObj instanceof Long) {
                                        return ((Long) totalSalesObj).doubleValue();
                                    } else if (totalSalesObj instanceof Number) {
                                        // Manejar cualquier tipo numérico
                                        return ((Number) totalSalesObj).doubleValue();
                                    } else if (totalSalesObj instanceof String) {
                                        // Intentar convertir String a Double
                                        return Double.parseDouble((String) totalSalesObj);
                                    } else {
                                        // Último recurso: convertir a String y luego a Double
                                        String strValue = totalSalesObj.toString();
                                        log.info("Convirtiendo valor de tipo desconocido a String: {}", strValue);
                                        return Double.parseDouble(strValue);
                                    }
                                } catch (Exception e) {
                                    log.error("Error al convertir el valor totalSalesObj a Double: {}", e.getMessage());
                                    log.error("Valor que causó el error: {} de tipo {}",
                                            totalSalesObj, totalSalesObj.getClass().getName());
                                    return 0.0;
                                }
                            })
                            .onErrorResume(e -> {
                                log.error("Error en la agregación para proveedor {} y formato {}: {}", providerId, format, e.getMessage());
                                return Mono.just(0.0);
                            })
                            .defaultIfEmpty(0.0)
                            .doOnSuccess(value -> {
                                log.info("Ventas totales calculadas para proveedor {} y formato {}: {}", providerId, format, value);
                            });
                });
    }


    /**
     * Implementación del método para generar KPIs de Transacciones para todos los formatos
     *
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    @Override
    public Flux<Kpi> generateKpiTransactions() {
        String batchId = generateBatchId();
        log.info("Generando KPIs de Transacciones para todos los formatos. Batch ID: {}", batchId);

        // Ejecutamos todos los métodos de generación y concatenamos sus resultados
        return Flux.concat(
                generateKpiTransactionsMailingParent(batchId),
                generateKpiTransactionsMailingCabecera(batchId),
                generateKpiTransactionsMailingFeed(batchId),
                generateKpiTransactionsMailingBody(batchId),
                generateKpiTransactionsPushWeb(batchId),
                generateKpiTransactionsPushApp(batchId)
        );
    }

    /**
     * Genera KPIs de Transacciones para el formato Mailing Padre (MP)
     * MP-T = MCT + MFT + MBT
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiTransactionsMailingParent(String batchId) {
        log.info("Generando KPI de Transacciones para Mailing Padre (MP)");

        // Primero obtenemos las campañas activas para el formato MP
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").in("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_MP))),
                        Campaign.class
                )
                .flatMap(campaign -> {
                    log.info("Procesando campaña para MP-T: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Para MP-T necesitamos la suma de transacciones de MC, MF y MB
                    // Primero obtenemos las transacciones para cada formato
                    Mono<Double> transactionsMC = calculateTransactionsByProviderId(campaign.getProviderId(), FORMAT_MC);
                    Mono<Double> transactionsMF = calculateTransactionsByProviderId(campaign.getProviderId(), FORMAT_MF);
                    Mono<Double> transactionsMB = calculateTransactionsByProviderId(campaign.getProviderId(), FORMAT_MB);

                    // Combinamos los tres valores y los sumamos
                    return Mono.zip(transactionsMC, transactionsMF, transactionsMB)
                            .flatMap(tuple -> {
                                Double mcTransactions = tuple.getT1();
                                Double mfTransactions = tuple.getT2();
                                Double mbTransactions = tuple.getT3();

                                log.info("Componentes de transacciones para MP-T: MC={}, MF={}, MB={}",
                                        mcTransactions, mfTransactions, mbTransactions);

                                // Calcular la suma total
                                double totalTransactions = mcTransactions + mfTransactions + mbTransactions;
                                log.info("Total de transacciones calculado para MP-T: {}", totalTransactions);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MP-T");
                                kpi.setKpiDescription("Transacciones Mailing Padre");
                                kpi.setValue(totalTransactions);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MP);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());

                                log.info("Guardando KPI MP-T para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Transacciones para el formato Mailing Cabecera (MC)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiTransactionsMailingCabecera(String batchId) {
        log.info("Generando KPI de Transacciones para Mailing Cabecera (MC)");

        // Obtener las campañas activas para el formato MC
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").in("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_MC))),
                        Campaign.class
                )
                .flatMap(campaign -> {
                    log.info("Procesando campaña para MC-T: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las transacciones para esta campaña
                    return calculateTransactionsByProviderId(campaign.getProviderId(), FORMAT_MC)
                            .flatMap(transactions -> {
                                log.info("Transacciones calculadas para MC-T: {}", transactions);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MCT");
                                kpi.setKpiDescription("Transacciones Mailing Cabecera");
                                kpi.setValue(transactions);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MC);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());

                                log.info("Guardando KPI MC-T para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Transacciones para el formato Mailing Feed (MF)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiTransactionsMailingFeed(String batchId) {
        log.info("Generando KPI de Transacciones para Mailing Feed (MF)");

        // Obtener las campañas activas para el formato MF
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").in("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_MF))),
                        Campaign.class
                )
                .flatMap(campaign -> {
                    log.info("Procesando campaña para MF-T: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las transacciones para esta campaña
                    return calculateTransactionsByProviderId(campaign.getProviderId(), FORMAT_MF)
                            .flatMap(transactions -> {
                                log.info("Transacciones calculadas para MF-T: {}", transactions);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MFT");
                                kpi.setKpiDescription("Transacciones Mailing Feed");
                                kpi.setValue(transactions);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MF);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());

                                log.info("Guardando KPI MF-T para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Transacciones para el formato Mailing Body (MB)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiTransactionsMailingBody(String batchId) {
        log.info("Generando KPI de Transacciones para Mailing Body (MB)");

        // Obtener las campañas activas para el formato MB
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").in("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_MB))),
                        Campaign.class
                )
                .flatMap(campaign -> {
                    log.info("Procesando campaña para MB-T: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las transacciones para esta campaña
                    return calculateTransactionsByProviderId(campaign.getProviderId(), FORMAT_MB)
                            .flatMap(transactions -> {
                                log.info("Transacciones calculadas para MB-T: {}", transactions);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MBT");
                                kpi.setKpiDescription("Transacciones Mailing Body");
                                kpi.setValue(transactions);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MB);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());

                                log.info("Guardando KPI MB-T para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Transacciones para el formato Push Web (PW)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiTransactionsPushWeb(String batchId) {
        log.info("Generando KPI de Transacciones para Push Web (PW)");

        // Obtener las campañas activas para el formato PW
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").in("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_PW))),
                        Campaign.class
                )
                .flatMap(campaign -> {
                    log.info("Procesando campaña para PW-T: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las transacciones para esta campaña
                    return calculateTransactionsByProviderId(campaign.getProviderId(), FORMAT_PW)
                            .flatMap(transactions -> {
                                log.info("Transacciones calculadas para PW-T: {}", transactions);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("PW-T");
                                kpi.setKpiDescription("Transacciones Push Web");
                                kpi.setValue(transactions);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_PW);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());

                                log.info("Guardando KPI PW-T para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Transacciones para el formato Push App (PA)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiTransactionsPushApp(String batchId) {
        log.info("Generando KPI de Transacciones para Push App (PA)");

        // Obtener las campañas activas para el formato PA
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").in("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_PA))),
                        Campaign.class
                )
                .flatMap(campaign -> {
                    log.info("Procesando campaña para PA-T: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las transacciones para esta campaña
                    return calculateTransactionsByProviderId(campaign.getProviderId(), FORMAT_PA)
                            .flatMap(transactions -> {
                                log.info("Transacciones calculadas para PA-T: {}", transactions);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("PA-T");
                                kpi.setKpiDescription("Transacciones Push App");
                                kpi.setValue(transactions);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_PA);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                log.info("Guardando KPI PA-T para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Calcula el total de transacciones para un proveedor específico y un formato
     * Implementa exactamente el query:
     * SELECT sum(Transaccions) FROM ga4_own_media a INNER JOIN Campaign c ON c.campaignId = a.campaignId WHERE c.ProveedorId = "ID"
     *
     * @param providerId ID del proveedor
     * @param format     Formato para el que se calculan las transacciones
     * @return Mono<Double> Total de transacciones
     */
    private Mono<Double> calculateTransactionsByProviderId(String providerId, String format) {
        log.info("Calculando transacciones para proveedor: {} y formato: {}", providerId, format);

        // Primero obtenemos todos los campaignIds asociados con este providerId para el formato específico
        Query campaignsQuery = Query.query(
                Criteria.where("providerId").is(providerId)
                        .and("media").elemMatch(
                                Criteria.where("format").is(format)
                        )
        );

        log.info("Ejecutando consulta para obtener campañas con providerId: {} y formato: {}", providerId, format);

        return reactiveMongoTemplate.find(campaignsQuery, Campaign.class)
                .map(Campaign::getCampaignId)
                .filter(id -> id != null && !id.isEmpty())
                .collectList()
                .flatMap(campaignIds -> {
                    if (campaignIds.isEmpty()) {
                        log.warn("No se encontraron campañas para el proveedor: {} y formato: {}", providerId, format);
                        return Mono.just(0.0);
                    }

                    log.info("Campañas encontradas para proveedor {} y formato {}: {}", providerId, format, campaignIds.size());
                    log.debug("Lista de campaignIds: {}", campaignIds);

                    // Construir la agregación para calcular las transacciones totales
                    // Esto implementa el JOIN y WHERE del SQL original
                    Aggregation aggregation = Aggregation.newAggregation(
                            // Filtrar por campaignIds (implementa el JOIN con WHERE)
                            Aggregation.match(Criteria.where("campaignId").in(campaignIds)),
                            // Calcular suma de transactions
                            Aggregation.group().sum("transactions").as("totalTransactions")
                    );

                    log.info("Ejecutando agregación para calcular transacciones en ga4_own_media");

                    return reactiveMongoTemplate.aggregate(
                                    aggregation,
                                    "ga4_own_media",
                                    Document.class
                            )
                            .next()
                            .map(result -> {
                                // Extraer el total de transacciones del resultado
                                Object totalTransactionsObj = result.get("totalTransactions");
                                log.info("Resultado de la agregación para proveedor {} y formato {}: totalTransactionsObj = {}, tipo = {}",
                                        providerId, format, totalTransactionsObj,
                                        totalTransactionsObj != null ? totalTransactionsObj.getClass().getName() : "null");

                                if (totalTransactionsObj == null) {
                                    log.warn("totalTransactionsObj es null para proveedor: {} y formato: {}", providerId, format);
                                    return 0.0;
                                }

                                // Convertir el resultado a Double de manera más robusta
                                try {
                                    if (totalTransactionsObj instanceof Double) {
                                        return (Double) totalTransactionsObj;
                                    } else if (totalTransactionsObj instanceof Integer) {
                                        return ((Integer) totalTransactionsObj).doubleValue();
                                    } else if (totalTransactionsObj instanceof Long) {
                                        return ((Long) totalTransactionsObj).doubleValue();
                                    } else if (totalTransactionsObj instanceof Number) {
                                        // Manejar cualquier tipo numérico
                                        return ((Number) totalTransactionsObj).doubleValue();
                                    } else if (totalTransactionsObj instanceof String) {
                                        // Intentar convertir String a Double
                                        return Double.parseDouble((String) totalTransactionsObj);
                                    } else {
                                        // Último recurso: convertir a String y luego a Double
                                        String strValue = totalTransactionsObj.toString();
                                        log.info("Convirtiendo valor de tipo desconocido a String: {}", strValue);
                                        return Double.parseDouble(strValue);
                                    }
                                } catch (Exception e) {
                                    log.error("Error al convertir el valor totalTransactionsObj a Double: {}", e.getMessage());
                                    log.error("Valor que causó el error: {} de tipo {}",
                                            totalTransactionsObj, totalTransactionsObj.getClass().getName());
                                    return 0.0;
                                }
                            })
                            .onErrorResume(e -> {
                                log.error("Error en la agregación para proveedor {} y formato {}: {}", providerId, format, e.getMessage());
                                return Mono.just(0.0);
                            })
                            .defaultIfEmpty(0.0)
                            .doOnSuccess(value -> {
                                log.info("Transacciones totales calculadas para proveedor {} y formato {}: {}", providerId, format, value);
                            });
                });
    }


    /**
     * Implementación del método para generar KPIs de clicks para todos los formatos
     */
    @Override
    public Flux<Kpi> generateKpiClicks() {
        String batchId = generateBatchId();
        log.info("Generando KPIs de clicks para todos los formatos. Batch ID: {}", batchId);

        return Flux.concat(
                generateKpiClicksParents(batchId),     // MP-C (lo que ya tienes implementado)
                generateKpiClicksByFormat(batchId)     // MCC, MFC, MBC (lo que ya tienes implementado)
        );
    }


    /**
     * Procesa los clicks del mail padre
     */
    public Flux<Kpi> generateKpiClicksParents(String batchId) {
        log.info("Iniciando generación de KPIs de clicks para mail padre");

        // Primero obtenemos las campañas activas con formato MP
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("status").is("En proceso")
                                .and("media").elemMatch(Criteria.where("format").is(FORMAT_MP))),
                        Campaign.class
                )
                .doOnNext(campaign ->
                        log.info("Procesando campaña: {}, providerId: {}",
                                campaign.getCampaignId(), campaign.getProviderId())
                )
                .flatMap(campaign -> {
                    String providerId = campaign.getProviderId();
                    if (providerId == null || providerId.trim().isEmpty()) {
                        log.warn("ProviderId no encontrado para campaña: {}", campaign.getCampaignId());
                        return Mono.empty();
                    }

                    // Agregación para contar clicks
                    Aggregation aggregation = Aggregation.newAggregation(
                            // Group por SendID para contar clicks
                            Aggregation.group("SendID")
                                    .count().as("clickCount"),

                            // Proyección inicial
                            Aggregation.project()
                                    .and("_id").as("SendID")
                                    .and("clickCount").as("clickCount"),

                            // Join con sendjobs
                            Aggregation.lookup()
                                    .from("bq_ds_campanias_salesforce_sendjobs")
                                    .localField("SendID")
                                    .foreignField("SendID")
                                    .as("sendjobs"),

                            // Desenrollar resultados del join
                            Aggregation.unwind("sendjobs", true),

                            // Proyección con campaignId
                            Aggregation.project()
                                    .and("SendID").as("SendID")
                                    .and("clickCount").as("clickCount")
                                    .and("sendjobs.campaignId").as("campaignId")
                                    .and("sendjobs.Corporacion").as("Corporacion"), // Agregando Corporación

                            // Filtrar registros válidos
                            Aggregation.match(
                                    Criteria.where("campaignId").exists(true)
                                            .and("Corporacion").in("MIFARMA", "INKAFARMA")
                            ),

                            // Join con campaigns para validación adicional
                            Aggregation.lookup()
                                    .from("campaigns")
                                    .localField("campaignId")
                                    .foreignField("campaignId")
                                    .as("campaign"),

                            Aggregation.unwind("campaign", true)
                    );

                    return reactiveMongoTemplate.aggregate(
                                    aggregation,
                                    "bq_ds_campanias_salesforce_clicks",
                                    Document.class
                            )
                            .filter(doc -> doc.get("clickCount") != null && doc.get("campaign") != null)
                            .map(doc -> {
                                try {
                                    Kpi kpi = new Kpi();
                                    kpi.setCampaignId(doc.getString("campaignId"));
                                    kpi.setCampaignSubId(doc.getString("campaignId"));
                                    kpi.setKpiId("MP-C");
                                    kpi.setKpiDescription("Clics medios propios");
                                    kpi.setValue(Double.valueOf(doc.getInteger("clickCount")));
                                    kpi.setType("cantidad");
                                    kpi.setCreatedUser("-");
                                    kpi.setCreatedDate(LocalDateTime.now());
                                    kpi.setUpdatedDate(LocalDateTime.now());
                                    kpi.setStatus("A");
                                    kpi.setFormat(FORMAT_MP);
                                    kpi.setBatchId(batchId);
                                    kpi.setTypeMedia(MEDIO_PROPIO);
                                    kpi.setProviderId(providerId);

                                    log.info("KPI generado para campaña: {}, clicks: {}",
                                            kpi.getCampaignId(), kpi.getValue());

                                    return kpi;
                                } catch (Exception e) {
                                    log.error("Error al crear KPI para campaña {}: {}",
                                            doc.getString("campaignId"), e.getMessage());
                                    return null;
                                }
                            })
                            .filter(Objects::nonNull)
                            .flatMap(this::saveKpi)
                            .doOnError(error ->
                                    log.error("Error procesando clicks para campaña {}: {}",
                                            campaign.getCampaignId(), error.getMessage())
                            );
                });
    }

    public Flux<Kpi> generateKpiClicksByFormat(String batchId) {
        log.info("Iniciando generación de KPIs de clicks por formato");

        Aggregation aggregation = Aggregation.newAggregation(
                // Agrupar por SendID y campaignSubId (importante para diferenciar formatos)
                Aggregation.group("SendID", "campaignSubId")
                        .count().as("clickCount"),

                // Proyección inicial
                Aggregation.project()
                        .and("_id.SendID").as("SendID")
                        .and("_id.campaignSubId").as("campaignSubId")
                        .and("clickCount").as("clickCount"),

                // Join con sendjobs para obtener campaignId y Corporación
                Aggregation.lookup()
                        .from("bq_ds_campanias_salesforce_sendjobs")
                        .localField("SendID")
                        .foreignField("SendID")
                        .as("sendjobs"),

                // Desenrollar sendjobs
                Aggregation.unwind("sendjobs", true),

                // Proyección con datos de sendjobs
                Aggregation.project()
                        .and("campaignSubId").as("campaignSubId")
                        .and("clickCount").as("clickCount")
                        .and("sendjobs.campaignId").as("campaignId")
                        .and("sendjobs.Corporacion").as("Corporacion"),

                // Filtrar por Corporación válida
                Aggregation.match(
                        Criteria.where("Corporacion").in("MIFARMA", "INKAFARMA")
                ),

                // Join con campaigns
                Aggregation.lookup()
                        .from("campaigns")
                        .localField("campaignId")
                        .foreignField("campaignId")
                        .as("campaign"),

                // Desenrollar campaign
                Aggregation.unwind("campaign", true)
        );

        return reactiveMongoTemplate.aggregate(
                        aggregation,
                        "bq_ds_campanias_salesforce_clicks",
                        Document.class
                )
                .doOnNext(doc -> log.debug("Documento recibido: campaignSubId={}, clickCount={}",
                        doc.getString("campaignSubId"), doc.get("clickCount")))
                // Filtramos documentos válidos
                .filter(doc -> {
                    boolean isValid = doc.get("clickCount") != null &&
                            doc.get("campaignId") != null &&
                            doc.getString("campaignSubId") != null;
                    if (!isValid) {
                        log.debug("Documento filtrado por datos incompletos");
                    }
                    return isValid;
                })
                .flatMap(doc -> {
                    try {
                        String campaignId = doc.getString("campaignId");
                        String format = getFormatFromSubId(doc.getString("campaignSubId"));

                        if (format == null) {
                            log.debug("Formato no válido para campaignSubId: {}", doc.getString("campaignSubId"));
                            return Mono.empty();
                        }

                        // Construir el campaignSubId correcto
                        String correctCampaignSubId = campaignId + format;
                        // Obtener providerId de la campaña
                        return reactiveMongoTemplate.findOne(
                                        Query.query(Criteria.where("campaignId").is(campaignId)),
                                        Campaign.class
                                )
                                .flatMap(campaign -> {
                                    String providerId = campaign.getProviderId();
                                    if (providerId == null || providerId.trim().isEmpty()) {
                                        log.warn("ProviderId no encontrado para campaña: {}", campaignId);
                                        return Mono.empty();
                                    }

                                    Kpi kpi = new Kpi();
                                    kpi.setCampaignId(campaignId);
                                    kpi.setCampaignSubId(correctCampaignSubId);
                                    kpi.setKpiId(format + "C"); // MC-C, MF-C, MB-C
                                    kpi.setKpiDescription("Clics " + getFormatDescription(format));
                                    kpi.setValue(Double.valueOf(doc.getInteger("clickCount")));
                                    kpi.setType("cantidad");
                                    kpi.setCreatedUser("-");
                                    kpi.setCreatedDate(LocalDateTime.now());
                                    kpi.setUpdatedDate(LocalDateTime.now());
                                    kpi.setStatus("A");
                                    kpi.setFormat(format);
                                    kpi.setBatchId(batchId);
                                    kpi.setTypeMedia(MEDIO_PROPIO);
                                    kpi.setProviderId(providerId);

                                    log.info("KPI generado para formato {}, campaña {}, clicks: {}",
                                            format, campaignId, kpi.getValue());

                                    return saveKpi(kpi);
                                });
                    } catch (Exception e) {
                        log.error("Error al procesar documento: {}", e.getMessage());
                        return Mono.empty();
                    }
                })
                .onErrorResume(error -> {
                    log.error("Error en generateKpiClicksByFormat: {}", error.getMessage());
                    return Flux.empty();
                });
    }

    private String getFormatFromSubId(String campaignSubId) {
        if (campaignSubId == null) return null;

        if (campaignSubId.endsWith("MC")) {
            return "MC"; // Mailing Cabecera
        } else if (campaignSubId.endsWith("MF")) {
            return "MF"; // Mailing Feed
        } else if (campaignSubId.endsWith("MB")) {
            return "MB"; // Mailing Body
        }
        return null;
    }

    private String getFormatDescription(String format) {
        return switch (format) {
            case "MC" -> "Mailing Cabecera";
            case "MF" -> "Mailing Feed";
            case "MB" -> "Mailing Body";
            default -> "";
        };
    }


    /**
     * Implementación del método para generar KPIs de rates para todos los formatos
     */
    @Override
    public Flux<Kpi> generateKpiRates() {
        String batchId = generateBatchId();
        log.info("Generando KPIs de rates para todos los formatos. Batch ID: {}", batchId);

        return Flux.concat(
                generateKpiRatesMailingParent(batchId),             // MP-OR, MP-CR (lo que ya tienes implementado)
                generateKpiClickRatesByFormat(batchId),// MCCR, MFCR, MBCR (lo que ya tienes implementado)
                generateKpiPushAppOpenRate(batchId)    // PA-OR (lo que ya tienes implementado)
        );
    }


    public Flux<Kpi> generateKpiRatesMailingParent(String batchId) {
        log.info("Iniciando generación de KPIs de rates para mail padre");

        // Primero obtenemos todos los datos necesarios para los cálculos
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("kpiId").in("MP-I", "MP-A", "MP-C")
                                .and("status").is("A")),
                        Kpi.class,
                        "kpi_v2"
                )
                .collectMultimap(Kpi::getCampaignId)
                .flatMapMany(kpisByCampaign -> {
                    // Obtener los providerId de las campañas
                    Set<String> campaignIds = kpisByCampaign.keySet();

                    return reactiveMongoTemplate.find(
                                    Query.query(Criteria.where("campaignId").in(campaignIds)
                                            .and("status").is("En proceso")),
                                    Campaign.class
                            )
                            .collectMap(Campaign::getCampaignId)
                            .flatMapMany(campaignMap -> {
                                List<Mono<Kpi>> rateKpis = new ArrayList<>();

                                kpisByCampaign.forEach((campaignId, kpis) -> {
                                    Campaign campaign = campaignMap.get(campaignId);
                                    if (campaign == null) {
                                        log.warn("No se encontró campaña activa para campaignId: {}", campaignId);
                                        return;
                                    }

                                    String providerId = campaign.getProviderId();
                                    if (providerId == null || providerId.trim().isEmpty()) {
                                        log.warn("ProviderId no encontrado para campaña: {}", campaignId);
                                        return;
                                    }

                                    Map<String, Double> values = kpis.stream()
                                            .collect(Collectors.toMap(
                                                    Kpi::getKpiId,
                                                    Kpi::getValue,
                                                    (v1, v2) -> v1 // En caso de duplicados, mantener el primero
                                            ));

                                    // Calcular Open Rate (OR) = Impresiones / Alcance
                                    if (values.containsKey("MP-I") && values.containsKey("MP-A") && values.get("MP-A") > 0) {
                                        double openRate = values.get("MP-I") / values.get("MP-A");
                                        rateKpis.add(createRateKpi(
                                                campaignId,
                                                "MP-OR",
                                                "Open Rate (OR)",
                                                openRate,
                                                batchId,
                                                providerId
                                        ));

                                        log.info("OR calculado para campaña {}: {}", campaignId, openRate);
                                    } else {
                                        log.warn("Datos insuficientes para calcular OR de campaña: {}", campaignId);
                                    }

                                    // Calcular CTR (CR) = Clics / Impresiones
                                    if (values.containsKey("MP-C") && values.containsKey("MP-I") && values.get("MP-I") > 0) {
                                        double ctr = values.get("MP-C") / values.get("MP-I");
                                        rateKpis.add(createRateKpi(
                                                campaignId,
                                                "MP-CR",
                                                "CTR (CR)",
                                                ctr,
                                                batchId,
                                                providerId
                                        ));

                                        log.info("CTR calculado para campaña {}: {}", campaignId, ctr);
                                    } else {
                                        log.warn("Datos insuficientes para calcular CTR de campaña: {}", campaignId);
                                    }
                                });

                                return Flux.concat(rateKpis);
                            });
                })
                .onErrorResume(error -> {
                    log.error("Error en generateKpiRatesMailingParent: {}", error.getMessage());
                    return Flux.empty();
                });
    }

    private Mono<Kpi> createRateKpi(
            String campaignId,
            String kpiId,
            String description,
            double value,
            String batchId,
            String providerId
    ) {
        Kpi kpi = new Kpi();
        kpi.setCampaignId(campaignId);
        kpi.setCampaignSubId(campaignId);
        kpi.setKpiId(kpiId);
        kpi.setKpiDescription(description);
        kpi.setValue(value);
        kpi.setType("porcentaje");
        kpi.setCreatedUser("-");
        kpi.setCreatedDate(LocalDateTime.now());
        kpi.setUpdatedDate(LocalDateTime.now());
        kpi.setStatus("A");
        kpi.setFormat(FORMAT_MP);
        kpi.setBatchId(batchId);
        kpi.setTypeMedia(MEDIO_PROPIO);
        kpi.setProviderId(providerId);

        return saveKpi(kpi);
    }

    public Flux<Kpi> generateKpiClickRatesByFormat(String batchId) {
        log.info("Iniciando generación de Click Rates por formato");

        // Primero obtenemos los clicks por formato y las impresiones
        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("kpiId").in("MCC", "MFC", "MBC", "MP-I")
                                .and("status").is("A")),
                        Kpi.class,
                        "kpi_v2"
                )
                .collectMultimap(Kpi::getCampaignId)
                .flatMapMany(kpisByCampaign -> {
                    // Obtener los providerId de las campañas
                    Set<String> campaignIds = kpisByCampaign.keySet();

                    return reactiveMongoTemplate.find(
                                    Query.query(Criteria.where("campaignId").in(campaignIds)
                                            .and("status").is("En proceso")),
                                    Campaign.class
                            )
                            .collectMap(Campaign::getCampaignId)
                            .flatMapMany(campaignMap -> {
                                List<Mono<Kpi>> rateKpis = new ArrayList<>();

                                kpisByCampaign.forEach((campaignId, kpis) -> {
                                    Campaign campaign = campaignMap.get(campaignId);
                                    if (campaign == null) {
                                        log.warn("No se encontró campaña activa para campaignId: {}", campaignId);
                                        return;
                                    }

                                    String providerId = campaign.getProviderId();
                                    if (providerId == null || providerId.trim().isEmpty()) {
                                        log.warn("ProviderId no encontrado para campaña: {}", campaignId);
                                        return;
                                    }

                                    Double impressions = kpis.stream()
                                            .filter(k -> k.getKpiId().equals("MP-I"))
                                            .map(Kpi::getValue)
                                            .findFirst()
                                            .orElse(0.0);

                                    if (impressions > 0) {
                                        // Calcular Click Rate para cada formato
                                        kpis.stream()
                                                .filter(k -> k.getKpiId().matches("(MCC|MFC|MBC)"))
                                                .forEach(clickKpi -> {
                                                    String format = clickKpi.getFormat(); // MC, MF, o MB
                                                    double clickRate = clickKpi.getValue() / impressions;

                                                    Kpi rateKpi = new Kpi();
                                                    rateKpi.setCampaignId(campaignId);
                                                    rateKpi.setCampaignSubId(clickKpi.getCampaignSubId());
                                                    rateKpi.setKpiId(format + "CR");
                                                    rateKpi.setKpiDescription("Click Rate " + getFormatDescription(format));
                                                    rateKpi.setValue(clickRate);
                                                    rateKpi.setType("porcentaje");
                                                    rateKpi.setCreatedUser("-");
                                                    rateKpi.setCreatedDate(LocalDateTime.now());
                                                    rateKpi.setUpdatedDate(LocalDateTime.now());
                                                    rateKpi.setStatus("A");
                                                    rateKpi.setFormat(format);
                                                    rateKpi.setBatchId(batchId);
                                                    rateKpi.setTypeMedia(MEDIO_PROPIO);
                                                    rateKpi.setProviderId(providerId);

                                                    log.info("Click Rate calculado para formato {}, campaña {}: {}",
                                                            format, campaignId, clickRate);

                                                    rateKpis.add(saveKpi(rateKpi));
                                                });
                                    } else {
                                        log.warn("Impresiones en cero para campaña: {}", campaignId);
                                    }
                                });

                                return Flux.concat(rateKpis);
                            });
                })
                .doOnNext(kpi -> log.info("Click Rate generado: {}", kpi))
                .onErrorResume(error -> {
                    log.error("Error en generateKpiClickRatesByFormat: {}", error.getMessage());
                    return Flux.empty();
                });
    }


    public Flux<Kpi> generateKpiPushAppOpenRate(String batchId) {
        log.info("Iniciando generación de Open Rate para Push App");

        return reactiveMongoTemplate.find(
                        Query.query(Criteria.where("kpiId").in("PA-I", "PA-A")
                                .and("status").is("A")),
                        Kpi.class,
                        "kpi_v2"
                )
                .collectMultimap(Kpi::getCampaignId)
                .flatMapMany(kpisByCampaign -> {
                    // Obtener los providerId de las campañas activas
                    Set<String> campaignIds = kpisByCampaign.keySet();

                    return reactiveMongoTemplate.find(
                                    Query.query(Criteria.where("campaignId").in(campaignIds)
                                            .and("status").is("En proceso")
                                            .and("media").elemMatch(Criteria.where("format").is(FORMAT_PA))),
                                    Campaign.class
                            )
                            .collectMap(Campaign::getCampaignId)
                            .flatMapMany(campaignMap -> {
                                List<Mono<Kpi>> rateKpis = new ArrayList<>();

                                kpisByCampaign.forEach((campaignId, kpis) -> {
                                    Campaign campaign = campaignMap.get(campaignId);
                                    if (campaign == null) {
                                        log.warn("No se encontró campaña activa para campaignId: {}", campaignId);
                                        return;
                                    }

                                    String providerId = campaign.getProviderId();
                                    if (providerId == null || providerId.trim().isEmpty()) {
                                        log.warn("ProviderId no encontrado para campaña: {}", campaignId);
                                        return;
                                    }

                                    Double impressions = kpis.stream()
                                            .filter(k -> k.getKpiId().equals("PA-I"))
                                            .map(Kpi::getValue)
                                            .findFirst()
                                            .orElse(0.0);

                                    Double scope = kpis.stream()
                                            .filter(k -> k.getKpiId().equals("PA-A"))
                                            .map(Kpi::getValue)
                                            .findFirst()
                                            .orElse(0.0);

                                    if (scope > 0) {
                                        double openRate = impressions / scope;

                                        // Validar que el openRate tenga sentido
                                        if (openRate >= 0 && openRate <= 1) {
                                            Kpi rateKpi = new Kpi();
                                            rateKpi.setCampaignId(campaignId);
                                            rateKpi.setCampaignSubId(campaignId);
                                            rateKpi.setKpiId("PA-OR");
                                            rateKpi.setKpiDescription("Open Rate Push App (OR)");
                                            rateKpi.setValue(openRate);
                                            rateKpi.setType("porcentaje");
                                            rateKpi.setCreatedUser("-");
                                            rateKpi.setCreatedDate(LocalDateTime.now());
                                            rateKpi.setUpdatedDate(LocalDateTime.now());
                                            rateKpi.setStatus("A");
                                            rateKpi.setFormat(FORMAT_PA);
                                            rateKpi.setBatchId(batchId);
                                            rateKpi.setTypeMedia(MEDIO_PROPIO);
                                            rateKpi.setProviderId(providerId);

                                            log.info("Open Rate calculado para Push App, campaña {}: {}",
                                                    campaignId, openRate);

                                            rateKpis.add(saveKpi(rateKpi));
                                        } else {
                                            log.warn("Open Rate fuera de rango para campaña {}: {}",
                                                    campaignId, openRate);
                                        }
                                    } else {
                                        log.warn("Scope en cero para campaña: {}", campaignId);
                                    }
                                });

                                if (rateKpis.isEmpty()) {
                                    log.info("No se generaron KPIs de Open Rate para Push App");
                                }

                                return Flux.concat(rateKpis);
                            });
                })
                .doOnNext(kpi -> log.debug("KPI Push App Open Rate guardado: {}", kpi))
                .doOnComplete(() -> log.info("Proceso de generación de Open Rate Push App completado"))
                .onErrorResume(error -> {
                    log.error("Error en generateKpiPushAppOpenRate: {}", error.getMessage());
                    return Flux.empty();
                });
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
     * Implementación optimizada del método para generar KPIs de correos entregados para Mailing Padre (MP)
     *
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    public Flux<Kpi> generateKpiDeliveredMailParent() {
        final String batchId = generateBatchId();
        log.info("Iniciando generación de KPIs de correos entregados para Mailing Padre (MP). Batch ID: {}", batchId);

        try {
            // Paso 1: Obtener todas las campañas activas con formato MP
            log.info("Paso 1: Consultando campañas activas con formato MP");
            return reactiveMongoTemplate.find(
                            Query.query(Criteria.where("status").in("En proceso")
                                    .and("media").elemMatch(Criteria.where("format").is(FORMAT_MP))),
                            Campaign.class
                    )
                    .collectList()
                    .doOnNext(campaigns -> log.info("Se encontraron {} campañas activas", campaigns.size()))
                    .flatMapMany(campaigns -> {
                        // Verificar si hay campañas
                        if (campaigns == null || campaigns.isEmpty()) {
                            log.info("No se encontraron campañas activas para Mailing Padre (MP)");
                            return Flux.empty();
                        }

                        // Paso 2: Agrupar campañas por providerId
                        log.info("Paso 2: Agrupando campañas por providerId");
                        Map<String, List<Campaign>> campaignsByProvider = campaigns.stream()
                                .filter(campaign -> {
                                    if (campaign == null) {
                                        log.warn("Se encontró una campaña nula");
                                        return false;
                                    }

                                    String providerId = campaign.getProviderId();
                                    if (providerId == null || providerId.trim().isEmpty()) {
                                        log.warn("Campaña {} ignorada: providerId nulo o vacío",
                                                campaign.getCampaignId() != null ? campaign.getCampaignId() : "ID desconocido");
                                        return false;
                                    }
                                    return true;
                                })
                                .collect(Collectors.groupingBy(Campaign::getProviderId));

                        log.info("Se encontraron {} proveedores diferentes", campaignsByProvider.size());

                        // Paso 3: Procesar cada providerId por separado
                        return Flux.fromIterable(campaignsByProvider.entrySet())
                                .flatMap(entry -> {
                                    final String providerId = entry.getKey();
                                    final List<Campaign> providerCampaigns = entry.getValue();

                                    log.info("Paso 3: Procesando providerId: '{}' con {} campañas", providerId, providerCampaigns.size());

                                    // Paso 3.1: Extraer campaignIds válidos
                                    List<String> campaignIds = providerCampaigns.stream()
                                            .map(Campaign::getCampaignId)
                                            .filter(id -> {
                                                boolean isValid = id != null && !id.trim().isEmpty();
                                                if (!isValid) {
                                                    log.warn("Se encontró un campaignId nulo o vacío para providerId: {}", providerId);
                                                }
                                                return isValid;
                                            })
                                            .collect(Collectors.toList());

                                    log.info("Encontrados {} campaignIds válidos para providerId: '{}'", campaignIds.size(), providerId);

                                    if (campaignIds.isEmpty()) {
                                        log.warn("No hay campaignIds válidos para providerId: '{}'", providerId);
                                        return Flux.empty();
                                    }

                                    // Paso 4: Ejecutar la agregación para contar emailAddress únicos
                                    log.info("Paso 4: Ejecutando agregación para providerId: '{}' con campaignIds: {}", providerId, campaignIds);

                                    try {
                                        // Esta agregación sigue exactamente la estructura del query SQL:
                                        // SELECT DISTINCT COUNT(a.EmailAddress)
                                        // FROM bq_ds_campanias_salesforce_opens a
                                        // INNER JOIN bq_ds_campanias_salesforce_sendjobs b ON a.SendID = b.SendID
                                        // INNER JOIN Campaigns c ON c.campaingId = b.campaingId
                                        // WHERE c.ProveedorId = "ID" AND EventType = "Sent"
                                        return reactiveMongoTemplate.aggregate(
                                                        Aggregation.newAggregation(
                                                                // Parte del WHERE c.campaignId IN (ids de campañas del proveedor)
                                                                Aggregation.match(Criteria.where("campaignId").in(campaignIds)),
                                                                // Proyectar solo los campos necesarios
                                                                Aggregation.project("SendID", "campaignId")
                                                                        .andExclude("_id"),
                                                                // INNER JOIN con la colección de opens
                                                                Aggregation.lookup(
                                                                        "bq_ds_campanias_salesforce_opens",  // from
                                                                        "SendID",                            // localField
                                                                        "SendID",                            // foreignField
                                                                        "opens"                              // as
                                                                ),
                                                                // Filtrar documentos que tienen al menos un resultado en el join
                                                                Aggregation.match(Criteria.where("opens").not().size(0)),
                                                                // Desenrollar el array de opens para procesarlos individualmente
                                                                Aggregation.unwind("opens"),
                                                                // Filtrar por EventType = "Sent"
                                                                Aggregation.match(Criteria.where("opens.EventType").is("Sent")),
                                                                // Proyectar EmailAddress para el paso siguiente
                                                                Aggregation.project("opens.EmailAddress"),
                                                                // Agrupar por EmailAddress para contar cada uno una sola vez (DISTINCT)
                                                                Aggregation.group("EmailAddress"),
                                                                // Contar el número total de grupos (COUNT DISTINCT)
                                                                Aggregation.group().count().as("distinctCount")
                                                        ),
                                                        "bq_ds_campanias_salesforce_sendjobs",
                                                        Document.class
                                                )
                                                .collectList()
                                                .doOnNext(results -> log.info("Agregación completada para providerId: '{}'. Resultados: {}",
                                                        providerId, results.size()))
                                                .flatMapMany(results -> {
                                                    // Extraer el conteo o usar 0 si no hay resultados
                                                    final Integer distinctCount;
                                                    if (!results.isEmpty() && results.get(0) != null) {
                                                        Document countDoc = results.get(0);
                                                        distinctCount = countDoc.getInteger("distinctCount", 0);
                                                    } else {
                                                        distinctCount = 0;
                                                    }

                                                    log.info("Conteo final para providerId '{}': {} emails únicos", providerId, distinctCount);

                                                    // Paso 5: Crear KPIs para cada campaña del proveedor
                                                    log.info("Paso 5: Creando KPIs para las {} campañas del providerId: '{}'",
                                                            providerCampaigns.size(), providerId);

                                                    return Flux.fromIterable(providerCampaigns)
                                                            .flatMap(campaign -> {
                                                                String campaignId = campaign.getCampaignId();
                                                                if (campaignId == null || campaignId.trim().isEmpty()) {
                                                                    log.warn("Saltando creación de KPI: campaignId nulo o vacío");
                                                                    return Mono.empty();
                                                                }

                                                                log.info("Creando KPI MP-E para campaña: '{}', providerId: '{}', valor: {}",
                                                                        campaignId, providerId, distinctCount);

                                                                try {
                                                                    Kpi kpi = new Kpi();
                                                                    kpi.setCampaignId(campaignId);
                                                                    kpi.setCampaignSubId(campaignId);
                                                                    kpi.setKpiId("MP-E");
                                                                    kpi.setKpiDescription("Correos Entregados Mailing Padre");
                                                                    // Almacenar como Double pero desde un Integer
                                                                    kpi.setValue(distinctCount.doubleValue());
                                                                    kpi.setType("cantidad");
                                                                    kpi.setCreatedUser("-");
                                                                    kpi.setCreatedDate(LocalDateTime.now());
                                                                    kpi.setUpdatedDate(LocalDateTime.now());
                                                                    kpi.setStatus("A");
                                                                    kpi.setProviderId(campaign.getProviderId());
                                                                    kpi.setKpiDescription("MEDIO PROPIO");
                                                                    kpi.setFormat("MP-E");
                                                                    kpi.setBatchId(batchId);


                                                                    // Guardar el KPI
                                                                    log.info("Guardando KPI MP-E para campaña: '{}'", campaignId);
                                                                    return saveKpi(kpi)
                                                                            .doOnSuccess(savedKpi ->
                                                                                    log.info("KPI guardado exitosamente para campaña: '{}'", campaignId))
                                                                            .doOnError(error ->
                                                                                    log.error("Error al guardar KPI para campaña '{}': {}",
                                                                                            campaignId, error.getMessage()));

                                                                } catch (Exception e) {
                                                                    log.error("Error al crear KPI para campaña '{}': {}",
                                                                            campaignId, e.getMessage());
                                                                    return Mono.empty();
                                                                }
                                                            });
                                                });
                                    } catch (Exception e) {
                                        log.error("Error durante la agregación para providerId '{}': {}",
                                                providerId, e.getMessage(), e);
                                        return Flux.empty();
                                    }
                                });
                    });
        } catch (Exception e) {
            log.error("Error general en generateKpiDeliveredMailParent: {}", e.getMessage(), e);
            return Flux.empty();
        }
    }

    /**
     * Calcula el total de correos entregados para un proveedor específico
     * Implementa exactamente el query:
     * SELECT DISTINCT COUNT(a.EmailAddress)
     * FROM bq_ds_campanias_salesforce_opens a
     * INNER JOIN bq_ds_campanias_salesforce_sendjobs b ON a.SendID = b.SendID
     * INNER JOIN Campaign c ON c.campaingId = sendjobs.campaingId
     * WHERE c.ProveedorId = "ID" AND EventType = "Sent"
     *
     * @param providerId ID del proveedor
     * @return Mono<Double> Total de correos entregados
     */
    private Mono<Double> calculateDeliveredEmailsByProviderId(String providerId) {
        log.info("Calculando correos entregados para proveedor: {}", providerId);

        // Primero obtenemos todos los campaignIds asociados con este providerId
        Query campaignsQuery = Query.query(Criteria.where("providerId").is(providerId));

        log.info("Ejecutando consulta para obtener campañas con providerId: {}", providerId);

        return reactiveMongoTemplate.find(campaignsQuery, Campaign.class)
                .map(Campaign::getCampaignId)
                .filter(id -> id != null && !id.isEmpty())
                .collectList()
                .flatMap(campaignIds -> {
                    if (campaignIds.isEmpty()) {
                        log.warn("No se encontraron campañas para el proveedor calculateDeliveredEmailsByProviderId: {}", providerId);
                        return Mono.just(0.0);
                    }

                    log.info("Campañas encontradas para proveedor  calculateDeliveredEmailsByProviderId {}: {}", providerId, campaignIds.size());
                    log.debug("Lista de campaignIds calculateDeliveredEmailsByProviderId: {}", campaignIds);

                    // Ahora obtenemos todos los SendIDs asociados con estos campaignIds
                    return reactiveMongoTemplate.find(
                                    Query.query(Criteria.where("campaignId").in(campaignIds)),
                                    Document.class,
                                    "bq_ds_campanias_salesforce_sendjobs"
                            )
                            .map(doc -> doc.getString("SendID"))
                            .filter(sendId -> sendId != null && !sendId.isEmpty())
                            .collectList()
                            .flatMap(sendIds -> {
                                if (sendIds.isEmpty()) {
                                    log.warn("No se encontraron SendIDs para las campañas del proveedor: {}", providerId);
                                    return Mono.just(0.0);
                                }

                                log.info("SendIDs encontrados: {}", sendIds.size());

                                // Ahora realizamos la agregación para contar los emails entregados
                                Aggregation aggregation = Aggregation.newAggregation(
                                        // Filtrar por SendIDs y EventType
                                        Aggregation.match(
                                                Criteria.where("SendID").in(sendIds)
                                                        .and("EventType").is("Sent")
                                        ),
                                        // Agrupar por EmailAddress para contar solo una vez por email
                                        Aggregation.group("EmailAddress"),
                                        // Contar el número de grupos (emails únicos)
                                        Aggregation.group().count().as("deliveredCount")
                                );

                                log.info("Ejecutando agregación para contar correos entregados en bq_ds_campanias_salesforce_opens");

                                return reactiveMongoTemplate.aggregate(
                                                aggregation,
                                                "bq_ds_campanias_salesforce_opens",
                                                Document.class
                                        )
                                        .next()
                                        .map(result -> {
                                            // Extraer el conteo de correos entregados
                                            Object deliveredCountObj = result.get("deliveredCount");
                                            log.info("Resultado de la agregación para proveedor {}: deliveredCountObj = {}, tipo = {}",
                                                    providerId, deliveredCountObj,
                                                    deliveredCountObj != null ? deliveredCountObj.getClass().getName() : "null");

                                            if (deliveredCountObj == null) {
                                                log.warn("deliveredCountObj es null para proveedor: {}", providerId);
                                                return 0.0;
                                            }

                                            // Convertir el resultado a Double de manera más robusta
                                            try {
                                                if (deliveredCountObj instanceof Double) {
                                                    return (Double) deliveredCountObj;
                                                } else if (deliveredCountObj instanceof Integer) {
                                                    return ((Integer) deliveredCountObj).doubleValue();
                                                } else if (deliveredCountObj instanceof Long) {
                                                    return ((Long) deliveredCountObj).doubleValue();
                                                } else if (deliveredCountObj instanceof Number) {
                                                    // Manejar cualquier tipo numérico
                                                    return ((Number) deliveredCountObj).doubleValue();
                                                } else if (deliveredCountObj instanceof String) {
                                                    // Intentar convertir String a Double
                                                    return Double.parseDouble((String) deliveredCountObj);
                                                } else {
                                                    // Último recurso: convertir a String y luego a Double
                                                    String strValue = deliveredCountObj.toString();
                                                    log.info("Convirtiendo valor de tipo desconocido a String: {}", strValue);
                                                    return Double.parseDouble(strValue);
                                                }
                                            } catch (Exception e) {
                                                log.error("Error al convertir el valor deliveredCountObj a Double: {}", e.getMessage());
                                                log.error("Valor que causó el error: {} de tipo {}",
                                                        deliveredCountObj, deliveredCountObj.getClass().getName());
                                                return 0.0;
                                            }
                                        })
                                        .onErrorResume(e -> {
                                            log.error("Error en la agregación para proveedor {}: {}", providerId, e.getMessage());
                                            return Mono.just(0.0);
                                        })
                                        .defaultIfEmpty(0.0)
                                        .doOnSuccess(value -> {
                                            log.info("Correos entregados totales calculados para proveedor {}: {}", providerId, value);
                                        });
                            });
                });
    }


    /**
     * Implementación del método para generar KPIs de ROAS (Return On Advertising Spend) para todos los formatos
     *
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    @Override
    public Flux<Object> generateKpiRoasGeneral() {
        String batchId = generateBatchId();
        log.info("Generando KPIs de ROAS para todos los formatos. Batch ID: {}", batchId);

        // Ejecutamos todos los métodos de generación y concatenamos sus resultados
        return Flux.concat(

                generateKpiRoasMailingCabecera(batchId),
                generateKpiRoasMailingFeed(batchId),
                generateKpiRoasMailingBody(batchId),
                generateKpiRoasMailingParent(batchId),
                generateKpiRoasPushWeb(batchId),
                generateKpiRoasPushApp(batchId)
        );
    }

    /**
     * Método auxiliar para obtener la inversión de una campaña específica para un formato determinado
     *
     * @param campaignId ID de la campaña
     * @param format     Formato para el que se requiere la inversión
     * @return Mono<Double> Valor de la inversión
     */
//    private Mono<Double> getInvestment(String campaignId, String format) {
//        log.info("Obteniendo inversión para campaña: {} y formato: {}", campaignId, format);
//
//        // Construir query para buscar en la colección campaigns
//        Query query = new Query(Criteria.where("campaignId").is(campaignId));
//
//        return reactiveMongoTemplate.findOne(query, Campaign.class)
//                .flatMap(campaign -> {
//                    if (campaign == null || campaign.getMedia() == null) {
//                        log.warn("No se encontró la campaña o no tiene media definido: {}", campaignId);
//                        return Mono.just(0.0);
//                    }
//
//                    // Buscar el formato específico en el array de media
//                    return Flux.fromIterable(campaign.getMedia())
//                            .filter(media -> format.equals(media.getFormat()))
//                            .next()
//                            .map(media -> {
//                                // Si encontramos el medio con el formato, retornamos la inversión
//                                if (campaign.getInvestment() != null) {
//                                    return campaign.getInvestment().doubleValue();
//                                }
//                                return 0.0;
//                            })
//                            .defaultIfEmpty(0.0);
//                })
//                .defaultIfEmpty(0.0);
//    }
    private Mono<Double> getInvestment(String campaignId, String format) {
        log.info("Obteniendo inversión para campaña: {} y formato: {}", campaignId, format);

        // Construir query para buscar todos los documentos con el campaignId dado
        Query query = new Query(Criteria.where("campaignId").is(campaignId));

        return reactiveMongoTemplate.find(query, Document.class, "campaigns")
                .flatMap(doc -> {
                    // Log para depuración
                    log.debug("Analizando documento con ID: {}", doc.getObjectId("_id").toString());

                    // Extraer la lista de media
                    List<Document> mediaList = (List<Document>) doc.get("media");
                    if (mediaList == null || mediaList.isEmpty()) {
                        return Mono.empty(); // Skip this document if no media
                    }

                    // Buscar el formato específico en este documento
                    for (Document media : mediaList) {
                        String mediaFormat = media.getString("format");
                        if (mediaFormat != null && mediaFormat.equals(format)) {
                            // Encontramos un documento con el formato correcto
                            log.info("Encontrado formato {} en documento {}",
                                    format, doc.getObjectId("_id").toString());

                            // Extraer inversión
                            Object investmentObj = doc.get("investment");
                            if (investmentObj == null) {
                                log.warn("Inversión no definida en documento con formato correcto");
                                return Mono.just(0.0);
                            }

                            try {
                                Double investment;
                                if (investmentObj instanceof Integer) {
                                    investment = ((Integer) investmentObj).doubleValue();
                                } else if (investmentObj instanceof Double) {
                                    investment = (Double) investmentObj;
                                } else if (investmentObj instanceof Long) {
                                    investment = ((Long) investmentObj).doubleValue();
                                } else {
                                    investment = Double.parseDouble(investmentObj.toString());
                                }

                                log.info("Inversión encontrada para campaña {} con formato {}: {}",
                                        campaignId, format, investment);
                                return Mono.just(investment);
                            } catch (Exception e) {
                                log.error("Error al convertir inversión: {}", e.getMessage());
                                return Mono.just(0.0);
                            }
                        }
                    }

                    // Este documento no tiene el formato buscado
                    return Mono.empty();
                })
                .next() // Tomar el primer documento que coincida con el formato
                .defaultIfEmpty(0.0)
                .onErrorResume(e -> {
                    log.error("Error al obtener inversión para campaña {} y formato {}: {}",
                            campaignId, format, e.getMessage());
                    return Mono.just(0.0);
                });
    }

    /**
     * Genera KPIs de ROAS para el formato Mailing Padre (MP)
     * MP-VRA = (MCVRA + MFVRA + MBVRA) / inversión
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiRoasMailingParent(String batchId) {
        log.info("Generando KPI de ROAS para Mailing Padre (MP)");

        // Obtener todas las campañas para el formato MP
        return getCampaignsByFormat(FORMAT_MP)
                .flatMap(campaign -> {
                    // Obtener ventas totales para esta campaña (combinando MC, MF y MB)
                    Mono<Double> salesMono = calculateTotalSalesByProviderId(campaign.getProviderId());

                    // Obtener inversión para esta campaña y formato
                    Mono<Double> investmentMono = getInvestment(campaign.getCampaignId(), FORMAT_MP);

                    // Combinar ventas e inversión para calcular ROAS
                    return Mono.zip(salesMono, investmentMono)
                            .flatMap(tuple -> {
                                Double sales = tuple.getT1();
                                Double investment = tuple.getT2();

                                if (investment == null || investment == 0.0) {
                                    log.warn("Inversión es cero o nula para campaña: {} y formato: {}",
                                            campaign.getCampaignId(), FORMAT_MP);
                                    return Mono.empty();
                                }

                                // Calcular ROAS
                                double roasValue = sales / investment;

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MP-VRA");
                                kpi.setKpiDescription("ROAS Mailing Padre");
                                kpi.setValue(roasValue);
                                kpi.setType("ratio");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MP);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de ROAS para el formato Mailing Cabecera (MC)
     * MCVRA = Ventas mailing cabecera / inversión
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiRoasMailingCabecera(String batchId) {
        log.info("Generando KPI de ROAS para Mailing Cabecera (MC)");

        // Obtener todas las campañas para el formato MC
        return getCampaignsByFormat(FORMAT_MC)
                .flatMap(campaign -> {
                    // Obtener ventas totales para esta campaña
                    Mono<Double> salesMono = calculateTotalSalesByProviderId(campaign.getProviderId());

                    // Obtener inversión para esta campaña y formato
                    Mono<Double> investmentMono = getInvestment(campaign.getCampaignId(), FORMAT_MC);

                    // Combinar ventas e inversión para calcular ROAS
                    return Mono.zip(salesMono, investmentMono)
                            .flatMap(tuple -> {
                                Double sales = tuple.getT1();
                                Double investment = tuple.getT2();

                                if (investment == null || investment == 0.0) {
                                    log.warn("Inversión es cero o nula para campaña: {} y formato: {}",
                                            campaign.getCampaignId(), FORMAT_MC);
                                    return Mono.empty();
                                }

                                // Calcular ROAS
                                double roasValue = sales / investment;

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MCVRA");
                                kpi.setKpiDescription("ROAS Mailing Cabecera");
                                kpi.setValue(roasValue);
                                kpi.setType("ratio");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MC);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de ROAS para el formato Mailing Feed (MF)
     * MFVRA = Ventas mailing feed / inversión
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiRoasMailingFeed(String batchId) {
        log.info("Generando KPI de ROAS para Mailing Feed (MF)");

        // Obtener todas las campañas para el formato MF
        return getCampaignsByFormat(FORMAT_MF)
                .flatMap(campaign -> {
                    // Obtener ventas totales para esta campaña
                    Mono<Double> salesMono = calculateTotalSalesByProviderId(campaign.getProviderId());

                    // Obtener inversión para esta campaña y formato
                    Mono<Double> investmentMono = getInvestment(campaign.getCampaignId(), FORMAT_MF);

                    // Combinar ventas e inversión para calcular ROAS
                    return Mono.zip(salesMono, investmentMono)
                            .flatMap(tuple -> {
                                Double sales = tuple.getT1();
                                Double investment = tuple.getT2();

                                if (investment == null || investment == 0.0) {
                                    log.warn("Inversión es cero o nula para campaña: {} y formato: {}",
                                            campaign.getCampaignId(), FORMAT_MF);
                                    return Mono.empty();
                                }

                                // Calcular ROAS
                                double roasValue = sales / investment;

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MFVRA");
                                kpi.setKpiDescription("ROAS Mailing Feed");
                                kpi.setValue(roasValue);
                                kpi.setType("ratio");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MF);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de ROAS para el formato Mailing Body (MB)
     * MBVRA = Ventas mailing body / inversión
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiRoasMailingBody(String batchId) {
        log.info("Generando KPI de ROAS para Mailing Body (MB)");

        // Obtener todas las campañas para el formato MB
        return getCampaignsByFormat(FORMAT_MB)
                .flatMap(campaign -> {
                    // Obtener ventas totales para esta campaña
                    Mono<Double> salesMono = calculateTotalSalesByProviderId(campaign.getProviderId());

                    // Obtener inversión para esta campaña y formato
                    Mono<Double> investmentMono = getInvestment(campaign.getCampaignId(), FORMAT_MB);

                    // Combinar ventas e inversión para calcular ROAS
                    return Mono.zip(salesMono, investmentMono)
                            .flatMap(tuple -> {
                                Double sales = tuple.getT1();
                                Double investment = tuple.getT2();

                                if (investment == null || investment == 0.0) {
                                    log.warn("Inversión es cero o nula para campaña: {} y formato: {}",
                                            campaign.getCampaignId(), FORMAT_MB);
                                    return Mono.empty();
                                }

                                // Calcular ROAS
                                double roasValue = sales / investment;

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MBVRA");
                                kpi.setKpiDescription("ROAS Mailing Body");
                                kpi.setValue(roasValue);
                                kpi.setType("ratio");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MB);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de ROAS para el formato Push Web (PW)
     * PW-VRA = Ventas Push Web / inversión
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiRoasPushWeb(String batchId) {
        log.info("Generando KPI de ROAS para Push Web (PW)");

        // Obtener todas las campañas para el formato PW
        return getCampaignsByFormat(FORMAT_PW)
                .flatMap(campaign -> {
                    // Obtener ventas totales para esta campaña
                    Mono<Double> salesMono = calculateTotalSalesByProviderId(campaign.getProviderId());

                    // Obtener inversión para esta campaña y formato
                    Mono<Double> investmentMono = getInvestment(campaign.getCampaignId(), FORMAT_PW);

                    // Combinar ventas e inversión para calcular ROAS
                    return Mono.zip(salesMono, investmentMono)
                            .flatMap(tuple -> {
                                Double sales = tuple.getT1();
                                Double investment = tuple.getT2();

                                if (investment == null || investment == 0.0) {
                                    log.warn("Inversión es cero o nula para campaña: {} y formato: {}",
                                            campaign.getCampaignId(), FORMAT_PW);
                                    return Mono.empty();
                                }

                                // Calcular ROAS
                                double roasValue = sales / investment;

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("PW-VRA");
                                kpi.setKpiDescription("ROAS Push Web");
                                kpi.setValue(roasValue);
                                kpi.setType("ratio");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_PW);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de ROAS para el formato Push App (PA)
     * PA-VRA = Ventas Push App / inversión
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiRoasPushApp(String batchId) {
        log.info("Generando KPI de ROAS para Push App (PA)");

        // Obtener todas las campañas para el formato PA
        return getCampaignsByFormat(FORMAT_PA)
                .flatMap(campaign -> {
                    // Obtener ventas totales para esta campaña
                    Mono<Double> salesMono = calculateTotalSalesByProviderId(campaign.getProviderId());

                    // Obtener inversión para esta campaña y formato
                    Mono<Double> investmentMono = getInvestment(campaign.getCampaignId(), FORMAT_PA);

                    // Combinar ventas e inversión para calcular ROAS
                    return Mono.zip(salesMono, investmentMono)
                            .flatMap(tuple -> {
                                Double sales = tuple.getT1();
                                Double investment = tuple.getT2();

                                if (investment == null || investment == 0.0) {
                                    log.warn("Inversión es cero o nula para campaña: {} y formato: {}",
                                            campaign.getCampaignId(), FORMAT_PA);
                                    return Mono.empty();
                                }

                                // Calcular ROAS
                                double roasValue = sales / investment;

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("PA-VRA");
                                kpi.setKpiDescription("ROAS Push App");
                                kpi.setValue(roasValue);
                                kpi.setType("ratio");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_PA);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Calcula el total de ventas para un proveedor específico
     * Implementa exactamente el query:
     * SELECT sum(total_revenue) FROM ga4_own_media a INNER JOIN campaings c ON c.CampaignId= a.CampaignId WHERE c.ProveedorId = "ID"
     *
     * @param providerId ID del proveedor
     * @return Mono<Double> Total de ventas
     */
    private Mono<Double> calculateTotalSalesByProviderId(String providerId) {
        log.info("Calculando ventas totales para proveedor: {}", providerId);

        // Primero obtenemos todos los campaignIds asociados con este providerId
        Query campaignsQuery = new Query(Criteria.where("providerId").is(providerId));

        return reactiveMongoTemplate.find(campaignsQuery, Campaign.class)
                .map(Campaign::getCampaignId)
                .filter(id -> id != null && !id.isEmpty())
                .collectList()
                .flatMap(campaignIds -> {
                    if (campaignIds.isEmpty()) {
                        log.warn("No se encontraron campañas para el proveedor: {}", providerId);
                        return Mono.just(0.0);
                    }

                    log.info("Campañas encontradas para proveedor {}: {}", providerId, campaignIds.size());

                    // Construir la agregación para calcular las ventas totales
                    // Esto implementa el JOIN y WHERE del SQL original
                    Aggregation aggregation = Aggregation.newAggregation(
                            // Filtrar por campaignIds (implementa el JOIN con WHERE)
                            Aggregation.match(Criteria.where("campaignId").in(campaignIds)),
                            // Calcular suma de total_revenue
                            Aggregation.group().sum("total_revenue").as("totalSales")
                    );

                    return reactiveMongoTemplate.aggregate(
                                    aggregation,
                                    "ga4_own_media",
                                    Document.class
                            )
                            .next()
                            .map(result -> {
                                // Extraer el total de ventas del resultado
                                Object totalSalesObj = result.get("totalSales");
                                log.info("Resultado de la agregación para proveedor {}: totalSalesObj = {}, tipo = {}",
                                        providerId, totalSalesObj,
                                        totalSalesObj != null ? totalSalesObj.getClass().getName() : "null");

                                if (totalSalesObj == null) {
                                    log.warn("totalSalesObj es null para proveedor: {}", providerId);
                                    return 0.0;
                                }

                                // Convertir el resultado a Double de manera más robusta
                                try {
                                    if (totalSalesObj instanceof Double) {
                                        return (Double) totalSalesObj;
                                    } else if (totalSalesObj instanceof Integer) {
                                        return ((Integer) totalSalesObj).doubleValue();
                                    } else if (totalSalesObj instanceof Long) {
                                        return ((Long) totalSalesObj).doubleValue();
                                    } else if (totalSalesObj instanceof Number) {
                                        // Manejar cualquier tipo numérico
                                        return ((Number) totalSalesObj).doubleValue();
                                    } else if (totalSalesObj instanceof String) {
                                        // Intentar convertir String a Double
                                        return Double.parseDouble((String) totalSalesObj);
                                    } else {
                                        // Último recurso: convertir a String y luego a Double
                                        String strValue = totalSalesObj.toString();
                                        log.info("Convirtiendo valor de tipo desconocido a String: {}", strValue);
                                        return Double.parseDouble(strValue);
                                    }
                                } catch (Exception e) {
                                    log.error("Error al convertir el valor totalSalesObj a Double: {}", e.getMessage());
                                    log.error("Valor que causó el error: {} de tipo {}",
                                            totalSalesObj, totalSalesObj.getClass().getName());
                                    return 0.0;
                                }
                            })
                            .onErrorResume(e -> {
                                log.error("Error en la agregación para proveedor {}: {}", providerId, e.getMessage());
                                return Mono.just(0.0);
                            })
                            .defaultIfEmpty(0.0)
                            .doOnSuccess(value -> {
                                log.info("Ventas totales calculadas para proveedor {}: {}", providerId, value);
                            });
                });
    }

    /**
     * Obtiene las campañas para un formato específico
     *
     * @param format Formato para el que se requieren las campañas
     * @return Flux<Campaign> Flujo de campañas
     */
    private Flux<Campaign> getCampaignsByFormat(String format) {
        // Buscamos en el array de media para el formato específico
        Query query = Query.query(
                Criteria.where("media").elemMatch(
                                Criteria.where("format").is(format)
                        )
                        .and("status").in("En proceso")
        );

        return reactiveMongoTemplate.find(query, Campaign.class);
    }


    /**
     * Implementación del método para generar KPIs de Sesiones para todos los formatos
     *
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    @Override
    public Flux<Kpi> processSessions() {
        String batchId = generateBatchId();
        log.info("Generando KPIs de Sesiones para todos los formatos. Batch ID: {}", batchId);

        // Ejecutamos todos los métodos de generación y concatenamos sus resultados
        return Flux.concat(

                generateKpiSessionsMailingCabecera(batchId),
                generateKpiSessionsMailingFeed(batchId),
                generateKpiSessionsMailingBody(batchId),
                generateKpiSessionsMailingParent(batchId),
                generateKpiSessionsPushWeb(batchId),
                generateKpiSessionsPushApp(batchId)
        );
    }

    /**
     * Genera KPIs de Sesiones para el formato Mailing Padre (MP)
     * MP-S = MCS + MFS + MBS
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiSessionsMailingParent(String batchId) {
        log.info("Generando KPI de Sesiones para Mailing Padre (MP)");

        // Obtener todas las campañas para el formato MP
        return getCampaignsByFormat(FORMAT_MP)
                .flatMap(campaign -> {
                    log.info("Procesando campaña para MP-S: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Para MP-S necesitamos la suma de sesiones de MC, MF y MB
                    // Primero obtenemos las sesiones para cada formato
                    Mono<Double> sessionsMC = calculateSessionsByProviderId(campaign.getProviderId(), FORMAT_MC);
                    Mono<Double> sessionsMF = calculateSessionsByProviderId(campaign.getProviderId(), FORMAT_MF);
                    Mono<Double> sessionsMB = calculateSessionsByProviderId(campaign.getProviderId(), FORMAT_MB);

                    // Combinamos los tres valores y los sumamos
                    return Mono.zip(sessionsMC, sessionsMF, sessionsMB)
                            .flatMap(tuple -> {
                                Double mcSessions = tuple.getT1();
                                Double mfSessions = tuple.getT2();
                                Double mbSessions = tuple.getT3();

                                log.info("Componentes de sesiones para MP-S: MC={}, MF={}, MB={}", mcSessions, mfSessions, mbSessions);

                                // Calcular la suma total
                                double totalSessions = mcSessions + mfSessions + mbSessions;
                                log.info("Total de sesiones calculado para MP-S: {}", totalSessions);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MP-S");
                                kpi.setKpiDescription("Sesiones Mailing Padre");
                                kpi.setValue(totalSessions);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MP);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                log.info("Guardando KPI MP-S para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Sesiones para el formato Mailing Cabecera (MC)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiSessionsMailingCabecera(String batchId) {
        log.info("Generando KPI de Sesiones para Mailing Cabecera (MC)");

        // Obtener todas las campañas para el formato MC
        return getCampaignsByFormat(FORMAT_MC)
                .flatMap(campaign -> {
                    log.info("Procesando campaña para MC-S: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las sesiones para esta campaña y formato
                    return calculateSessionsByProviderId(campaign.getProviderId(), FORMAT_MC)
                            .flatMap(sessions -> {
                                log.info("Sesiones calculadas para MC-S: {}", sessions);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MCS");
                                kpi.setKpiDescription("Sesiones Mailing Cabecera");
                                kpi.setValue(sessions);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MC);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                log.info("Guardando KPI MC-S para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Sesiones para el formato Mailing Feed (MF)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiSessionsMailingFeed(String batchId) {
        log.info("Generando KPI de Sesiones para Mailing Feed (MF)");

        // Obtener todas las campañas para el formato MF
        return getCampaignsByFormat(FORMAT_MF)
                .flatMap(campaign -> {
                    log.info("Procesando campaña para MF-S: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las sesiones para esta campaña y formato
                    return calculateSessionsByProviderId(campaign.getProviderId(), FORMAT_MF)
                            .flatMap(sessions -> {
                                log.info("Sesiones calculadas para MF-S: {}", sessions);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MFS");
                                kpi.setKpiDescription("Sesiones Mailing Feed");
                                kpi.setValue(sessions);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MF);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                log.info("Guardando KPI MF-S para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Sesiones para el formato Mailing Body (MB)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiSessionsMailingBody(String batchId) {
        log.info("Generando KPI de Sesiones para Mailing Body (MB)");

        // Obtener todas las campañas para el formato MB
        return getCampaignsByFormat(FORMAT_MB)
                .flatMap(campaign -> {
                    log.info("Procesando campaña para MB-S: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las sesiones para esta campaña y formato
                    return calculateSessionsByProviderId(campaign.getProviderId(), FORMAT_MB)
                            .flatMap(sessions -> {
                                log.info("Sesiones calculadas para MB-S: {}", sessions);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("MBS");
                                kpi.setKpiDescription("Sesiones Mailing Body");
                                kpi.setValue(sessions);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_MB);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                log.info("Guardando KPI MB-S para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Sesiones para el formato Push Web (PW)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiSessionsPushWeb(String batchId) {
        log.info("Generando KPI de Sesiones para Push Web (PW)");

        // Obtener todas las campañas para el formato PW
        return getCampaignsByFormat(FORMAT_PW)
                .flatMap(campaign -> {
                    log.info("Procesando campaña para PW-S: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las sesiones para esta campaña y formato
                    return calculateSessionsByProviderId(campaign.getProviderId(), FORMAT_PW)
                            .flatMap(sessions -> {
                                log.info("Sesiones calculadas para PW-S: {}", sessions);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("PW-S");
                                kpi.setKpiDescription("Sesiones Push Web");
                                kpi.setValue(sessions);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_PW);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                log.info("Guardando KPI PW-S para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Genera KPIs de Sesiones para el formato Push App (PA)
     *
     * @param batchId ID del lote de procesamiento
     * @return Flux<Kpi> Flujo de KPIs generados
     */
    private Flux<Kpi> generateKpiSessionsPushApp(String batchId) {
        log.info("Generando KPI de Sesiones para Push App (PA)");

        // Obtener todas las campañas para el formato PA
        return getCampaignsByFormat(FORMAT_PA)
                .flatMap(campaign -> {
                    log.info("Procesando campaña para PA-S: {}, providerId: {}", campaign.getCampaignId(), campaign.getProviderId());

                    // Obtener las sesiones para esta campaña y formato
                    return calculateSessionsByProviderId(campaign.getProviderId(), FORMAT_PA)
                            .flatMap(sessions -> {
                                log.info("Sesiones calculadas para PA-S: {}", sessions);

                                // Crear y guardar el KPI
                                Kpi kpi = new Kpi();
                                kpi.setCampaignId(campaign.getCampaignId());
                                kpi.setCampaignSubId(campaign.getCampaignId());
                                kpi.setKpiId("PA-S");
                                kpi.setKpiDescription("Sesiones Push App");
                                kpi.setValue(sessions);
                                kpi.setType("cantidad");
                                kpi.setCreatedUser("-");
                                kpi.setCreatedDate(LocalDateTime.now());
                                kpi.setUpdatedDate(LocalDateTime.now());
                                kpi.setStatus("A");
                                kpi.setFormat(FORMAT_PA);
                                kpi.setBatchId(batchId);
                                kpi.setTypeMedia(MEDIO_PROPIO);
                                kpi.setProviderId(campaign.getProviderId());
                                log.info("Guardando KPI PA-S para campaña: {}", kpi.getCampaignId());
                                return saveKpi(kpi);
                            });
                });
    }

    /**
     * Calcula el total de sesiones para un proveedor específico y un formato
     * Implementa exactamente el query:
     * SELECT sum(Sessions) FROM ga4_own_media a INNER JOIN Campaign c ON c.campaignId = a.campaignId WHERE c.ProveedorId = "ID"
     *
     * @param providerId ID del proveedor
     * @param format     Formato para el que se calculan las sesiones
     * @return Mono<Double> Total de sesiones
     */
    private Mono<Double> calculateSessionsByProviderId(String providerId, String format) {
        log.info("Calculando sesiones para proveedor: {} y formato: {}", providerId, format);

        // Primero obtenemos todos los campaignIds asociados con este providerId para el formato específico
        Query campaignsQuery = Query.query(
                Criteria.where("providerId").is(providerId)
                        .and("media").elemMatch(
                                Criteria.where("format").is(format)
                        )
        );

        log.info("Ejecutando consulta para obtener campañas con providerId: {} y formato: {}", providerId, format);

        return reactiveMongoTemplate.find(campaignsQuery, Campaign.class)
                .map(Campaign::getCampaignId)
                .filter(id -> id != null && !id.isEmpty())
                .collectList()
                .flatMap(campaignIds -> {
                    if (campaignIds.isEmpty()) {
                        log.warn("No se encontraron campañas para el proveedor: {} y formato: {}", providerId, format);
                        return Mono.just(0.0);
                    }

                    log.info("Campañas encontradas para proveedor {} y formato {}: {}", providerId, format, campaignIds.size());
                    log.debug("Lista de campaignIds: {}", campaignIds);

                    // Construir la agregación para calcular las sesiones totales
                    // Esto implementa el JOIN y WHERE del SQL original
                    Aggregation aggregation = Aggregation.newAggregation(
                            // Filtrar por campaignIds (implementa el JOIN con WHERE)
                            Aggregation.match(Criteria.where("campaignId").in(campaignIds)),
                            // Calcular suma de sessions
                            Aggregation.group().sum("sessions").as("totalSessions")
                    );

                    log.info("Ejecutando agregación para calcular sesiones en ga4_own_media");

                    return reactiveMongoTemplate.aggregate(
                                    aggregation,
                                    "ga4_own_media",
                                    Document.class
                            )
                            .next()
                            .map(result -> {
                                // Extraer el total de sesiones del resultado
                                Object totalSessionsObj = result.get("totalSessions");
                                log.info("Resultado de la agregación para proveedor {} y formato {}: totalSessionsObj = {}, tipo = {}",
                                        providerId, format, totalSessionsObj,
                                        totalSessionsObj != null ? totalSessionsObj.getClass().getName() : "null");

                                if (totalSessionsObj == null) {
                                    log.warn("totalSessionsObj es null para proveedor: {} y formato: {}", providerId, format);
                                    return 0.0;
                                }

                                // Convertir el resultado a Double de manera más robusta
                                try {
                                    if (totalSessionsObj instanceof Double) {
                                        return (Double) totalSessionsObj;
                                    } else if (totalSessionsObj instanceof Integer) {
                                        return ((Integer) totalSessionsObj).doubleValue();
                                    } else if (totalSessionsObj instanceof Long) {
                                        return ((Long) totalSessionsObj).doubleValue();
                                    } else if (totalSessionsObj instanceof Number) {
                                        // Manejar cualquier tipo numérico
                                        return ((Number) totalSessionsObj).doubleValue();
                                    } else if (totalSessionsObj instanceof String) {
                                        // Intentar convertir String a Double
                                        return Double.parseDouble((String) totalSessionsObj);
                                    } else {
                                        // Último recurso: convertir a String y luego a Double
                                        String strValue = totalSessionsObj.toString();
                                        log.info("Convirtiendo valor de tipo desconocido a String: {}", strValue);
                                        return Double.parseDouble(strValue);
                                    }
                                } catch (Exception e) {
                                    log.error("Error al convertir el valor totalSessionsObj a Double: {}", e.getMessage());
                                    log.error("Valor que causó el error: {} de tipo {}",
                                            totalSessionsObj, totalSessionsObj.getClass().getName());
                                    return 0.0;
                                }
                            })
                            .onErrorResume(e -> {
                                log.error("Error en la agregación para proveedor {} y formato {}: {}", providerId, format, e.getMessage());
                                return Mono.just(0.0);
                            })
                            .defaultIfEmpty(0.0)
                            .doOnSuccess(value -> {
                                log.info("Sesiones totales calculadas para proveedor {} y formato {}: {}", providerId, format, value);
                            });
                });
    }


    public Mono<Void> generateAllMetrics() {
        System.out.println("Iniciando proceso de generación de todas las métricas...");
        return generateMetricsGeneral();

    }


    public Mono<Void> generateMetricsGeneral() {
        System.out.println("Iniciando generación de métricas generales...");

        return reactiveMongoTemplate.findAll(Provider.class)
                .doOnNext(provider -> System.out.println("Procesando proveedor: " + provider.getProviderId()))
                .flatMap(provider -> {
                    String providerId = provider.getProviderId();
                    LocalDateTime now = LocalDateTime.now();
                    LocalDateTime startOfDay = now.toLocalDate().atStartOfDay();
                    LocalDateTime endOfDay = now.toLocalDate().atTime(23, 59, 59);

                    // 1. Contar campañas activas
                    Mono<Long> activeCampaignsMono = reactiveMongoTemplate.find(
                                    Query.query(Criteria.where("status").is("En proceso")
                                            .and("providerId").is(providerId)),
                                    Campaign.class)
                            .count()
                            .onErrorReturn(0L);

                    // 2. Sumar inversión
                    Mono<Double> totalInvestmentMono = reactiveMongoTemplate.find(
                                    Query.query(Criteria.where("status").is("En proceso")
                                            .and("providerId").is(providerId)),
                                    Campaign.class)
                            .collectList()
                            .map(campaigns -> campaigns.stream()
                                    .map(c -> c.getInvestment() != null ? c.getInvestment() : 0)
                                    .mapToDouble(Integer::doubleValue)
                                    .sum())
                            .onErrorReturn(0.0);


                    // 3. Sumar KPIs del día (MP-V, PW-V, PA-V) para el provider actual
                    Mono<Double> totalSalesMono = reactiveMongoTemplate.find(
                                    Query.query(Criteria.where("providerId").is(providerId)
                                            .and("status").in("En proceso")), Campaign.class)
                            .flatMap(campaign -> reactiveMongoTemplate.find(
                                            Query.query(Criteria.where("campaignId").is(campaign.getCampaignId())
                                                    .and("createdDate").gte(startOfDay).lte(endOfDay)
                                                    .and("kpiId").in("MF-V", "MB-V", "MC-V", "PW-V", "PA-V")),
                                            Kpi.class)
                                    .map(kpi -> kpi.getValue() != null ? kpi.getValue() : 0.0)
                                    .reduce(0.0, Double::sum)
                            )
                            .reduce(0.0, Double::sum)
                            .doOnNext(sum -> log.info("Total ventas para {}: {}", providerId, sum))
                            .onErrorReturn(0.0);

                    return Mono.zip(activeCampaignsMono, totalInvestmentMono, totalSalesMono)
                            .flatMap(tuple -> {
                                int totalActive = tuple.getT1().intValue();
                                double investment = tuple.getT2();
                                double sales = tuple.getT3();

                                Query existingMetricsQuery = new Query(Criteria.where("providerId").is(providerId)
                                        .and("createdDate").gte(startOfDay).lte(endOfDay));

                                return reactiveMongoTemplate.findOne(existingMetricsQuery, Metrics.class)
                                        .flatMap(existingMetrics -> {
                                            // Actualizar existente
                                            existingMetrics.setTotalActiveCampaigns(totalActive);
                                            existingMetrics.setTotalInvestmentPeriod(investment);
                                            existingMetrics.setTotalSales(sales);
                                            existingMetrics.setUpdatedDate(now);
                                            return reactiveMongoTemplate.save(existingMetrics)
                                                    .doOnSuccess(m -> System.out.println("Métrica actualizada para proveedor " + providerId));
                                        })
                                        .switchIfEmpty(Mono.defer(() -> {
                                            // Crear nueva si no existe
                                            Metrics metrics = Metrics.builder()
                                                    .providerId(providerId)
                                                    .totalActiveCampaigns(totalActive)
                                                    .totalInvestmentPeriod(investment)
                                                    .totalSales(sales)
                                                    .createdUser("system")
                                                    .createdDate(now)
                                                    .updatedDate(now)
                                                    .build();
                                            return reactiveMongoTemplate.save(metrics)
                                                    .doOnSuccess(m -> System.out.println("Métrica creada para proveedor " + providerId));
                                        }))
                                        .doOnError(e -> System.err.println("Error al guardar métrica de " + providerId + ": " + e.getMessage()));
                            });
                })
                .then()
                .doOnSuccess(v -> System.out.println("Proceso completado correctamente."))
                .doOnError(e -> System.err.println("Error general en generación de métricas: " + e.getMessage()));
    }

}




