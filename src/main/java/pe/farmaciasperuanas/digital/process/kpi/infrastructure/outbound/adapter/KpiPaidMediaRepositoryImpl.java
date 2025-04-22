package pe.farmaciasperuanas.digital.process.kpi.infrastructure.outbound.adapter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.Provider;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.KpiPaidMediaRepository;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.KpiRepository;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.KpiRepositoryCustom;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.swing.text.Document;
import java.time.LocalDateTime;
import java.util.UUID;

@Repository
@Slf4j
@RequiredArgsConstructor
public class KpiPaidMediaRepositoryImpl  implements KpiPaidMediaRepository {

    @Autowired
    private ReactiveMongoTemplate reactiveMongoTemplate;
    private KpiRepositoryCustom kpiRepository;
    private static final String MEDIO_PROPIO = "MEDIO_PROPIO";
    private static final String MEDIO_PAGADO = "MEDIO_PAGADO";
    @Autowired
    public KpiPaidMediaRepositoryImpl(ReactiveMongoTemplate reactiveMongoTemplate,
                                      KpiRepositoryCustom kpiRepository) {
        this.reactiveMongoTemplate = reactiveMongoTemplate;
        this.kpiRepository = kpiRepository;

        // Verificaciones de nulos al inicializar
        if (this.reactiveMongoTemplate == null) {
            log.error("ReactiveMongoTemplate no ha sido inyectado correctamente");
        }

        if (this.kpiRepository == null) {
            log.error("KpiRepositoryCustom no ha sido inyectado correctamente");
        }
    }
    @Override
    public Flux<Kpi> CalculateInvestment() {

        log.info("Generando KPIs de CalculateInvestment para todos los formatos. Batch ID: {}","121");
        return Flux.concat(
                calculateMetaCarouselInvestment(),
                calculateMetaVideoInvestment(),
                calculateGoogleSearchInvestment(),
                calculateGoogleDisplayInvestment()
        );

    }
    @Override
    public Flux<Kpi> CalculateRoas() {
        log.info("Generando KPIs de CalculateRoas para todos los formatos. Batch ID: {}","121");
        return Flux.concat(
                calculateMetaCarouselRoas(),
                calculateMetaVideoRoas(),
                calculateGoogleSearchRoas(),
                calculateGoogleDisplayRoas()
        );
    }

    @Override
    public Flux<Kpi> CalculateScope() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateImpressions() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateFrequency() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateClics() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateCPC() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateCTR() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculatePageEngagements() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateCostPerPageEngagements() {
        return null;
    }

    @Override
    public Flux<Kpi> CalculateCPM() {
        return null;
    }

    @Override
    public Flux<Object> CalculateRevenue() {
        return null;
    }

    @Override
    public Flux<Object> CalculateCPA() {
        return null;
    }

    @Override
    public Flux<Object> CalculateConversions() {
        return null;
    }

    @Override
    public Flux<Object> CalculateSessions() {
        return null;
    }

    @Override
    public Flux<Object> CalculateThruPlay() {
        return null;
    }

    @Override
    public Flux<Object> CalculateCostPerThruPlay() {
        return null;
    }

    @Override
    public Flux<Object> CalculateImpressionShare() {
        return null;
    }




    // Meta Carousel Investment (CM-INV)
    private Flux<Kpi> calculateMetaCarouselInvestment() {
        log.info("Iniciando cálculo de inversión Meta Carrusel para todos los proveedores");

        // Obtener todos los proveedores
        return reactiveMongoTemplate.findAll(Provider.class)
                .doOnNext(provider -> {
                    log.info("Procesando proveedor: {} (ID: {})", provider.getName(), provider.getProviderId());
                })
                .flatMap(provider -> {
                    String providerId = provider.getProviderId();
                    log.debug("Ejecutando cálculo para providerId: {}", providerId);

                    try {
                        // Para cada proveedor, realizar la consulta
                        LookupOperation lookupCampaigns = LookupOperation.newLookup()
                                .from("campaigns")
                                .localField("campaignSubId")
                                .foreignField("campaignSubId")
                                .as("campaigns");

                        MatchOperation matchCampaignProvider = Aggregation.match(
                                Criteria.where("campaigns.providerId").is(providerId));

                        GroupOperation sumInversion = Aggregation.group()
                                .sum(ArithmeticOperators.Divide.valueOf("$Inversion_3_75").divideBy(0.40))
                                .as("inversion");

                        Aggregation aggregation = Aggregation.newAggregation(
                                lookupCampaigns,
                                matchCampaignProvider,
                                sumInversion
                        );

                        log.debug("Agregación creada para proveedor {}: {}", providerId, aggregation.toString());

                        // Ejecutar la agregación - Usar org.bson.Document en lugar de Document genérico
                        return reactiveMongoTemplate.aggregate(
                                        aggregation, "bq_analytics_slayer_ads_meta_metricas", org.bson.Document.class)
                                .doOnNext(doc -> {
                                    log.debug("Documento obtenido para proveedor {}: {}", providerId, doc.toJson());
                                })
                                .next()
                                .flatMap(result -> {
                                    if (result == null) {
                                        log.warn("No se encontraron resultados para el proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Object inversionObj = result.get("inversion");
                                    if (inversionObj == null) {
                                        log.warn("Campo 'inversion' no encontrado en el resultado para proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Double inversion = ((Number) inversionObj).doubleValue();
                                    log.info("Inversión calculada para proveedor {}: {}", providerId, inversion);

                                    // Crear objeto KPI
                                    String kpiId = "CM-INV";
                                    return saveOrUpdateKpi(
                                            kpiId,
                                            "Inversión",
                                            "Monetario",
                                            inversion,
                                            "S/ X,XXX.XX",
                                            providerId,
                                            MEDIO_PAGADO
                                    );
                                })
                                .doOnSuccess(kpi -> {
                                    if (kpi != null) {
                                        log.info("KPI guardado correctamente para proveedor {}: {}", providerId, kpi.getId());
                                    }
                                })
                                .onErrorResume(e -> {
                                    log.error("Error al calcular inversión Meta Carrusel para proveedor {}: {}",
                                            providerId, e.getMessage(), e);
                                    return Mono.empty();
                                });
                    } catch (Exception e) {
                        log.error("Error al crear agregación para proveedor {}: {}", providerId, e.getMessage(), e);
                        return Mono.empty();
                    }
                })
                .doOnComplete(() -> {
                    log.info("Cálculo de inversión Meta Carrusel completado para todos los proveedores");
                });
    }
    // Meta Video Investment (VM-INV)
    private Flux<Kpi> calculateMetaVideoInvestment() {
        log.info("Iniciando cálculo de inversión Meta Video para todos los proveedores");

        if (kpiRepository == null) {
            log.error("No se puede ejecutar calculateMetaVideoInvestment() porque kpiRepository es null");
            return Flux.empty();
        }

        // Obtener todos los proveedores
        return reactiveMongoTemplate.findAll(Provider.class)
                .doOnNext(provider -> {
                    log.info("Procesando proveedor para Meta Video: {} (ID: {})", provider.getName(), provider.getProviderId());
                })
                .flatMap(provider -> {
                    String providerId = provider.getProviderId();
                    log.debug("Ejecutando cálculo Meta Video para providerId: {}", providerId);

                    try {
                        // Para cada proveedor, realizar la consulta
                        LookupOperation lookupCampaigns = LookupOperation.newLookup()
                                .from("campaigns")
                                .localField("campaignSubId")
                                .foreignField("campaignSubId")
                                .as("campaigns");

                        MatchOperation matchCampaignProvider = Aggregation.match(
                                Criteria.where("campaigns.providerId").is(providerId));

                        GroupOperation sumInversion = Aggregation.group()
                                .sum(ArithmeticOperators.Divide.valueOf("$Inversion_3_75").divideBy(0.40))
                                .as("inversion");

                        Aggregation aggregation = Aggregation.newAggregation(
                                lookupCampaigns,
                                matchCampaignProvider,
                                sumInversion
                        );

                        log.debug("Agregación Meta Video creada para proveedor {}: {}", providerId, aggregation.toString());

                        // Ejecutar la agregación - Usar org.bson.Document explícitamente
                        return reactiveMongoTemplate.aggregate(
                                        aggregation, "bq_analytics_slayer_ads_meta_metricas", org.bson.Document.class)
                                .doOnNext(doc -> {
                                    log.debug("Documento Meta Video obtenido para proveedor {}: {}", providerId, doc.toJson());
                                })
                                .next()
                                .flatMap(result -> {
                                    if (result == null) {
                                        log.warn("No se encontraron resultados Meta Video para el proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Object inversionObj = result.get("inversion");
                                    if (inversionObj == null) {
                                        log.warn("Campo 'inversion' no encontrado en el resultado Meta Video para proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Double inversion = ((Number) inversionObj).doubleValue();
                                    log.info("Inversión Meta Video calculada para proveedor {}: {}", providerId, inversion);

                                    // Crear objeto KPI
                                    String kpiId = "VM-INV";
                                    return saveOrUpdateKpi(
                                            kpiId,
                                            "Inversión",
                                            "Monetario",
                                            inversion,
                                            "S/ X,XXX.XX",
                                            providerId,
                                            MEDIO_PAGADO
                                    );
                                })
                                .doOnSuccess(kpi -> {
                                    if (kpi != null) {
                                        log.info("KPI Meta Video guardado correctamente para proveedor {}: {}", providerId, kpi.getId());
                                    }
                                })
                                .onErrorResume(e -> {
                                    log.error("Error al calcular inversión Meta Video para proveedor {}: {}",
                                            providerId, e.getMessage(), e);
                                    return Mono.empty();
                                });
                    } catch (Exception e) {
                        log.error("Error al crear agregación Meta Video para proveedor {}: {}", providerId, e.getMessage(), e);
                        return Mono.empty();
                    }
                })
                .doOnComplete(() -> {
                    log.info("Cálculo de inversión Meta Video completado para todos los proveedores");
                });
    }

    // Google Search Investment (GS-INV)
    private Flux<Kpi> calculateGoogleSearchInvestment() {
        log.info("Iniciando cálculo de inversión Google Search para todos los proveedores");

        if (kpiRepository == null) {
            log.error("No se puede ejecutar calculateGoogleSearchInvestment() porque kpiRepository es null");
            return Flux.empty();
        }

        // Obtener todos los proveedores
        return reactiveMongoTemplate.findAll(Provider.class)
                .doOnNext(provider -> {
                    log.info("Procesando proveedor para Google Search: {} (ID: {})", provider.getName(), provider.getProviderId());
                })
                .flatMap(provider -> {
                    String providerId = provider.getProviderId();
                    log.debug("Ejecutando cálculo Google Search para providerId: {}", providerId);

                    try {
                        // Para cada proveedor, realizar la consulta
                        LookupOperation lookupCampaigns = LookupOperation.newLookup()
                                .from("campaigns")
                                .localField("campaignSubId")
                                .foreignField("campaignSubId")
                                .as("campaigns");

                        MatchOperation matchCampaignProvider = Aggregation.match(
                                Criteria.where("campaigns.providerId").is(providerId));

                        GroupOperation sumInversion = Aggregation.group()
                                .sum(ArithmeticOperators.Divide.valueOf("$Inversion_3_75").divideBy(0.40))
                                .as("inversion");

                        Aggregation aggregation = Aggregation.newAggregation(
                                lookupCampaigns,
                                matchCampaignProvider,
                                sumInversion
                        );

                        log.debug("Agregación Google Search creada para proveedor {}: {}", providerId, aggregation.toString());

                        // Ejecutar la agregación - Usar org.bson.Document explícitamente
                        return reactiveMongoTemplate.aggregate(
                                        aggregation, "bq_analytics_slayer_ads_google_metricas", org.bson.Document.class)
                                .doOnNext(doc -> {
                                    log.debug("Documento Google Search obtenido para proveedor {}: {}", providerId, doc.toJson());
                                })
                                .next()
                                .flatMap(result -> {
                                    if (result == null) {
                                        log.warn("No se encontraron resultados Google Search para el proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Object inversionObj = result.get("inversion");
                                    if (inversionObj == null) {
                                        log.warn("Campo 'inversion' no encontrado en el resultado Google Search para proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Double inversion = ((Number) inversionObj).doubleValue();
                                    log.info("Inversión Google Search calculada para proveedor {}: {}", providerId, inversion);

                                    // Crear objeto KPI
                                    String kpiId = "GS-INV";
                                    return saveOrUpdateKpi(
                                            kpiId,
                                            "Inversión",
                                            "Monetario",
                                            inversion,
                                            "S/ X,XXX.XX",
                                            providerId,
                                            MEDIO_PAGADO
                                    );
                                })
                                .doOnSuccess(kpi -> {
                                    if (kpi != null) {
                                        log.info("KPI Google Search guardado correctamente para proveedor {}: {}", providerId, kpi.getId());
                                    }
                                })
                                .onErrorResume(e -> {
                                    log.error("Error al calcular inversión Google Search para proveedor {}: {}",
                                            providerId, e.getMessage(), e);
                                    return Mono.empty();
                                });
                    } catch (Exception e) {
                        log.error("Error al crear agregación Google Search para proveedor {}: {}", providerId, e.getMessage(), e);
                        return Mono.empty();
                    }
                })
                .doOnComplete(() -> {
                    log.info("Cálculo de inversión Google Search completado para todos los proveedores");
                });
    }

    // Google Display Investment (GD-INV)
    private Flux<Kpi> calculateGoogleDisplayInvestment() {
        log.info("Iniciando cálculo de inversión Google Display para todos los proveedores");

        if (kpiRepository == null) {
            log.error("No se puede ejecutar calculateGoogleDisplayInvestment() porque kpiRepository es null");
            return Flux.empty();
        }

        // Obtener todos los proveedores
        return reactiveMongoTemplate.findAll(Provider.class)
                .doOnNext(provider -> {
                    log.info("Procesando proveedor para Google Display: {} (ID: {})", provider.getName(), provider.getProviderId());
                })
                .flatMap(provider -> {
                    String providerId = provider.getProviderId();
                    log.debug("Ejecutando cálculo Google Display para providerId: {}", providerId);

                    try {
                        // Para cada proveedor, realizar la consulta
                        LookupOperation lookupCampaigns = LookupOperation.newLookup()
                                .from("campaigns")
                                .localField("campaignSubId")
                                .foreignField("campaignSubId")
                                .as("campaigns");

                        MatchOperation matchCampaignProvider = Aggregation.match(
                                Criteria.where("campaigns.providerId").is(providerId));

                        GroupOperation sumInversion = Aggregation.group()
                                .sum(ArithmeticOperators.Divide.valueOf("$Inversion_3_75").divideBy(0.40))
                                .as("inversion");

                        Aggregation aggregation = Aggregation.newAggregation(
                                lookupCampaigns,
                                matchCampaignProvider,
                                sumInversion
                        );

                        log.debug("Agregación Google Display creada para proveedor {}: {}", providerId, aggregation.toString());

                        // Ejecutar la agregación - Usar org.bson.Document explícitamente
                        return reactiveMongoTemplate.aggregate(
                                        aggregation, "bq_analytics_slayer_ads_google_metricas", org.bson.Document.class)
                                .doOnNext(doc -> {
                                    log.debug("Documento Google Display obtenido para proveedor {}: {}", providerId, doc.toJson());
                                })
                                .next()
                                .flatMap(result -> {
                                    if (result == null) {
                                        log.warn("No se encontraron resultados Google Display para el proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Object inversionObj = result.get("inversion");
                                    if (inversionObj == null) {
                                        log.warn("Campo 'inversion' no encontrado en el resultado Google Display para proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Double inversion = ((Number) inversionObj).doubleValue();
                                    log.info("Inversión Google Display calculada para proveedor {}: {}", providerId, inversion);

                                    // Crear objeto KPI
                                    String kpiId = "GD-INV";
                                    return saveOrUpdateKpi(
                                            kpiId,
                                            "Inversión",
                                            "Monetario",
                                            inversion,
                                            "S/ X,XXX.XX",
                                            providerId,
                                            MEDIO_PAGADO
                                    );
                                })
                                .doOnSuccess(kpi -> {
                                    if (kpi != null) {
                                        log.info("KPI Google Display guardado correctamente para proveedor {}: {}", providerId, kpi.getId());
                                    }
                                })
                                .onErrorResume(e -> {
                                    log.error("Error al calcular inversión Google Display para proveedor {}: {}",
                                            providerId, e.getMessage(), e);
                                    return Mono.empty();
                                });
                    } catch (Exception e) {
                        log.error("Error al crear agregación Google Display para proveedor {}: {}", providerId, e.getMessage(), e);
                        return Mono.empty();
                    }
                })
                .doOnComplete(() -> {
                    log.info("Cálculo de inversión Google Display completado para todos los proveedores");
                });
    }


    // Meta Carousel ROAS (CM-ROA)
    private Flux<Kpi> calculateMetaCarouselRoas() {
        log.info("Iniciando cálculo de ROAS Meta Carrusel para todos los proveedores");

        if (kpiRepository == null) {
            log.error("No se puede ejecutar calculateMetaCarouselRoas() porque kpiRepository es null");
            return Flux.empty();
        }

        // Obtener todos los proveedores
        return reactiveMongoTemplate.findAll(Provider.class)
                .doOnNext(provider -> {
                    log.info("Procesando proveedor para ROAS Meta Carrusel: {} (ID: {})", provider.getName(), provider.getProviderId());
                })
                .flatMap(provider -> {
                    String providerId = provider.getProviderId();
                    log.debug("Ejecutando cálculo ROAS Meta Carrusel para providerId: {}", providerId);

                    try {
                        // Para cada proveedor, realizar la consulta
                        LookupOperation lookupCampaigns = LookupOperation.newLookup()
                                .from("campaigns")
                                .localField("campaignSubId")
                                .foreignField("campaignSubId")
                                .as("campaigns");

                        LookupOperation lookupGA4 = LookupOperation.newLookup()
                                .from("bd_analytics_slayer_ads_ga4_meta_tiktok")
                                .localField("campaignSubId")
                                .foreignField("campaignSubId")
                                .as("ga4_data");

                        MatchOperation matchCampaignProvider = Aggregation.match(
                                Criteria.where("campaigns.providerId").is(providerId));

                        GroupOperation summarize = Aggregation.group()
                                .sum("$ga4_data.TotalRevenue").as("totalRevenue")
                                .sum("$Inversion_3_75").as("inversion");

                        ProjectionOperation calculateRoas = Aggregation.project()
                                .and(ArithmeticOperators.Divide.valueOf("$totalRevenue")
                                        .divideBy(ArithmeticOperators.Divide.valueOf("$inversion").divideBy(0.40)))
                                .as("roas");

                        Aggregation aggregation = Aggregation.newAggregation(
                                lookupCampaigns,
                                lookupGA4,
                                matchCampaignProvider,
                                summarize,
                                calculateRoas
                        );

                        log.debug("Agregación ROAS Meta Carrusel creada para proveedor {}: {}", providerId, aggregation.toString());

                        // Ejecutar la agregación - Usar org.bson.Document explícitamente
                        return reactiveMongoTemplate.aggregate(
                                        aggregation, "bq_analytics_slayer_ads_meta_metricas", org.bson.Document.class)
                                .doOnNext(doc -> {
                                    log.debug("Documento ROAS Meta Carrusel obtenido para proveedor {}: {}", providerId, doc.toJson());
                                })
                                .next()
                                .flatMap(result -> {
                                    if (result == null) {
                                        log.warn("No se encontraron resultados ROAS Meta Carrusel para el proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Object roasObj = result.get("roas");
                                    if (roasObj == null) {
                                        log.warn("Campo 'roas' no encontrado en el resultado ROAS Meta Carrusel para proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Double roas = ((Number) roasObj).doubleValue();
                                    log.info("ROAS Meta Carrusel calculado para proveedor {}: {}", providerId, roas);

                                    // Crear objeto KPI
                                    String kpiId = "CM-ROA";
                                    return saveOrUpdateKpi(
                                            kpiId,
                                            "ROAS",
                                            "Número",
                                            roas,
                                            "X.X",
                                            providerId,
                                            MEDIO_PAGADO
                                    );
                                })
                                .doOnSuccess(kpi -> {
                                    if (kpi != null) {
                                        log.info("KPI ROAS Meta Carrusel guardado correctamente para proveedor {}: {}", providerId, kpi.getId());
                                    }
                                })
                                .onErrorResume(e -> {
                                    log.error("Error al calcular ROAS Meta Carrusel para proveedor {}: {}",
                                            providerId, e.getMessage(), e);
                                    return Mono.empty();
                                });
                    } catch (Exception e) {
                        log.error("Error al crear agregación ROAS Meta Carrusel para proveedor {}: {}", providerId, e.getMessage(), e);
                        return Mono.empty();
                    }
                })
                .doOnComplete(() -> {
                    log.info("Cálculo de ROAS Meta Carrusel completado para todos los proveedores");
                });
    }

    // Meta Video ROAS (VM-ROA)
    private Flux<Kpi> calculateMetaVideoRoas() {
        log.info("Iniciando cálculo de ROAS Meta Video para todos los proveedores");

        if (kpiRepository == null) {
            log.error("No se puede ejecutar calculateMetaVideoRoas() porque kpiRepository es null");
            return Flux.empty();
        }

        // Obtener todos los proveedores
        return reactiveMongoTemplate.findAll(Provider.class)
                .doOnNext(provider -> {
                    log.info("Procesando proveedor para ROAS Meta Video: {} (ID: {})", provider.getName(), provider.getProviderId());
                })
                .flatMap(provider -> {
                    String providerId = provider.getProviderId();
                    log.debug("Ejecutando cálculo ROAS Meta Video para providerId: {}", providerId);

                    try {
                        // Para cada proveedor, realizar la consulta
                        LookupOperation lookupCampaigns = LookupOperation.newLookup()
                                .from("campaigns")
                                .localField("campaignSubId")
                                .foreignField("campaignSubId")
                                .as("campaigns");

                        LookupOperation lookupGA4 = LookupOperation.newLookup()
                                .from("bd_analytics_slayer_ads_ga4_meta_tiktok")
                                .localField("campaignSubId")
                                .foreignField("campaignSubId")
                                .as("ga4_data");

                        MatchOperation matchCampaignProvider = Aggregation.match(
                                Criteria.where("campaigns.providerId").is(providerId));

                        GroupOperation summarize = Aggregation.group()
                                .sum("$ga4_data.TotalRevenue").as("totalRevenue")
                                .sum("$Inversion_3_75").as("inversion");

                        ProjectionOperation calculateRoas = Aggregation.project()
                                .and(ArithmeticOperators.Divide.valueOf("$totalRevenue")
                                        .divideBy(ArithmeticOperators.Divide.valueOf("$inversion").divideBy(0.40)))
                                .as("roas");

                        Aggregation aggregation = Aggregation.newAggregation(
                                lookupCampaigns,
                                lookupGA4,
                                matchCampaignProvider,
                                summarize,
                                calculateRoas
                        );

                        log.debug("Agregación ROAS Meta Video creada para proveedor {}: {}", providerId, aggregation.toString());

                        // Ejecutar la agregación - Usar org.bson.Document explícitamente
                        return reactiveMongoTemplate.aggregate(
                                        aggregation, "bq_analytics_slayer_ads_meta_metricas", org.bson.Document.class)
                                .doOnNext(doc -> {
                                    log.debug("Documento ROAS Meta Video obtenido para proveedor {}: {}", providerId, doc.toJson());
                                })
                                .next()
                                .flatMap(result -> {
                                    if (result == null) {
                                        log.warn("No se encontraron resultados ROAS Meta Video para el proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Object roasObj = result.get("roas");
                                    if (roasObj == null) {
                                        log.warn("Campo 'roas' no encontrado en el resultado ROAS Meta Video para proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Double roas = ((Number) roasObj).doubleValue();
                                    log.info("ROAS Meta Video calculado para proveedor {}: {}", providerId, roas);

                                    // Crear objeto KPI
                                    String kpiId = "VM-ROA";
                                    return saveOrUpdateKpi(
                                            kpiId,
                                            "ROAS",
                                            "Número",
                                            roas,
                                            "X.X",
                                            providerId,
                                            MEDIO_PAGADO
                                    );
                                })
                                .doOnSuccess(kpi -> {
                                    if (kpi != null) {
                                        log.info("KPI ROAS Meta Video guardado correctamente para proveedor {}: {}", providerId, kpi.getId());
                                    }
                                })
                                .onErrorResume(e -> {
                                    log.error("Error al calcular ROAS Meta Video para proveedor {}: {}",
                                            providerId, e.getMessage(), e);
                                    return Mono.empty();
                                });
                    } catch (Exception e) {
                        log.error("Error al crear agregación ROAS Meta Video para proveedor {}: {}", providerId, e.getMessage(), e);
                        return Mono.empty();
                    }
                })
                .doOnComplete(() -> {
                    log.info("Cálculo de ROAS Meta Video completado para todos los proveedores");
                });
    }

    // Google Search ROAS (GS-ROA)
    private Flux<Kpi> calculateGoogleSearchRoas() {
        log.info("Iniciando cálculo de ROAS Google Search para todos los proveedores");

        if (kpiRepository == null) {
            log.error("No se puede ejecutar calculateGoogleSearchRoas() porque kpiRepository es null");
            return Flux.empty();
        }

        // Obtener todos los proveedores
        return reactiveMongoTemplate.findAll(Provider.class)
                .doOnNext(provider -> {
                    log.info("Procesando proveedor para ROAS Google Search: {} (ID: {})", provider.getName(), provider.getProviderId());
                })
                .flatMap(provider -> {
                    String providerId = provider.getProviderId();
                    log.debug("Ejecutando cálculo ROAS Google Search para providerId: {}", providerId);

                    try {
                        // Para cada proveedor, realizar la consulta
                        LookupOperation lookupCampaigns = LookupOperation.newLookup()
                                .from("campaigns")
                                .localField("campaignSubId")
                                .foreignField("campaignSubId")
                                .as("campaigns");

                        LookupOperation lookupGA4 = LookupOperation.newLookup()
                                .from("bd_analytics_slayer_ads_ga4_meta_google")
                                .localField("campaignSubId")
                                .foreignField("campaignSubId")
                                .as("ga4_data");

                        MatchOperation matchCampaignProvider = Aggregation.match(
                                Criteria.where("campaigns.providerId").is(providerId));

                        GroupOperation summarize = Aggregation.group()
                                .sum("$ga4_data.TotalRevenue").as("totalRevenue")
                                .sum("$Inversion_3_75").as("inversion");

                        ProjectionOperation calculateRoas = Aggregation.project()
                                .and(ArithmeticOperators.Divide.valueOf("$totalRevenue")
                                        .divideBy(ArithmeticOperators.Divide.valueOf("$inversion").divideBy(0.40)))
                                .as("roas");

                        Aggregation aggregation = Aggregation.newAggregation(
                                lookupCampaigns,
                                lookupGA4,
                                matchCampaignProvider,
                                summarize,
                                calculateRoas
                        );

                        log.debug("Agregación ROAS Google Search creada para proveedor {}: {}", providerId, aggregation.toString());

                        // Ejecutar la agregación - Usar org.bson.Document explícitamente
                        return reactiveMongoTemplate.aggregate(
                                        aggregation, "bq_analytics_slayer_ads_google_metricas", org.bson.Document.class)
                                .doOnNext(doc -> {
                                    log.debug("Documento ROAS Google Search obtenido para proveedor {}: {}", providerId, doc.toJson());
                                })
                                .next()
                                .flatMap(result -> {
                                    if (result == null) {
                                        log.warn("No se encontraron resultados ROAS Google Search para el proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Object roasObj = result.get("roas");
                                    if (roasObj == null) {
                                        log.warn("Campo 'roas' no encontrado en el resultado ROAS Google Search para proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Double roas = ((Number) roasObj).doubleValue();
                                    log.info("ROAS Google Search calculado para proveedor {}: {}", providerId, roas);

                                    // Crear objeto KPI
                                    String kpiId = "GS-ROA";
                                    return saveOrUpdateKpi(
                                            kpiId,
                                            "ROAS",
                                            "Número",
                                            roas,
                                            "X.X",
                                            providerId,
                                            MEDIO_PAGADO
                                    );
                                })
                                .doOnSuccess(kpi -> {
                                    if (kpi != null) {
                                        log.info("KPI ROAS Google Search guardado correctamente para proveedor {}: {}", providerId, kpi.getId());
                                    }
                                })
                                .onErrorResume(e -> {
                                    log.error("Error al calcular ROAS Google Search para proveedor {}: {}",
                                            providerId, e.getMessage(), e);
                                    return Mono.empty();
                                });
                    } catch (Exception e) {
                        log.error("Error al crear agregación ROAS Google Search para proveedor {}: {}", providerId, e.getMessage(), e);
                        return Mono.empty();
                    }
                })
                .doOnComplete(() -> {
                    log.info("Cálculo de ROAS Google Search completado para todos los proveedores");
                });
    }

    // Google Display ROAS (GD-ROA)
    private Flux<Kpi> calculateGoogleDisplayRoas() {
        log.info("Iniciando cálculo de ROAS Google Display para todos los proveedores");

        if (kpiRepository == null) {
            log.error("No se puede ejecutar calculateGoogleDisplayRoas() porque kpiRepository es null");
            return Flux.empty();
        }

        // Obtener todos los proveedores
        return reactiveMongoTemplate.findAll(Provider.class)
                .doOnNext(provider -> {
                    log.info("Procesando proveedor para ROAS Google Display: {} (ID: {})", provider.getName(), provider.getProviderId());
                })
                .flatMap(provider -> {
                    String providerId = provider.getProviderId();
                    log.debug("Ejecutando cálculo ROAS Google Display para providerId: {}", providerId);

                    try {
                        // Para cada proveedor, realizar la consulta
                        LookupOperation lookupCampaigns = LookupOperation.newLookup()
                                .from("campaigns")
                                .localField("campaignSubId")
                                .foreignField("campaignSubId")
                                .as("campaigns");

                        LookupOperation lookupGA4 = LookupOperation.newLookup()
                                .from("bd_analytics_slayer_ads_ga4_meta_google")
                                .localField("campaignSubId")
                                .foreignField("campaignSubId")
                                .as("ga4_data");

                        MatchOperation matchCampaignProvider = Aggregation.match(
                                Criteria.where("campaigns.providerId").is(providerId));

                        GroupOperation summarize = Aggregation.group()
                                .sum("$ga4_data.TotalRevenue").as("totalRevenue")
                                .sum("$Inversion_3_75").as("inversion");

                        ProjectionOperation calculateRoas = Aggregation.project()
                                .and(ArithmeticOperators.Divide.valueOf("$totalRevenue")
                                        .divideBy(ArithmeticOperators.Divide.valueOf("$inversion").divideBy(0.40)))
                                .as("roas");

                        Aggregation aggregation = Aggregation.newAggregation(
                                lookupCampaigns,
                                lookupGA4,
                                matchCampaignProvider,
                                summarize,
                                calculateRoas
                        );

                        log.debug("Agregación ROAS Google Display creada para proveedor {}: {}", providerId, aggregation.toString());

                        // Ejecutar la agregación - Usar org.bson.Document explícitamente
                        return reactiveMongoTemplate.aggregate(
                                        aggregation, "bq_analytics_slayer_ads_google_metricas", org.bson.Document.class)
                                .doOnNext(doc -> {
                                    log.debug("Documento ROAS Google Display obtenido para proveedor {}: {}", providerId, doc.toJson());
                                })
                                .next()
                                .flatMap(result -> {
                                    if (result == null) {
                                        log.warn("No se encontraron resultados ROAS Google Display para el proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Object roasObj = result.get("roas");
                                    if (roasObj == null) {
                                        log.warn("Campo 'roas' no encontrado en el resultado ROAS Google Display para proveedor: {}", providerId);
                                        return Mono.empty();
                                    }

                                    Double roas = ((Number) roasObj).doubleValue();
                                    log.info("ROAS Google Display calculado para proveedor {}: {}", providerId, roas);

                                    // Crear objeto KPI
                                    String kpiId = "GD-ROA";
                                    return saveOrUpdateKpi(
                                            kpiId,
                                            "ROAS",
                                            "Número",
                                            roas,
                                            "X.X",
                                            providerId,
                                            MEDIO_PAGADO
                                    );
                                })
                                .doOnSuccess(kpi -> {
                                    if (kpi != null) {
                                        log.info("KPI ROAS Google Display guardado correctamente para proveedor {}: {}", providerId, kpi.getId());
                                    }
                                })
                                .onErrorResume(e -> {
                                    log.error("Error al calcular ROAS Google Display para proveedor {}: {}",
                                            providerId, e.getMessage(), e);
                                    return Mono.empty();
                                });
                    } catch (Exception e) {
                        log.error("Error al crear agregación ROAS Google Display para proveedor {}: {}", providerId, e.getMessage(), e);
                        return Mono.empty();
                    }
                })
                .doOnComplete(() -> {
                    log.info("Cálculo de ROAS Google Display completado para todos los proveedores");
                });
    }

    // Método auxiliar para guardar o actualizar KPI
    private Mono<Kpi> saveOrUpdateKpi(String kpiId,
                                      String kpiDescription,
                                      String type,
                                      Double value,
                                      String format,
                                      String providerId,
                                      String typeMedia) {
        // Buscar si ya existe un KPI con el mismo ID y fecha de hoy
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime startOfDay = now.toLocalDate().atStartOfDay();
        LocalDateTime endOfDay = now.toLocalDate().atTime(23, 59, 59);

        return kpiRepository.findByKpiIdAndProviderIdAndCreatedDateBetween(
                        kpiId, providerId, startOfDay, endOfDay)
                .flatMap(existingKpi -> {
                    // Si existe, actualizar
                    existingKpi.setValue(value);
                    existingKpi.setUpdatedDate(now);
                    return kpiRepository.save(existingKpi);
                })
                .switchIfEmpty(
                        // Si no existe, crear nuevo
                        Mono.just(Kpi.builder()
                                        .kpiId(kpiId)
                                        .kpiDescription(kpiDescription)
                                        .type(type)
                                        .value(value)
                                        .format(format)
                                        .status("ACTIVE")
                                        .providerId(providerId)
                                        .typeMedia(typeMedia)
                                        .createdUser("SYSTEM")
                                        .createdDate(now)
                                        .updatedDate(now)
                                        .batchId(UUID.randomUUID().toString())
                                        .build())
                                .flatMap(kpiRepository::save)
                );
    }










}
