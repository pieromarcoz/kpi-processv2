package pe.farmaciasperuanas.digital.process.kpi.application.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.stereotype.Service;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.Campaign;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.CampaignMedium;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.CampaignRepository;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.KpiRepositoryCustom;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.service.AdminNotificationService;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.service.KpiValueValidatorService;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.service.PaidMediaKpiCalculatorService;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.service.PaidMediaMetricsService;
import pe.farmaciasperuanas.digital.process.kpi.infrastructure.RetryHandler;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Implementación mejorada del servicio de cálculo de KPIs para medios pagados
 * usando MongoDB para el procesamiento y almacenamiento de métricas.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class MongoDbPaidMediaKpiCalculator implements PaidMediaKpiCalculatorService {

    private final KpiRepositoryCustom kpiRepository;
    private final CampaignRepository campaignRepository;
    private final ReactiveMongoTemplate mongoTemplate;
    private final KpiValueValidatorService kpiValidator;
    private final AdminNotificationService notificationService;
    private final PaidMediaMetricsService aggregationService;


    // Mapeo de formatos a prefijos de KPI
    private static final Map<String, String> FORMAT_PREFIX_MAP = Map.of(
            "CM", "CM-", // Meta Carrusel
            "VM", "VM-", // Meta Video
            "GS", "GS-", // Google Search
            "GD", "GD-"  // Google Display
    );

    // Códigos de KPI por formato
    private static final Map<String, List<String>> KPI_CODES_BY_FORMAT = Map.of(
            "CM", Arrays.asList("INV", "ROA", "ALC", "IMP", "FRE", "CLE", "CPC", "CTR", "VIP", "COV", "CPM", "COM", "CPA", "CON", "SES"),
            "VM", Arrays.asList("INV", "ROA", "ALC", "IMP", "FRE", "CLE", "CPC", "CTR", "VIP", "COV", "CPM", "THR", "CTH", "COM", "CPA", "CON", "SES"),
            "GS", Arrays.asList("INV", "ROA", "ALC", "IMP", "CLE", "CPC", "CTR", "CPM", "IMS", "SES", "COM", "CON"),
            "GD", Arrays.asList("INV", "ROA", "ALC", "IMP", "FRE", "CLE", "CPC", "CTR", "CPM", "SES", "COM", "CON")
    );

    @Override
    public Mono<Long> calculateAllPaidMediaKpis() {
        log.info("Iniciando cálculo de métricas de medios pagados");

        // Verificar si existe la colección KPI
        return mongoTemplate.collectionExists("kpi_des")
                .flatMap(exists -> {
                    if (!exists) {
                        log.info("La colección kpi_des no existe. Creándola...");
                        return mongoTemplate.createCollection("kpi_des")
                                .then(Mono.just(true));
                    }
                    return Mono.just(true);
                })
                .flatMap(collectionReady -> RetryHandler.withDefaultRetry(
                        () -> findActivePaidMediaCampaigns()
                                .flatMap(this::processProviderCampaigns)
                                .reduce(0L, Long::sum),
                        "Cálculo de KPIs de medios pagados"
                ))
                .doOnSuccess(count -> log.info("Cálculo de KPIs completado. Total procesado: {}", count))
                .doOnError(e -> {
                    log.error("Error en cálculo de KPIs: {}", e.getMessage(), e);
                    notificationService.notifyCriticalError(
                            "Error crítico en cálculo de KPIs de medios pagados",
                            "Se ha producido un error durante el cálculo de KPIs: " + e.getMessage()
                    );
                });
    }

    /**
     * Busca todas las campañas activas de medios pagados
     */
    private Flux<ArrayList<Campaign>> findActivePaidMediaCampaigns() {
        log.info("Buscando campañas activas de medios pagados");

        return campaignRepository.findAll() // Recupera todas las campañas sin filtrar por estado
                .filter(this::isPaidMediaCampaign) // Filtro por campañas de medios pagados
                .filter(this::hasValidFormat) // Filtro por los formatos dentro de media
                .doOnNext(campaign -> log.debug("Campaña de medios pagados encontrada: {}", campaign.getCampaignId()))
                .collectMultimap(Campaign::getProviderId) // Agrupamos por proveedor
                .map(Map::values) // Obtenemos las listas de campañas agrupadas por proveedor
                .flatMapIterable(values -> values) // Aplanamos las listas de campañas
                .map(ArrayList::new) // Convertimos cada grupo en una nueva lista
                .doOnNext(campaigns -> {
                    if (!campaigns.isEmpty()) {
                        log.info("Se han encontrado {} campañas filtradas.", campaigns.size());
                        // Logueamos todas las campañas filtradas
                        campaigns.forEach(campaign -> log.info("Campaña filtrada: {}", campaign.getCampaignId()));
                    } else {
                        log.info("No se encontraron campañas que coincidan con los filtros.");
                    }
                });
    }
    /**
     * Verifica si una campaña es de medios pagados
     */
    private boolean isPaidMediaCampaign(Campaign campaign) {
        if (campaign.getMedia() == null || campaign.getMedia().isEmpty()) {
            return false;
        }

        return campaign.getMedia().stream()
                .anyMatch(media -> "Medios pagados".equals(media.getMedium()));
    }

    // Método que verifica si el formato de la campaña dentro de media es válido (CM, MV, GS, GD)
    private boolean hasValidFormat(Campaign campaign) {
        if (campaign.getMedia() == null || campaign.getMedia().isEmpty()) {
            return false;
        }

        // Filtramos los medios por formato
        return campaign.getMedia().stream()
                .anyMatch(media -> media.getFormat() != null &&
                        media.getFormat().matches("CM|VM|GS|GD")); // Verifica si el formato es CM, MV, GS o GD
    }

    /**
     * Procesa las campañas de un proveedor
     */
    private Mono<Long> processProviderCampaigns(List<Campaign> providerCampaigns) {
        if (providerCampaigns.isEmpty()) {
            return Mono.just(0L);
        }

        String providerId = providerCampaigns.get(0).getProviderId();
        log.info("Procesando {} campañas para proveedor: {}", providerCampaigns.size(), providerId);

        return Flux.fromIterable(providerCampaigns)
                .flatMap(campaign -> RetryHandler.withDefaultRetry(
                        () -> processCampaignFormats(campaign),
                        "Procesamiento de formatos para campaña " + campaign.getCampaignId()
                ))
                .reduce(0L, Long::sum);
    }
    /**
     * Procesa los diferentes formatos de una campaña
     */
    private Mono<Long> processCampaignFormats(Campaign campaign) {
        if (campaign.getMedia() == null || campaign.getMedia().isEmpty()) {
            log.warn("Campaña sin medios definidos: {}", campaign.getCampaignId());
            return Mono.just(0L);
        }

        log.info("Procesando formatos para campaña: {}", campaign.getCampaignId());

        // Filtrar los medios únicos por formato para evitar duplicidades
        Set<String> processedFormats = new HashSet<>();
        List<Mono<Integer>> formatProcessingTasks = campaign.getMedia().stream()
                .filter(medium -> "Medios pagados".equals(medium.getMedium()))
                .filter(medium -> medium.getFormat() != null && medium.getFormat().matches("CM|VM|GS|GD"))
                .filter(medium -> {
                    String format = medium.getFormat();
                    // Solo procesamos el formato si no lo hemos procesado antes
                    boolean notProcessedYet = processedFormats.add(format);
                    if (!notProcessedYet) {
                        log.info("Formato {} ya procesado para campaña {}, omitiendo duplicado",
                                format, campaign.getCampaignId());
                    }
                    return notProcessedYet;
                })
                .map(medium -> processFormatKpis(campaign, medium, new AtomicInteger(0)))
                .collect(Collectors.toList());

        if (formatProcessingTasks.isEmpty()) {
            log.warn("No se encontraron formatos de medios pagados para la campaña: {}", campaign.getCampaignId());
            // Si no se encuentran formatos, creamos KPIs con valor 0 para al menos un formato por defecto
            return createDefaultKpisForCampaign(campaign);
        }

        return Flux.merge(formatProcessingTasks)
                .reduce(0, Integer::sum)
                .map(Long::valueOf)
                .doOnSuccess(count -> log.info("Procesados {} KPIs para campaña {}",
                        count, campaign.getCampaignId()));
    }

    /**
     * Verifica si ya existen KPIs calculados para una campaña y formato específico hoy
     * para evitar duplicaciones
     */
    private Mono<Integer> checkExistingKpisForToday(String campaignId, String formatCode) {
        LocalDateTime today = LocalDateTime.now().withHour(0).withMinute(0).withSecond(0).withNano(0);
        LocalDateTime tomorrow = today.plusDays(1);

        // Buscamos por campaignId y formato, filtrando por la fecha de hoy
        return kpiRepository.findByCampaignIdAndFormatAndCreatedDateBetween(
                        campaignId,
                        formatCode,
                        today,
                        tomorrow
                )
                .count()
                .map(Long::intValue)
                .doOnSuccess(count -> {
                    if (count > 0) {
                        log.info("Se encontraron {} KPIs existentes para campaña {} formato {} creados hoy",
                                count, campaignId, formatCode);
                    }
                });
    }
    /**
     * Crea KPIs por defecto para una campaña cuando no se encuentran formatos válidos
     */
    private Mono<Long> createDefaultKpisForCampaign(Campaign campaign) {
        log.info("Creando KPIs por defecto para campaña: {}", campaign.getCampaignId());

        // Elegimos un formato por defecto (en este caso VM)
        String defaultFormat = "VM";

        // Verificar si ya existen KPIs para esta campaña y formato hoy
        return checkExistingKpisForToday(campaign.getCampaignId(), defaultFormat)
                .flatMap(existingCount -> {
                    if (existingCount > 0) {
                        log.info("Ya existen {} KPIs para la campaña {} con formato {}. Omitiendo creación por defecto.",
                                existingCount, campaign.getCampaignId(), defaultFormat);
                        return Mono.just((long) existingCount);
                    }

                    // Si no existen, proceder a crear los KPIs por defecto
                    return processFormatKpis(campaign, createDefaultMedium(defaultFormat), new AtomicInteger(0))
                            .map(Long::valueOf)
                            .doOnSuccess(count -> log.info("Creados {} KPIs por defecto para campaña {}",
                                    count, campaign.getCampaignId()));
                });
    }

    /**
     * Crea un medio por defecto para un formato específico
     */
    private CampaignMedium createDefaultMedium(String format) {
        CampaignMedium medium = new CampaignMedium();
        medium.setMedium("Medios pagados");
        medium.setFormat(format);
        return medium;
    }

    /**
     * Procesa los KPIs para un formato específico de una campaña
     */
    /**
     * Procesa los KPIs para un formato específico de una campaña
     */
    private Mono<Integer> processFormatKpis(Campaign campaign, CampaignMedium medium, AtomicInteger counter) {
        String format = medium.getFormat();

        // Si el formato no está en nuestro mapa, intentamos identificarlo
        if (!FORMAT_PREFIX_MAP.containsKey(format)) {
            format = identifyFormatCode(medium);
            if (format == null) {
                format = "VM"; // Formato por defecto si no se puede identificar
                log.warn("Formato no identificado para medio, usando formato por defecto VM: {}", medium);
            }
        }

        final String formatCode = format;
        String kpiPrefix = FORMAT_PREFIX_MAP.get(formatCode);
        log.info("Proveedor:" + campaign.getProviderId());
        log.info("Calculando KPIs para campaña {} formato {}",
                campaign.getCampaignId(), formatCode);

        // En lugar de omitir el cálculo si ya existen KPIs, ahora siempre calculamos/actualizamos
        return checkExistingKpisForToday(campaign.getCampaignId(), formatCode)
                .flatMap(existingCount -> {
                    if (existingCount > 0) {
                        log.info("Se encontraron {} KPIs existentes para campaña {} formato {}. Actualizando valores.",
                                existingCount, campaign.getCampaignId(), formatCode);
                        counter.addAndGet(existingCount);
                    }

                    // Siempre calculamos/actualizamos los KPIs
                    return calculateKpisForFormat(campaign, formatCode, kpiPrefix)
                            .map(kpis -> {
                                int count = kpis.size();
                                if (existingCount == 0) {
                                    counter.addAndGet(count);
                                }
                                log.info("{} {} KPIs para campaña {} formato {}",
                                        existingCount > 0 ? "Actualizados" : "Calculados",
                                        count, campaign.getCampaignId(), formatCode);
                                return count > 0 ? count : existingCount;
                            })
                            .onErrorResume(e -> {
                                log.error("Error calculando KPIs para campaña {} formato {}: {}",
                                        campaign.getCampaignId(), formatCode, e.getMessage(), e);
                                // Asegurar que siempre se creen KPIs por defecto en caso de error
                                return createDefaultKpisForFormat(campaign, formatCode, kpiPrefix);
                            })
                            // Si no se calcularon KPIs, crear por defecto
                            .flatMap(count -> {
                                if (count == 0) {
                                    log.info("No se calcularon KPIs para campaña {} formato {}. Creando KPIs por defecto.",
                                            campaign.getCampaignId(), formatCode);
                                    return createDefaultKpisForFormat(campaign, formatCode, kpiPrefix);
                                }
                                return Mono.just(count);
                            });
                });
    }
    /**
     * Crea KPIs por defecto con valor 0 para un formato específico
     */
    private Mono<Integer> createDefaultKpisForFormat(Campaign campaign, String formatCode, String kpiPrefix) {
        List<String> kpiCodes = KPI_CODES_BY_FORMAT.getOrDefault(formatCode, Collections.emptyList());

        if (kpiCodes.isEmpty()) {
            log.warn("No hay códigos de KPI definidos para formato: {}", formatCode);
            return Mono.just(0);
        }

        log.info("Creando KPIs por defecto para campaña {} formato {}",
                campaign.getCampaignId(), formatCode);

        List<Mono<Kpi>> kpiCreations = kpiCodes.stream()
                .map(kpiCode -> createKpiWithZeroValue(campaign, formatCode, kpiPrefix, kpiCode))
                .collect(Collectors.toList());

        return Flux.merge(kpiCreations)
                .collectList()
                .map(List::size);
    }

    /**
     * Crea un KPI con valor 0
     */
    private Mono<Kpi> createKpiWithZeroValue(Campaign campaign, String formatCode, String kpiPrefix, String kpiCode) {
        String fullKpiCode = kpiPrefix + kpiCode;

        log.debug("Creando KPI por defecto: {} para campaña {}", fullKpiCode, campaign.getCampaignId());

        Kpi newKpi = Kpi.builder()
                .campaignId(campaign.getCampaignId())
                .campaignSubId(campaign.getCampaignSubId() != null ?
                        campaign.getCampaignSubId() : campaign.getCampaignId())
                .kpiId(fullKpiCode)
                .kpiDescription(getKpiDescription(kpiCode))
                .type(getKpiType(kpiCode))
                .value(0.0)
                .status("A")
                .createdUser("system")
                .createdDate(LocalDateTime.now())
                .updatedDate(LocalDateTime.now())
                .format(formatCode)
                .build();

        return kpiRepository.save(newKpi)
                .doOnSuccess(kpi -> log.info("KPI por defecto creado: {}", fullKpiCode))
                .doOnError(e -> log.error("Error creando KPI por defecto {}: {}", fullKpiCode, e.getMessage()));
    }
    /**
     * Identifica el código de formato basado en la información del medio
     */
    private String identifyFormatCode(CampaignMedium medium) {
        String platform = medium.getPlatform();
        String formatName = medium.getFormat();

        if (platform == null || formatName == null) {
            return null;
        }

        // Mapeos basados en la plataforma y nombre del formato
        if ("Meta".equals(platform)) {
            if (formatName.contains("Carrusel")) {
                return "CM";
            } else if (formatName.contains("Video")) {
                return "VM";
            }
        } else if ("Google".equals(platform)) {
            if (formatName.contains("Search")) {
                return "GS";
            } else if (formatName.contains("Display")) {
                return "GD";
            }
        }

        return null;
    }

    /**
     * Calcula los KPIs para un formato específico de una campaña
     */
    private Mono<List<Kpi>> calculateKpisForFormat(Campaign campaign, String formatCode, String kpiPrefix) {
        List<String> kpiCodes = KPI_CODES_BY_FORMAT.getOrDefault(formatCode, Collections.emptyList());

        if (kpiCodes.isEmpty()) {
            log.warn("No hay códigos de KPI definidos para formato: {}", formatCode);
            return Mono.just(Collections.emptyList());
        }

        // Intentar calcular cada KPI individualmente
        List<Mono<Kpi>> kpiCalculations = kpiCodes.stream()
                .map(kpiCode -> calculateSingleKpi(campaign, formatCode, kpiPrefix, kpiCode)
                        .onErrorResume(e -> {
                            log.error("Error calculando KPI para {}-{}: {}", formatCode, kpiCode, e.getMessage());
                            return Mono.empty(); // Retornar Empty en caso de error, luego se crearán por defecto si es necesario
                        }))
                .collect(Collectors.toList());

        // Combinar todos los KPIs calculados
        return Flux.merge(kpiCalculations)
                .collectList();
    }
    /**
     * Calcula un KPI específico para una campaña y formato
     */
    private Mono<Kpi> calculateSingleKpi(Campaign campaign, String formatCode, String kpiPrefix, String kpiCode) {
        String fullKpiCode = kpiPrefix + kpiCode;

        log.debug("Calculando KPI {} para campaña {}", fullKpiCode, campaign.getCampaignId());
        LocalDateTime startTime = LocalDateTime.now();

        // Verificar si ya existe un KPI calculado para hoy
        return findExistingKpi(campaign.getCampaignId(), fullKpiCode)
                .flatMap(existingKpi -> {
                    if (existingKpi != null) {
                        log.debug("KPI existente encontrado, actualizando: {}", fullKpiCode);
                        return updateExistingKpi(existingKpi, campaign, formatCode, kpiCode);
                    } else {
                        log.debug("Creando nuevo KPI: {}", fullKpiCode);
                        return createNewKpi(campaign, formatCode, kpiPrefix, kpiCode);
                    }
                })
                .doOnSuccess(kpi -> {
                    LocalDateTime endTime = LocalDateTime.now();
                    log.debug("KPI {} calculado en {} ms", fullKpiCode,
                            java.time.Duration.between(startTime, endTime).toMillis());
                });
    }

    /**
     * Busca un KPI existente para la campaña y código de KPI dado
     */
    private Mono<Kpi> findExistingKpi(String campaignId, String kpiId) {
        LocalDateTime today = LocalDateTime.now().withHour(0).withMinute(0).withSecond(0).withNano(0);
        LocalDateTime tomorrow = today.plusDays(1);

        return kpiRepository.findByCampaignIdAndKpiIdInAndCreatedDateBetween(
                campaignId,
                List.of(kpiId),
                today,
                tomorrow
        ).next();
    }

    /**
     * Actualiza un KPI existente
     */
    private Mono<Kpi> updateExistingKpi(Kpi existingKpi, Campaign campaign, String formatCode, String kpiCode) {
        return calculateKpiValue(campaign, formatCode, kpiCode)
                .defaultIfEmpty(0.0) // Asegurar que siempre hay un valor, incluso si la consulta no devuelve resultados
                .flatMap(value -> {
                    existingKpi.setValue(value);
                    existingKpi.setUpdatedDate(LocalDateTime.now());
                    existingKpi.setFormat(formatCode); // Asegurar que el formato está establecido

                    // Comprobar si el valor es sospechoso (cambio brusco)
                    return kpiValidator.isSuspiciousChange(existingKpi)
                            .flatMap(isSuspicious -> {
                                if (isSuspicious) {
                                    notificationService.notifyAnomalousValue(
                                            existingKpi.getKpiId(),
                                            campaign.getCampaignId(),
                                            value,
                                            "Cambio sospechoso detectado respecto a valores históricos"
                                    );
                                }

                                // Validar que el valor esté en rango aceptable
                                boolean isValidRange = kpiValidator.isValueInAcceptableRange(kpiCode, value);
                                if (!isValidRange) {
                                    log.warn("Valor fuera de rango para KPI {}: {}", existingKpi.getKpiId(), value);
                                }

                                log.info("Actualizando KPI: {} con valor: {}", existingKpi.getKpiId(), value);
                                return kpiRepository.save(existingKpi);
                            });
                });
    }
    /**
     * Crea un nuevo KPI
     */
    private Mono<Kpi> createNewKpi(Campaign campaign, String formatCode, String kpiPrefix, String kpiCode) {
        return calculateKpiValue(campaign, formatCode, kpiCode)
                .defaultIfEmpty(0.0) // Asegurar que siempre hay un valor, incluso si la consulta no devuelve resultados
                .flatMap(value -> {
                    String fullKpiCode = kpiPrefix + kpiCode;

                    Kpi newKpi = Kpi.builder()
                            .campaignId(campaign.getCampaignId())
                            .campaignSubId(campaign.getCampaignSubId() != null ?
                                    campaign.getCampaignSubId() : campaign.getCampaignId())
                            .kpiId(fullKpiCode)
                            .kpiDescription(getKpiDescription(kpiCode))
                            .type(getKpiType(kpiCode))
                            .value(value)
                            .status("A")
                            .createdUser("system")
                            .createdDate(LocalDateTime.now())
                            .updatedDate(LocalDateTime.now())
                            .format(formatCode) // Añadir el formato al KPI
                            .build();

                    // Validar que el valor esté en rango aceptable
                    boolean isValidRange = kpiValidator.isValueInAcceptableRange(kpiCode, value);
                    if (!isValidRange) {
                        log.warn("Valor fuera de rango para nuevo KPI {}: {}", newKpi.getKpiId(), value);
                    }

                    log.info("Creando nuevo KPI: {} con valor: {}", fullKpiCode, value);
                    return kpiRepository.save(newKpi);
                });
    }

    /**
     * Obtiene el tipo para un código de KPI (cantidad, porcentaje, monetario)
     */
    private String getKpiType(String kpiCode) {
        if (Arrays.asList("ROA", "CTR", "FRE").contains(kpiCode)) {
            return "porcentaje";
        } else if (Arrays.asList("INV", "CPC", "CPM", "COV", "CPA", "COM", "CTH").contains(kpiCode)) {
            return "monetario";
        } else {
            return "cantidad";
        }
    }
    /**
     * Calcula el valor para un KPI específico basado en su código
     */
    private Mono<Double> calculateKpiValue(Campaign campaign, String formatCode, String kpiCode) {
        String campaignId = campaign.getCampaignId();
        log.debug("Calculando valor para KPI {}-{} de campaña {}", formatCode, kpiCode, campaignId);

        switch (kpiCode) {
            case "INV": // Inversión
                return aggregationService.calculateInvestment(campaignId,
                                formatCode,
                                campaign.getProviderId(),
                                campaign.getCampaignSubId() != null ? campaign.getCampaignSubId() : campaignId)
                        .doOnSuccess(value -> log.debug("Valor calculado para INV: {}", value))
                        .onErrorResume(e -> {
                            log.error("Error calculando INV: {}", e.getMessage());
                            return Mono.just(0.0);
                        });

            case "ALC": // Alcance
                return aggregationService.calculateReach(
                                campaignId,
                                formatCode,
                                campaign.getProviderId(),
                                campaign.getCampaignSubId() != null ? campaign.getCampaignSubId() : campaignId)
                        .doOnSuccess(value -> log.debug("Valor calculado para ALC: {}", value))
                        .onErrorResume(e -> {
                            log.error("Error calculando ALC: {}", e.getMessage());
                            return Mono.just(0.0);
                        });

            case "IMP": // Impresiones
                return aggregationService.calculateImpressions(campaignId, formatCode)
                        .doOnSuccess(value -> log.debug("Valor calculado para IMP: {}", value))
                        .onErrorResume(e -> {
                            log.error("Error calculando IMP: {}", e.getMessage());
                            return Mono.just(0.0);
                        });

            case "CLE": // Clics en el enlace
                return aggregationService.calculateClicks(campaignId, formatCode)
                        .doOnSuccess(value -> log.debug("Valor calculado para CLE: {}", value))
                        .onErrorResume(e -> {
                            log.error("Error calculando CLE: {}", e.getMessage());
                            return Mono.just(0.0);
                        });

            case "COM": // Compras/Ventas
                return aggregationService.calculateSales(campaignId, formatCode)
                        .doOnSuccess(value -> log.debug("Valor calculado para COM: {}", value))
                        .onErrorResume(e -> {
                            log.error("Error calculando COM: {}", e.getMessage());
                            return Mono.just(0.0);
                        });

            case "CON": // Conversiones
                return aggregationService.calculateConversions(campaignId, formatCode)
                        .doOnSuccess(value -> log.debug("Valor calculado para CON: {}", value))
                        .onErrorResume(e -> {
                            log.error("Error calculando CON: {}", e.getMessage());
                            return Mono.just(0.0);
                        });


            case "SES": // Sesiones
                return aggregationService.calculateSessions(campaignId,
                                formatCode,
                                campaign.getProviderId(),
                                campaign.getCampaignSubId() != null ? campaign.getCampaignSubId() : campaignId)
                        .doOnSuccess(value -> log.debug("Valor calculado para SES: {}", value))
                        .onErrorResume(e -> {
                            log.error("Error calculando SES: {}", e.getMessage());
                            return Mono.just(0.0);
                        });

            case "VIP": // Visitas a la página
                return aggregationService.calculatePageVisits(campaignId, formatCode)
                        .doOnSuccess(value -> log.debug("Valor calculado para VIP: {}", value))
                        .onErrorResume(e -> {
                            log.error("Error calculando VIP: {}", e.getMessage());
                            return Mono.just(0.0);
                        });

            case "THR": // ThruPlay (solo para Video)
                if ("VM".equals(formatCode)) {
                    return aggregationService.calculateThruPlay(campaignId)
                            .doOnSuccess(value -> log.debug("Valor calculado para THR: {}", value))
                            .onErrorResume(e -> {
                                log.error("Error calculando THR: {}", e.getMessage());
                                return Mono.just(0.0);
                            });
                }
                return Mono.just(0.0);

            case "IMS": // Impression Share % (solo para Google Search)
                if ("GS".equals(formatCode)) {
                    return aggregationService.calculateImpressionShare(campaignId)
                            .doOnSuccess(value -> log.debug("Valor calculado para IMS: {}", value))
                            .onErrorResume(e -> {
                                log.error("Error calculando IMS: {}", e.getMessage());
                                return Mono.just(0.0);
                            });
                }
                return Mono.just(0.0);

            // Métricas calculadas a partir de otras métricas
            case "FRE": // Frecuencia = Impresiones / Alcance
                return calculateDerivedMetric(
                        campaign,
                        formatCode,
                        "IMP",
                        "ALC",
                        (imp, alc) -> alc > 0 ? imp / alc : 0.0
                );

            case "ROA": // ROAS = Ventas / Inversión
                return calculateDerivedMetric(
                        campaign,
                        formatCode,
                        "COM",
                        "INV",
                        (com, inv) -> inv > 0 ? com / inv : 0.0
                );

            case "CPC": // CPC = Inversión / Clics
                return calculateDerivedMetric(
                        campaign,
                        formatCode,
                        "INV",
                        "CLE",
                        (inv, cle) -> cle > 0 ? inv / cle : 0.0
                );

            case "CTR": // CTR = Clics / Impresiones
                return calculateDerivedMetric(
                        campaign,
                        formatCode,
                        "CLE",
                        "IMP",
                        (cle, imp) -> imp > 0 ? (cle / imp) : 0.0
                );

            case "CPM": // CPM = (Inversión / Impresiones) * 1000
                return calculateDerivedMetric(
                        campaign,
                        formatCode,
                        "INV",
                        "IMP",
                        (inv, imp) -> imp > 0 ? (inv / imp) * 1000 : 0.0
                );

            case "COV": // Costo por Visita = Inversión / Visitas
                return calculateDerivedMetric(
                        campaign,
                        formatCode,
                        "INV",
                        "VIP",
                        (inv, vip) -> vip > 0 ? inv / vip : 0.0
                );

            case "CPA": // CPA = Inversión / Conversiones
                return calculateDerivedMetric(
                        campaign,
                        formatCode,
                        "INV",
                        "CON",
                        (inv, con) -> con > 0 ? inv / con : 0.0
                );

            case "CTH": // Costo por ThruPlay = Inversión / ThruPlay (solo para Video)
                if ("VM".equals(formatCode)) {
                    return calculateDerivedMetric(
                            campaign,
                            formatCode,
                            "INV",
                            "THR",
                            (inv, thr) -> thr > 0 ? inv / thr : 0.0
                    );
                }
                return Mono.just(0.0);

            default:
                log.warn("Código de KPI no reconocido: {}", kpiCode);
                return Mono.just(0.0);
        }
    }
    /**
     * Calcula una métrica derivada a partir de dos métricas base
     */
    private Mono<Double> calculateDerivedMetric(
            Campaign campaign,
            String formatCode,
            String metric1Code,
            String metric2Code,
            BiFunction<Double, Double, Double> calculator) {

        String prefix = FORMAT_PREFIX_MAP.get(formatCode);
        log.debug("Calculando métrica derivada: {}/{} para formato {}", metric1Code, metric2Code, formatCode);

        Mono<Double> metric1Mono = calculateKpiValue(campaign, formatCode, metric1Code);
        Mono<Double> metric2Mono = calculateKpiValue(campaign, formatCode, metric2Code);

        // Combina ambos Mono<Double> usando el BiFunction y asegura que no hay valores nulos
        return Mono.zip(
                        metric1Mono.defaultIfEmpty(0.0),
                        metric2Mono.defaultIfEmpty(0.0)
                )
                .map(tuple -> calculator.apply(tuple.getT1(), tuple.getT2()))
                .doOnSuccess(result -> log.debug("Resultado de métrica derivada {}/{}: {}",
                        metric1Code, metric2Code, result))
                .onErrorResume(e -> {
                    log.error("Error calculando métrica derivada {}/{}: {}",
                            metric1Code, metric2Code, e.getMessage());
                    return Mono.just(0.0);
                });
    }

    /**
     * Obtiene la descripción para un código de KPI
     */
    private String getKpiDescription(String kpiCode) {
        switch (kpiCode) {
            case "INV":
                return "Inversión";
            case "ROA":
                return "ROAS";
            case "ALC":
                return "Alcance";
            case "IMP":
                return "Impresiones";
            case "FRE":
                return "Frecuencia";
            case "CLE":
                return "Clics en el enlace";
            case "CPC":
                return "CPC";
            case "CTR":
                return "CTR";
            case "VIP":
                return "Visitas a la página";
            case "COV":
                return "Costo por Visita";
            case "CPM":
                return "CPM";
            case "COM":
                return "Compras";
            case "CPA":
                return "CPA";
            case "CON":
                return "Conversiones";
            case "SES":
                return "Sesiones";
            case "THR":
                return "ThruPlay";
            case "CTH":
                return "Costo por ThruPlay";
            case "IMS":
                return "Impression Share %";
            default:
                return kpiCode;  // Si no se encuentra el código, devuelve el código tal cual
        }
    }

    /**
     * Interfaz funcional para cálculos de métricas derivadas
     */
    @FunctionalInterface
    private interface BiFunction<T, U, R> {
        R apply(T t, U u);
    }
}