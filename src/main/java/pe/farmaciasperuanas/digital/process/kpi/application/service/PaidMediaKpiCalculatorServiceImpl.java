//package pe.farmaciasperuanas.digital.process.kpi.application.service;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
//import org.springframework.stereotype.Service;
//import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
//import pe.farmaciasperuanas.digital.process.kpi.domain.model.Campaign;
//import pe.farmaciasperuanas.digital.process.kpi.domain.model.CampaignMedium;
//import pe.farmaciasperuanas.digital.process.kpi.domain.model.KpiModel;
//import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.CampaignRepository;
//import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.KpiRepositoryCustom;
//import pe.farmaciasperuanas.digital.process.kpi.domain.port.service.PaidMediaKpiCalculatorService;
//import reactor.core.publisher.Flux;
//import reactor.core.publisher.Mono;
//
//import java.time.LocalDateTime;
//import java.util.*;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.stream.Collectors;
//
///**
// * Servicio de cálculo de KPIs para medios pagados
// * basado en la historia de usuario para el cálculo automatizado
// * de métricas de medios pagados.
// */
//@Service
//@Slf4j
//@RequiredArgsConstructor
//public class PaidMediaKpiCalculatorServiceImpl implements PaidMediaKpiCalculatorService {
//
//    private final KpiRepositoryCustom kpiRepository;
//    private final CampaignRepository campaignRepository;
//    private final ReactiveMongoTemplate mongoTemplate;
//
//    // Mapeo de formatos a prefijos de KPI
//    private static final Map<String, String> FORMAT_PREFIX_MAP = Map.of(
//            "CM", "CM-", // Meta Carrusel
//            "VM", "VM-", // Meta Video
//            "GS", "GS-", // Google Search
//            "GD", "GD-"  // Google Display
//    );
//
//    // Códigos de KPI por formato
//    private static final Map<String, List<String>> KPI_CODES_BY_FORMAT = Map.of(
//            "CM", Arrays.asList("INV", "ROA", "ALC", "IMP", "FRE", "CLE", "CPC", "CTR", "VIP", "COV", "CPM", "COM", "CPA", "CON", "SES"),
//            "VM", Arrays.asList("INV", "ROA", "ALC", "IMP", "FRE", "CLE", "CPC", "CTR", "VIP", "COV", "CPM", "THR", "CTH", "COM", "CPA", "CON", "SES"),
//            "GS", Arrays.asList("INV", "ROA", "ALC", "IMP", "CLE", "CPC", "CTR", "CPM", "IMS", "SES", "COM", "CON"),
//            "GD", Arrays.asList("INV", "ROA", "ALC", "IMP", "FRE", "CLE", "CPC", "CTR", "CPM", "SES", "COM", "CON")
//    );
//
//    // Validaciones de rango de valores
//    private static final Map<String, double[]> KPI_VALID_RANGES = Map.of(
//            "ROA", new double[]{0.1, 20.0},
//            "CTR", new double[]{0.001, 0.20},
//            "CPC", new double[]{0.01, 100.0},
//            "CPM", new double[]{0.5, 50.0},
//            "FRE", new double[]{1.0, 10.0}
//    );
//
//    @Override
//    public Mono<Long> calculateAllPaidMediaKpis() {
//        log.info("Iniciando cálculo de métricas de medios pagados");
//
//        // Obtenemos todas las campañas activas de medios pagados
//        return findActivePaidMediaCampaigns()
//                .flatMap(this::processProviderCampaigns)
//                .reduce(0L, Long::sum)
//                .doOnSuccess(count -> log.info("Cálculo de KPIs completado. Total procesado: {}", count))
//                .doOnError(e -> log.error("Error en cálculo de KPIs: {}", e.getMessage(), e));
//    }
//
//    /**
//     * Busca todas las campañas activas de medios pagados
//     */
//    private Flux<List<Campaign>> findActivePaidMediaCampaigns() {
//        return campaignRepository.findAll()
//                .filter(this::isPaidMediaCampaign)
//                .filter(this::isActiveCampaign)
//                .collectMultimap(Campaign::getProviderId)
//                .map(Map::values)
//                .flatMapIterable(values -> values)
//                .map(ArrayList::new);
//    }
//
//    /**
//     * Verifica si una campaña es de medios pagados
//     */
//    private boolean isPaidMediaCampaign(Campaign campaign) {
//        if (campaign.getMedia() == null || campaign.getMedia().isEmpty()) {
//            return false;
//        }
//
//        return campaign.getMedia().stream()
//                .anyMatch(media -> "Medios pagados".equals(media.getMedium()));
//    }
//
//    /**
//     * Verifica si una campaña está activa
//     */
//    private boolean isActiveCampaign(Campaign campaign) {
//        String status = campaign.getStatus();
//        return "En proceso".equals(status) || "Programado".equals(status);
//    }
//
//    /**
//     * Procesa las campañas de un proveedor
//     */
//    private Mono<Long> processProviderCampaigns(List<Campaign> providerCampaigns) {
//        if (providerCampaigns.isEmpty()) {
//            return Mono.just(0L);
//        }
//
//        String providerId = providerCampaigns.get(0).getProviderId();
//        log.info("Procesando {} campañas para proveedor: {}", providerCampaigns.size(), providerId);
//
//        return Flux.fromIterable(providerCampaigns)
//                .flatMap(this::processCampaignFormats)
//                .reduce(0L, Long::sum);
//    }
//
//    /**
//     * Procesa los diferentes formatos de una campaña
//     */
//    private Mono<Long> processCampaignFormats(Campaign campaign) {
//        if (campaign.getMedia() == null || campaign.getMedia().isEmpty()) {
//            log.warn("Campaña sin medios definidos: {}", campaign.getCampaignId());
//            return Mono.just(0L);
//        }
//
//        log.info("Procesando formatos para campaña: {}", campaign.getCampaignId());
//
//        AtomicInteger processedCount = new AtomicInteger(0);
//
//        // Procesar cada formato de forma asíncrona
//        List<Mono<Integer>> formatProcessingTasks = campaign.getMedia().stream()
//                .map(medium -> processFormatKpis(campaign, medium, processedCount))
//                .collect(Collectors.toList());
//
//        return Flux.merge(formatProcessingTasks)
//                .reduce(0, Integer::sum)
//                .map(Long::valueOf);
//    }
//
//    /**
//     * Procesa los KPIs para un formato específico de una campaña
//     */
//    private Mono<Integer> processFormatKpis(Campaign campaign, CampaignMedium medium, AtomicInteger counter) {
//        String format = medium.getFormat();
//
//        // Si el formato no está en nuestro mapa, intentamos identificarlo
//        if (!FORMAT_PREFIX_MAP.containsKey(format)) {
//            format = identifyFormatCode(medium);
//            if (format == null) {
//                log.warn("Formato no identificado para medio: {}", medium);
//                return Mono.just(0);
//            }
//        }
//
//        final String formatCode = format;
//        String kpiPrefix = FORMAT_PREFIX_MAP.get(formatCode);
//
//        log.info("Calculando KPIs para campaña {} formato {}",
//                campaign.getCampaignId(), formatCode);
//
//        return calculateKpisForFormat(campaign, formatCode, kpiPrefix)
//                .map(kpis -> {
//                    int count = kpis.size();
//                    counter.addAndGet(count);
//                    log.info("Calculados {} KPIs para campaña {} formato {}",
//                            count, campaign.getCampaignId(), formatCode);
//                    return count;
//                })
//                .onErrorResume(e -> {
//                    log.error("Error calculando KPIs para campaña {} formato {}: {}",
//                            campaign.getCampaignId(), formatCode, e.getMessage(), e);
//                    return Mono.just(0);
//                });
//    }
//
//    /**
//     * Identifica el código de formato basado en la información del medio
//     */
//    private String identifyFormatCode(CampaignMedium medium) {
//        String platform = medium.getPlatform();
//        String formatName = medium.getFormat();
//
//        // Mapeos basados en la plataforma y nombre del formato
//        if ("Meta".equals(platform)) {
//            if ("Carrusel".equals(formatName) || formatName.contains("Carrusel")) {
//                return "CM";
//            } else if ("Video".equals(formatName) || formatName.contains("Video")) {
//                return "VM";
//            }
//        } else if ("Google".equals(platform)) {
//            if ("Search".equals(formatName) || formatName.contains("Search")) {
//                return "GS";
//            } else if ("Display".equals(formatName) || formatName.contains("Display")) {
//                return "GD";
//            }
//        }
//
//        return null;
//    }
//
//    /**
//     * Calcula los KPIs para un formato específico de una campaña
//     */
//    private Mono<List<Kpi>> calculateKpisForFormat(Campaign campaign, String formatCode, String kpiPrefix) {
//        List<String> kpiCodes = KPI_CODES_BY_FORMAT.getOrDefault(formatCode, Collections.emptyList());
//
//        if (kpiCodes.isEmpty()) {
//            log.warn("No hay códigos de KPI definidos para formato: {}", formatCode);
//            return Mono.just(Collections.emptyList());
//        }
//
//        // Procesar cada KPI de forma paralela
//        List<Mono<Kpi>> kpiCalculations = kpiCodes.stream()
//                .map(kpiCode -> calculateSingleKpi(campaign, formatCode, kpiPrefix, kpiCode))
//                .collect(Collectors.toList());
//
//        // Combina todos los Mono<Kpi> en un solo Flux y luego colecciona los resultados en una lista
//        return Flux.merge(kpiCalculations)
//                .collectList();  // Recoge todos los elementos emitidos y los devuelve en una lista
//    }
//
//    /**
//     * Calcula un KPI específico para una campaña y formato
//     */
//    private Mono<Kpi> calculateSingleKpi(Campaign campaign, String formatCode, String kpiPrefix, String kpiCode) {
//        String fullKpiCode = kpiPrefix + kpiCode;
//
//        log.debug("Calculando KPI {} para campaña {}", fullKpiCode, campaign.getCampaignId());
//
//        // Verificar si ya existe un KPI calculado para hoy
//        return findExistingKpi(campaign.getCampaignId(), fullKpiCode)
//                .flatMap(existingKpi -> {
//                    if (existingKpi != null) {
//                        log.debug("KPI existente encontrado, actualizando: {}", fullKpiCode);
//                        return updateExistingKpi(existingKpi, campaign, formatCode, kpiCode);
//                    } else {
//                        log.debug("Creando nuevo KPI: {}", fullKpiCode);
//                        return createNewKpi(campaign, formatCode, kpiPrefix, kpiCode);
//                    }
//                })
//                .onErrorResume(e -> {
//                    log.error("Error calculando KPI {}: {}", fullKpiCode, e.getMessage(), e);
//                    return Mono.empty(); // Se maneja el error y se devuelve Mono.empty()
//                });
//    }
//    /**
//     * Busca un KPI existente para la campaña y código de KPI dado
//     */
//    private Mono<Kpi> findExistingKpi(String campaignId, String kpiId) {
//        LocalDateTime today = LocalDateTime.now().withHour(0).withMinute(0).withSecond(0).withNano(0);
//        LocalDateTime tomorrow = today.plusDays(1);
//
//        return kpiRepository.findByCampaignIdAndKpiIdInAndCreatedDateBetween(
//                campaignId,
//                List.of(kpiId),
//                today,
//                tomorrow
//        ).next();
//    }
//
//    /**
//     * Actualiza un KPI existente
//     */
//    private Mono<Kpi> updateExistingKpi(Kpi existingKpi, Campaign campaign, String formatCode, String kpiCode) {
//        return calculateKpiValue(campaign, formatCode, kpiCode)
//                .flatMap(value -> {
//                    existingKpi.setValue(value);
//                    existingKpi.setUpdatedDate(LocalDateTime.now());
//                    return validateKpiValue(existingKpi, kpiCode, value)
//                            ? kpiRepository.save(existingKpi)
//                            : Mono.just(existingKpi);
//                });
//    }
//
//    /**
//     * Crea un nuevo KPI
//     */
//    private Mono<Kpi> createNewKpi(Campaign campaign, String formatCode, String kpiPrefix, String kpiCode) {
//        return calculateKpiValue(campaign, formatCode, kpiCode)
//                .flatMap(value -> {
//                    String fullKpiCode = kpiPrefix + kpiCode;
//
//                    Kpi newKpi = Kpi.builder()
//                            .campaignId(campaign.getCampaignId())
//                            .campaignSubId(campaign.getCampaignSubId())
//                            .kpiId(fullKpiCode)
//                            .kpiDescription(getKpiDescription(kpiCode))
//                            .type(fullKpiCode)
//                            .value(value)
//                            .status("ACTIVE")
//                            .createdUser("system")
//                            .createdDate(LocalDateTime.now())
//                            .updatedDate(LocalDateTime.now())
//                            .format(formatCode)
//                            .build();
//
//                    return validateKpiValue(newKpi, kpiCode, value)
//                            ? kpiRepository.save(newKpi)
//                            : Mono.just(newKpi);
//                });
//    }
//
//    /**
//     * Calcula el valor para un KPI específico basado en su código y los datos de la campaña
//     */
//    private Mono<Double> calculateKpiValue(Campaign campaign, String formatCode, String kpiCode) {
//        // Aquí iría la lógica para obtener los datos de BigQuery o de otras fuentes
//        // En este ejemplo, generamos valores simulados
//
//        // En un caso real, aquí implementarías las consultas específicas a BigQuery
//        // Utilizando la lógica definida en el archivo compartido (Lógica de indicadores.xlsx)
//
//        switch (kpiCode) {
//            case "INV":
//                // Inversión - Si existe en la campaña, usar ese valor, sino simular
//                return Mono.just(campaign.getInvestment() != null ?
//                        campaign.getInvestment().doubleValue() : simulateMetricValue(100, 5000));
//            case "ROA":
//                // ROAS - Simular un valor entre 0.5 y 10
//                return Mono.just(simulateMetricValue(0.5, 10));
//            case "ALC":
//                // Alcance - Simular un valor entre 1000 y 100000
//                return Mono.just(simulateMetricValue(1000, 100000));
//            case "IMP":
//                // Impresiones - Simular un valor entre 2000 y 500000
//                return Mono.just(simulateMetricValue(2000, 500000));
//            case "FRE":
//                // Frecuencia - Simular un valor entre 1 y 8
//                return Mono.just(simulateMetricValue(1, 8));
//            case "CLE":
//                // Clics en el enlace - Simular un valor entre 50 y 5000
//                return Mono.just(simulateMetricValue(50, 5000));
//            case "CPC":
//                // CPC - Simular un valor entre 0.1 y 10
//                return Mono.just(simulateMetricValue(0.1, 10));
//            case "CTR":
//                // CTR - Simular un valor entre 0.001 y 0.1 (0.1% - 10%)
//                return Mono.just(simulateMetricValue(0.001, 0.1));
//            case "VIP":
//                // Visitas a la página - Simular un valor entre 30 y 3000
//                return Mono.just(simulateMetricValue(30, 3000));
//            case "COV":
//                // Costo por Visita - Simular un valor entre 0.5 y 20
//                return Mono.just(simulateMetricValue(0.5, 20));
//            case "CPM":
//                // CPM - Simular un valor entre 1 y 30
//                return Mono.just(simulateMetricValue(1, 30));
//            case "COM":
//                // Compras - Simular un valor entre 5 y 500
//                return Mono.just(simulateMetricValue(5, 500));
//            case "CPA":
//                // CPA - Simular un valor entre 5 y 100
//                return Mono.just(simulateMetricValue(5, 100));
//            case "CON":
//                // Conversiones - Simular un valor entre 1 y 300
//                return Mono.just(simulateMetricValue(1, 300));
//            case "SES":
//                // Sesiones - Simular un valor entre 20 y 2000
//                return Mono.just(simulateMetricValue(20, 2000));
//            case "THR":
//                // ThruPlay (Solo para Video) - Simular un valor entre 100 y 10000
//                return Mono.just(simulateMetricValue(100, 10000));
//            case "CTH":
//                // Costo por ThruPlay - Simular un valor entre 0.05 y 5
//                return Mono.just(simulateMetricValue(0.05, 5));
//            case "IMS":
//                // Impression Share % - Simular un valor entre 0.1 y 0.8 (10% - 80%)
//                return Mono.just(simulateMetricValue(0.1, 0.8));
//            default:
//                log.warn("Código de KPI no reconocido: {}", kpiCode);
//                return Mono.just(0.0);
//        }
//    }
//
//    /**
//     * Simula un valor métrico dentro de un rango para propósitos de prueba
//     */
//    private double simulateMetricValue(double min, double max) {
//        return min + (max - min) * Math.random();
//    }
//
//    /**
//     * Obtiene la descripción para un código de KPI
//     */
//    private String getKpiDescription(String kpiCode) {
//        switch (kpiCode) {
//            case "INV":
//                return "Inversión";
//            case "ROA":
//                return "ROAS";
//            case "ALC":
//                return "Alcance";
//            case "IMP":
//                return "Impresiones";
//            case "FRE":
//                return "Frecuencia";
//            case "CLE":
//                return "Clics en el enlace";
//            case "CPC":
//                return "CPC";
//            case "CTR":
//                return "CTR";
//            case "VIP":
//                return "Visitas a la página";
//            case "COV":
//                return "Costo por Visita";
//            case "CPM":
//                return "CPM";
//            case "COM":
//                return "Compras";
//            case "CPA":
//                return "CPA";
//            case "CON":
//                return "Conversiones";
//            case "SES":
//                return "Sesiones";
//            case "THR":
//                return "ThruPlay";
//            case "CTH":
//                return "Costo por ThruPlay";
//            case "IMS":
//                return "Impression Share %";
//            default:
//                return kpiCode;  // Devuelve el código si no se encuentra en el switch
//        }
//    }
//
//    /**
//     * Valida si el valor de un KPI está dentro de los rangos aceptables
//     */
//    private boolean validateKpiValue(Kpi kpi, String kpiCode, double value) {
//        // Si el KPI tiene rangos de validación definidos
//        if (KPI_VALID_RANGES.containsKey(kpiCode)) {
//            double[] range = KPI_VALID_RANGES.get(kpiCode);
//            double min = range[0];
//            double max = range[1];
//
//            if (value < min || value > max) {
//                log.warn("Valor de KPI fuera de rango: {} = {} (rango: {} - {})",
//                        kpi.getKpiId(), value, min, max);
//                // No lo marcamos como error, pero lo registramos para revisión manual
//                return false;
//            }
//        }
//
//        // Validaciones de consistencia entre métricas (en un caso real se implementarían aquí)
//        // Por ejemplo, verificar que CTR = Clics / Impresiones
//
//        return true;
//    }
//
//    /**
//     * Verifica la consistencia entre métricas relacionadas
//     * Por ejemplo: CTR = Clics / Impresiones
//     */
//    private boolean validateMetricsConsistency(Map<String, Double> metrics, String formatCode) {
//        // Esta validación sería más completa en un escenario real
//        // Por ahora, solo verificamos algunos casos como ejemplo
//
//        String prefix = FORMAT_PREFIX_MAP.get(formatCode);
//        boolean isValid = true;
//
//        // Verificar CTR = Clics / Impresiones
//        if (metrics.containsKey(prefix + "CLE") && metrics.containsKey(prefix + "IMP") && metrics.containsKey(prefix + "CTR")) {
//            double clics = metrics.get(prefix + "CLE");
//            double impresiones = metrics.get(prefix + "IMP");
//            double ctr = metrics.get(prefix + "CTR");
//
//            // Permitir un pequeño margen de error (1%)
//            if (impresiones > 0) {
//                double calculatedCtr = clics / impresiones;
//                if (Math.abs(calculatedCtr - ctr) / ctr > 0.01) {
//                    log.warn("Inconsistencia en CTR: calculado={}, valor={}", calculatedCtr, ctr);
//                    isValid = false;
//                }
//            }
//        }
//
//        // Verificar CPC = Inversión / Clics
//        if (metrics.containsKey(prefix + "INV") && metrics.containsKey(prefix + "CLE") && metrics.containsKey(prefix + "CPC")) {
//            double inversion = metrics.get(prefix + "INV");
//            double clics = metrics.get(prefix + "CLE");
//            double cpc = metrics.get(prefix + "CPC");
//
//            if (clics > 0) {
//                double calculatedCpc = inversion / clics;
//                if (Math.abs(calculatedCpc - cpc) / cpc > 0.01) {
//                    log.warn("Inconsistencia en CPC: calculado={}, valor={}", calculatedCpc, cpc);
//                    isValid = false;
//                }
//            }
//        }
//
//        return isValid;
//    }
//}