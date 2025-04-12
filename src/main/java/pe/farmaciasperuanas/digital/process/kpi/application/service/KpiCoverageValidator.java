package pe.farmaciasperuanas.digital.process.kpi.application.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.Campaign;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.CampaignMedium;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Servicio para validar y verificar la cobertura de KPIs en las campañas.
 * Detecta campañas con KPIs faltantes y realiza validaciones de integridad.
 *
 * <b>Copyright</b>: &copy; 2025 Digital.<br/>
 * <b>Company</b>: Digital.<br/>
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class KpiCoverageValidator {

    private final ReactiveMongoTemplate mongoTemplate;
    private final KpiServiceImpl kpiService;

    // Listas de todos los KPIs esperados por tipo de medio
    private static final Map<String, List<String>> EXPECTED_KPIS_BY_FORMAT = Map.of(
            // Mail y formatos relacionados
            "MP", Arrays.asList("MP-I", "MP-A", "MP-C", "MP-V", "MP-T", "MP-S", "MP-OR", "MP-CR"),
            "MC", Arrays.asList("MCC", "MCCR", "MCVRA"),
            "MB", Arrays.asList("MBC", "MBCR", "MBVRA"),
            "MF", Arrays.asList("MFC", "MFCR", "MFVRA"),

            // Push y formatos relacionados
            "PA", Arrays.asList("PA-I", "PA-A", "PA-V", "PA-T", "PA-S", "PA-OR"),
            "PW", Arrays.asList("PW-V", "PW-T", "PW-S", "PW-OR")
    );

    /**
     * Verifica diariamente la cobertura de KPIs para todas las campañas activas.
     * Programado para ejecutarse a las 5:00 AM
     */
    @Scheduled(cron = "0 0 5 * * ?")
    public void scheduledKpiCoverageCheck() {
        log.info("Iniciando verificación programada de cobertura de KPIs");
        verifyKpiCoverage()
                .doOnSuccess(v -> log.info("Verificación programada de cobertura de KPIs completada"))
                .doOnError(e -> log.error("Error en verificación de cobertura de KPIs: {}", e.getMessage(), e))
                .subscribe();
    }

    /**
     * Verifica la cobertura de KPIs para todas las campañas activas.
     * Identifica KPIs faltantes y registra advertencias.
     *
     * @return Mono que completa cuando termina la verificación
     */
    public Mono<Void> verifyKpiCoverage() {
        log.info("Iniciando verificación de cobertura de KPIs");
        LocalDate today = LocalDate.now();
        LocalDateTime startOfDay = today.atStartOfDay();
        LocalDateTime endOfDay = today.atTime(23, 59, 59, 999999999);

        // Buscar campañas activas
        return mongoTemplate.find(
                        Query.query(Criteria.where("status").in("Programado", "En proceso")),
                        Campaign.class
                )
                .filter(this::hasOwnedMedia)
                .flatMap(campaign -> {
                    String campaignId = campaign.getCampaignId();
                    log.debug("Verificando cobertura de KPIs para campaña: {}", campaignId);

                    // Determinar formatos de la campaña
                    Set<String> campaignFormats = getCampaignFormats(campaign);

                    // Si no hay formatos aplicables, omitir esta campaña
                    if (campaignFormats.isEmpty()) {
                        return Mono.empty();
                    }

                    // Determinar todos los KPIs esperados para estos formatos
                    Set<String> expectedKpis = new HashSet<>();
                    for (String format : campaignFormats) {
                        List<String> formatKpis = EXPECTED_KPIS_BY_FORMAT.get(format);
                        if (formatKpis != null) {
                            expectedKpis.addAll(formatKpis);
                        }
                    }

                    // Buscar KPIs existentes para esta campaña hoy
                    return mongoTemplate.find(
                                    Query.query(Criteria.where("campaignId").is(campaignId)
                                            .and("createdDate").gte(startOfDay).lte(endOfDay)),
                                    Kpi.class
                            )
                            .collectList()
                            .flatMap(existingKpis -> {
                                // Extraer códigos KPI existentes
                                Set<String> existingKpiCodes = existingKpis.stream()
                                        .map(Kpi::getKpiId)
                                        .collect(Collectors.toSet());

                                // Determinar KPIs faltantes
                                Set<String> missingKpis = new HashSet<>(expectedKpis);
                                missingKpis.removeAll(existingKpiCodes);

                                if (!missingKpis.isEmpty()) {
                                    log.warn("Campaña {} con ID {} falta generar los siguientes KPIs: {}",
                                            campaign.getName(), campaignId, String.join(", ", missingKpis));

                                    // Verificar si faltan KPIs críticos (OR, CR, ROAS)
                                    boolean missingCriticalKpis = missingKpis.stream()
                                            .anyMatch(kpi -> kpi.endsWith("OR") || kpi.endsWith("CR") || kpi.endsWith("RA"));

                                    if (missingCriticalKpis) {
                                        log.error("¡ALERTA! Campaña {} con ID {} tiene KPIs críticos faltantes",
                                                campaign.getName(), campaignId);

                                        // Auto-regenerar KPIs críticos si faltan
                                        return regenerateMissingKpis(campaign, missingKpis);
                                    }
                                } else {
                                    log.debug("Campaña {} con ID {} tiene todos los KPIs esperados",
                                            campaign.getName(), campaignId);
                                }

                                return Mono.empty();
                            });
                })
                .then();
    }

    /**
     * Regenera KPIs faltantes para una campaña.
     *
     * @param campaign La campaña con KPIs faltantes
     * @param missingKpis Conjunto de KPIs faltantes
     * @return Mono que completa cuando se regeneran los KPIs
     */
    private Mono<Void> regenerateMissingKpis(Campaign campaign, Set<String> missingKpis) {
        log.info("Intentando regenerar KPIs faltantes para campaña: {}", campaign.getCampaignId());

        // Simplemente usamos el método principal de generación de KPIs
        // que internamente ejecuta todos los cálculos necesarios
        return kpiService.generateKpiFromSalesforceData()
                .doOnSuccess(result -> log.info("KPIs regenerados para campaña: {} - Resultado: {}",
                        campaign.getCampaignId(), result.get("message")))
                .doOnError(e -> log.error("Error al regenerar KPIs para campaña {}: {}",
                        campaign.getCampaignId(), e.getMessage()))
                .then();
    }

    /**
     * Verifica si una campaña tiene medios propios configurados
     */
    private boolean hasOwnedMedia(Campaign campaign) {
        if (campaign.getMedia() == null || campaign.getMedia().isEmpty()) {
            return false;
        }

        return campaign.getMedia().stream()
                .anyMatch(media -> "Medios propios".equals(media.getMedium()));
    }

    /**
     * Obtiene los formatos de medios propios de una campaña
     */
    private Set<String> getCampaignFormats(Campaign campaign) {
        Set<String> formats = new HashSet<>();

        // Siempre incluimos MP para cualquier campaña de medios propios
        formats.add("MP");

        if (campaign.getMedia() != null) {
            for (CampaignMedium medium : campaign.getMedia()) {
                if ("Medios propios".equals(medium.getMedium()) && medium.getFormat() != null) {
                    // Añadir el formato específico si está en nuestra lista de formatos esperados
                    String format = medium.getFormat();
                    if (EXPECTED_KPIS_BY_FORMAT.containsKey(format)) {
                        formats.add(format);
                    }
                }
            }
        }

        return formats;
    }

    /**
     * Verifica valores nulos o cero en KPIs y registra posibles problemas
     * Este método se puede invocar manualmente para diagnóstico
     */
    public Mono<Map<String, Integer>> checkForEmptyValues() {
        log.info("Verificando valores nulos o cero en KPIs");

        Map<String, Integer> results = new HashMap<>();
        results.put("nullValues", 0);
        results.put("zeroValues", 0);
        results.put("missingBatchId", 0);
        results.put("missingMediaType", 0);

        LocalDate today = LocalDate.now();
        LocalDateTime startOfDay = today.atStartOfDay();
        LocalDateTime endOfDay = today.atTime(23, 59, 59, 999999999);

        return mongoTemplate.find(
                        Query.query(Criteria.where("createdDate").gte(startOfDay).lte(endOfDay)),
                        Kpi.class
                )
                .doOnNext(kpi -> {
                    if (kpi.getValue() == null) {
                        results.put("nullValues", results.get("nullValues") + 1);
                        log.warn("KPI con valor NULL: {} para campaña {}", kpi.getKpiId(), kpi.getCampaignId());
                    } else if (kpi.getValue() == 0) {
                        results.put("zeroValues", results.get("zeroValues") + 1);
                        log.debug("KPI con valor cero: {} para campaña {}", kpi.getKpiId(), kpi.getCampaignId());
                    }

                    if (kpi.getBatchId() == null || kpi.getBatchId().isEmpty()) {
                        results.put("missingBatchId", results.get("missingBatchId") + 1);
                        log.warn("KPI sin batchId: {} para campaña {}", kpi.getKpiId(), kpi.getCampaignId());
                    }

                    if (kpi.getMediaType() == null || kpi.getMediaType().isEmpty()) {
                        results.put("missingMediaType", results.get("missingMediaType") + 1);
                        log.warn("KPI sin mediaType: {} para campaña {}", kpi.getKpiId(), kpi.getCampaignId());
                    }
                })
                .then(Mono.just(results));
    }
}