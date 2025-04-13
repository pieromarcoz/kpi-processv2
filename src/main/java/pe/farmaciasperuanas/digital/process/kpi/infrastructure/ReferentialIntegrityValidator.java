package pe.farmaciasperuanas.digital.process.kpi.infrastructure;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.Campaign;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.CampaignRepository;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.ProviderRepository;
import reactor.core.publisher.Mono;

/**
 * Servicio para validar la integridad referencial antes de procesar KPIs
 * como se especifica en los requisitos de validación
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ReferentialIntegrityValidator {

    private final ReactiveMongoTemplate mongoTemplate;
    private final ProviderRepository providerRepository;
    private final CampaignRepository campaignRepository;

    /**
     * Verifica que el proveedor especificado exista
     * @param providerId ID del proveedor a verificar
     * @return Mono que emite true si existe, false en caso contrario
     */
    public Mono<Boolean> verifyProviderExists(String providerId) {
        if (providerId == null || providerId.isEmpty()) {
            log.warn("ID de proveedor nulo o vacío");
            return Mono.just(false);
        }

        return providerRepository.findByProviderId(providerId)
                .map(provider -> {
                    log.debug("Proveedor encontrado: {}", providerId);
                    return true;
                })
                .defaultIfEmpty(false)
                .doOnNext(exists -> {
                    if (!exists) {
                        log.warn("Proveedor no encontrado: {}", providerId);
                    }
                });
    }

    /**
     * Verifica que la campaña especificada exista
     * @param campaignId ID de la campaña a verificar
     * @return Mono que emite true si existe, false en caso contrario
     */
    public Mono<Boolean> verifyCampaignExists(String campaignId) {
        if (campaignId == null || campaignId.isEmpty()) {
            log.warn("ID de campaña nulo o vacío");
            return Mono.just(false);
        }

        return campaignRepository.findByCampaignId(campaignId)
                .next()
                .map(campaign -> {
                    log.debug("Campaña encontrada: {}", campaignId);
                    return true;
                })
                .defaultIfEmpty(false)
                .doOnNext(exists -> {
                    if (!exists) {
                        log.warn("Campaña no encontrada: {}", campaignId);
                    }
                });
    }

    /**
     * Verifica que la subcampaña especificada exista
     * @param campaignSubId ID de la subcampaña a verificar
     * @return Mono que emite true si existe, false en caso contrario
     */
    public Mono<Boolean> verifyCampaignSubIdExists(String campaignSubId) {
        if (campaignSubId == null || campaignSubId.isEmpty()) {
            log.warn("ID de subcampaña nulo o vacío");
            return Mono.just(false);
        }

        Query query = new Query(Criteria.where("campaignSubId").is(campaignSubId));
        return mongoTemplate.exists(query, Campaign.class)
                .doOnNext(exists -> {
                    if (exists) {
                        log.debug("Subcampaña encontrada: {}", campaignSubId);
                    } else {
                        log.warn("Subcampaña no encontrada: {}", campaignSubId);
                    }
                });
    }

    /**
     * Valida todas las referencias necesarias para un conjunto de KPIs
     * @param providerId ID del proveedor
     * @param campaignId ID de la campaña
     * @param campaignSubId ID de la subcampaña (opcional)
     * @return Mono que emite true si todas las referencias son válidas, false en caso contrario
     */
    public Mono<Boolean> validateAllReferences(String providerId, String campaignId, String campaignSubId) {
        return verifyProviderExists(providerId)
                .flatMap(providerExists -> {
                    if (!providerExists) {
                        return Mono.just(false);
                    }
                    return verifyCampaignExists(campaignId);
                })
                .flatMap(campaignExists -> {
                    if (!campaignExists) {
                        return Mono.just(false);
                    }

                    if (campaignSubId != null && !campaignSubId.isEmpty()) {
                        return verifyCampaignSubIdExists(campaignSubId);
                    }

                    return Mono.just(true);
                })
                .doOnNext(valid -> {
                    if (valid) {
                        log.info("Validación de integridad referencial exitosa para proveedor={}, campaña={}, subcampaña={}",
                                providerId, campaignId, campaignSubId);
                    } else {
                        log.error("Validación de integridad referencial fallida para proveedor={}, campaña={}, subcampaña={}",
                                providerId, campaignId, campaignSubId);
                    }
                });
    }

    /**
     * Verifica si una consulta SQL es segura (prevención de inyección SQL)
     * Nota: Esta es una implementación básica que debería adaptarse
     * según las necesidades específicas
     *
     * @param query Consulta SQL a validar
     * @return true si la consulta parece segura, false en caso contrario
     */
    public boolean isSqlQuerySafe(String query) {
        if (query == null || query.isEmpty()) {
            return false;
        }

        // Patrones peligrosos que podrían indicar un intento de inyección SQL
        String[] dangerousPatterns = {
                ";", "--", "/*", "*/", "xp_", "sp_", "exec ", "execute ",
                "drop ", "alter ", "truncate ", "delete from", "update "
        };

        // Verificar si la consulta contiene algún patrón peligroso
        for (String pattern : dangerousPatterns) {
            if (query.toLowerCase().contains(pattern.toLowerCase())) {
                log.warn("Consulta SQL potencialmente insegura detectada: {}", query);
                return false;
            }
        }

        return true;
    }
}