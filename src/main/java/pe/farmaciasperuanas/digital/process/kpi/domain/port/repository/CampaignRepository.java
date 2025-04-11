package pe.farmaciasperuanas.digital.process.kpi.domain.port.repository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.Campaign;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.Date;
import java.util.List;

/**
 * Repositorio para acceder a la colección de campañas en MongoDB
 */
@Repository
public interface CampaignRepository extends ReactiveMongoRepository<Campaign, String> {

    /**
     * Busca campañas por su campaignId
     */
    Flux<Campaign> findByCampaignId(String campaignId);

    /**
     * Busca campañas por su providerId
     */
    Flux<Campaign> findByProviderId(String providerId);

    /**
     * Busca campañas por su campaignId y tipo
     */
    Flux<Campaign> findByCampaignIdAndType(String campaignId, String type);

    /**
     * Busca campañas que tengan un formato de medio específico
     */
    @Query("{ 'media.format': { $in: ?0 } }")
    Flux<Campaign> findByMediaFormatIn(List<String> formats);

    /**
     * Busca campañas por campaignId y que contengan un formato específico
     */
    @Query("{ 'campaignId': ?0, 'media.format': { $in: ?1 } }")
    Flux<Campaign> findByCampaignIdAndMediaFormatIn(String campaignId, List<String> formats);

    /**
     * Busca campañas por una lista de IDs
     */
    Flux<Campaign> findByCampaignIdIn(List<String> campaignIds);

    /**
     * Busca campañas activas dentro de un rango de fechas
     */
    @Query("{ 'providerId': ?0, 'startDate': { $lte: ?1 }, 'endDate': { $gte: ?2 } }")
    Flux<Campaign> findByProviderIdAndDateRange(String providerId, Date endDate, Date startDate);

    /**
     * Busca campañas por medio
     */
    @Query("{ 'media.medium': ?0 }")
    Flux<Campaign> findByMedium(String medium);

    /**
     * Busca campañas por estado
     */
    Flux<Campaign> findByStatus(String status);

    /**
     * Busca campañas por proveedor y estado
     */
    Flux<Campaign> findByProviderIdAndStatus(String providerId, String status);
}