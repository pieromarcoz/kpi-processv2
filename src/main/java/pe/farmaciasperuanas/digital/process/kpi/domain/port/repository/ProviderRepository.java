package pe.farmaciasperuanas.digital.process.kpi.domain.port.repository;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import pe.farmaciasperuanas.digital.process.kpi.domain.model.Provider;
import reactor.core.publisher.Mono;

/**
 * Repositorio para acceder a la colección de proveedores en MongoDB
 * Utilizado principalmente por ReferentialIntegrityValidator para verificar la existencia de proveedores
 */
@Repository
public interface ProviderRepository extends ReactiveMongoRepository<Provider, String> {

    /**
     * Busca un proveedor por su ID único
     *
     * @param providerId ID del proveedor a buscar
     * @return Mono que emite el proveedor si existe
     */
    Mono<Provider> findByProviderId(String providerId);

    /**
     * Verifica si existe un proveedor con el ID dado
     *
     * @param providerId ID del proveedor a verificar
     * @return Mono que emite true si existe, false en caso contrario
     */
    Mono<Boolean> existsByProviderId(String providerId);

    /**
     * Busca un proveedor por su nombre
     *
     * @param name Nombre del proveedor a buscar
     * @return Mono que emite el proveedor si existe
     */
    Mono<Provider> findByName(String name);

    /**
     * Busca un proveedor por su código SAP
     *
     * @param codSap Código SAP del proveedor
     * @return Mono que emite el proveedor si existe
     */
    Mono<Provider> findByCodSap(String codSap);
}