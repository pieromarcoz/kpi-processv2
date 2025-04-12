package pe.farmaciasperuanas.digital.process.kpi.infrastructure.config;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexInfo;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Configuración para crear índices en MongoDB
 * Mejora el rendimiento de las consultas utilizadas en los KPIs
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class MongoIndexConfig {

    private final ReactiveMongoTemplate mongoTemplate;

    /**
     * Estructura de índice con nombre y campos
     */
    private static class IndexDefinition {
        final String name;
        final Map<String, Sort.Direction> fields;

        IndexDefinition(String name, Map<String, Sort.Direction> fields) {
            this.name = name;
            this.fields = fields;
        }
    }

    @PostConstruct
    public void createIndexes() {
        log.info("Iniciando creación de índices para optimizar consultas KPI");

        // Índices para kpi
        Map<String, IndexDefinition> kpiIndexes = new HashMap<>();
        kpiIndexes.put("kpi_campaign_lookup_idx", new IndexDefinition(
                "kpi_campaign_lookup_idx",
                Map.of(
                        "campaignId", Sort.Direction.ASC,
                        "kpiId", Sort.Direction.ASC
                )
        ));
        kpiIndexes.put("kpi_date_lookup_idx", new IndexDefinition(
                "kpi_date_lookup_idx",
                Map.of(
                        "createdDate", Sort.Direction.ASC,
                        "campaignId", Sort.Direction.ASC
                )
        ));
        kpiIndexes.put("kpi_format_lookup_idx", new IndexDefinition(
                "kpi_format_lookup_idx",
                Map.of(
                        "format", Sort.Direction.ASC,
                        "campaignId", Sort.Direction.ASC
                )
        ));

        // Índices para colecciones de origen de datos
        Map<String, IndexDefinition> clicksIndexes = new HashMap<>();
        clicksIndexes.put("clicks_campaign_idx", new IndexDefinition(
                "clicks_campaign_idx",
                Map.of(
                        "campaignSubId", Sort.Direction.ASC,
                        "EventDate", Sort.Direction.DESC
                )
        ));
        clicksIndexes.put("clicks_sendid_idx", new IndexDefinition(
                "clicks_sendid_idx",
                Map.of("SendID", Sort.Direction.ASC)
        ));

        Map<String, IndexDefinition> opensIndexes = new HashMap<>();
        opensIndexes.put("opens_campaign_idx", new IndexDefinition(
                "opens_campaign_idx",
                Map.of("SendID", Sort.Direction.ASC)
        ));

        Map<String, IndexDefinition> pushIndexes = new HashMap<>();
        pushIndexes.put("push_campaign_idx", new IndexDefinition(
                "push_campaign_idx",
                Map.of(
                        "campaignId", Sort.Direction.ASC,
                        "DateTimeSend", Sort.Direction.DESC
                )
        ));

        Map<String, IndexDefinition> ga4Indexes = new HashMap<>();
        ga4Indexes.put("ga4_campaign_idx", new IndexDefinition(
                "ga4_campaign_idx",
                Map.of(
                        "campaignSubId", Sort.Direction.ASC,
                        "campaignId", Sort.Direction.ASC,
                        "date", Sort.Direction.DESC
                )
        ));

        // Crear todos los índices definidos
        createCollectionIndexes("kpi", kpiIndexes)
                .then(createCollectionIndexes("bq_ds_campanias_salesforce_clicks", clicksIndexes))
                .then(createCollectionIndexes("bq_ds_campanias_salesforce_opens", opensIndexes))
                .then(createCollectionIndexes("bq_ds_campanias_salesforce_push", pushIndexes))
                .then(createCollectionIndexes("ga4_own_media", ga4Indexes))
                .doOnSuccess(v -> log.info("Índices creados exitosamente"))
                .doOnError(e -> log.error("Error creando índices: {}", e.getMessage()))
                .subscribe();
    }

    /**
     * Crea índices para una colección específica
     */
    private Mono<Void> createCollectionIndexes(String collectionName, Map<String, IndexDefinition> indexes) {
        return mongoTemplate.indexOps(collectionName).getIndexInfo()
                .collectList()
                .flatMap(existingIndexes -> {
                    // Filtrar los índices que ya existen
                    Map<String, IndexDefinition> missingIndexes = new HashMap<>(indexes);
                    for (IndexInfo indexInfo : existingIndexes) {
                        missingIndexes.remove(indexInfo.getName());
                    }

                    if (missingIndexes.isEmpty()) {
                        log.info("Todos los índices ya existen para la colección: {}", collectionName);
                        return Mono.empty();
                    }

                    log.info("Creando {} índices para la colección: {}", missingIndexes.size(), collectionName);
                    return Mono.fromRunnable(() ->
                            missingIndexes.forEach((name, definition) -> {
                                Index index = new Index();
                                definition.fields.forEach(index::on);
                                index.named(name);

                                // Crear índice en segundo plano para no bloquear operaciones
                                index.background();

                                mongoTemplate.indexOps(collectionName)
                                        .ensureIndex(index)
                                        .doOnSuccess(indexName ->
                                                log.info("Índice creado: {} en colección: {}", indexName, collectionName))
                                        .doOnError(error ->
                                                log.error("Error creando índice {} en colección {}: {}",
                                                        name, collectionName, error.getMessage()))
                                        .subscribe();
                            })
                    );
                });
    }
}