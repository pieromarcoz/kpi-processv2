package pe.farmaciasperuanas.digital.process.kpi.infrastructure.outbound.adapter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.MongoExpression;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
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

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implement class for running Spring Boot framework.<br/>
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
 * </ul>
 * @version 1.0
 */

@Repository
@Slf4j
@RequiredArgsConstructor
public class KpiRepositoryImpl implements KpiRepository{

    @Autowired
    private ReactiveMongoTemplate reactiveMongoTemplate;

    @Override
    public Flux<Kpi> generateKpiImpressionsParents() {
        String batchId = generateBatchId();
        Flux<Document> results  = this.prepareQueryParent("bq_ds_campanias_salesforce_opens");
        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId("MP-I");
            kpi.setKpiDescription("Impresiones (Aperturas)");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiShippingScopeParents() {
        Flux<Document> results  = this.prepareQueryParent("bq_ds_campanias_salesforce_sents");
        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId("MP-A");
            kpi.setKpiDescription("Alcance (Envíos)");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiClicksParents() {
        Flux<Document> results  = this.prepareQueryParent("bq_ds_campanias_salesforce_clicks");
        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId("MP-C");
            kpi.setKpiDescription("Clicks");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiImpressionsPushParents() {
        List<Document> pipeline = Arrays.asList(
                new Document("$lookup", new Document("from", "campaigns")
                        .append("localField", "campaignId")
                        .append("foreignField", "campaignId")
                        .append("as", "campaign")
                ),
                new Document("$match", new Document("campaign.format", "PA")),

                new Document("$match",
                        new Document("FechaProceso", new Document("$exists", true).append("$ne", null))
                                .append("DateTimeSend", new Document("$exists", true).append("$ne", null))
                                .append("OpenDate", new Document("$exists", true).append("$ne", null))
                                .append("Status", "Success")
                ),

                new Document("$match",
                        new Document("FechaProceso", new Document("$gte", new java.util.Date(new java.util.Date().getTime() - 31536000000L)))
                                .append("DateTimeSend", new Document("$gte", new java.util.Date(new java.util.Date().getTime() - 31536000000L)))
                ),

                new Document("$group",
                        new Document("_id", "$campaignId")
                                .append("value", new Document("$sum", 1))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id"))
                                .append("value", new Document("$toDouble", "$value"))
                )
        );

        return reactiveMongoTemplate.getCollection("bq_ds_campanias_salesforce_push")
                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class))
                .map(document -> {
                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(document.getString("campaignId"));
                    kpi.setCampaignSubId(document.getString("campaignId"));
                    kpi.setKpiId("PA-I");
                    kpi.setKpiDescription("Impresiones (Aperturas)");
                    kpi.setValue(document.getDouble("value"));
                    kpi.setType("cantidad");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setStatus("A");
                    return kpi;
                }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiShippingScopePushParents() {
        List<Document> pipeline = Arrays.asList(
                new Document("$lookup", new Document("from", "campaigns")
                        .append("localField", "campaignId")
                        .append("foreignField", "campaignId")
                        .append("as", "campaign")
                ),

                new Document("$match", new Document("campaign.format", "PA")),

                new Document("$match",
                        new Document("FechaProceso", new Document("$exists", true).append("$ne", null))
                                .append("DateTimeSend", new Document("$exists", true).append("$ne", null))
                                .append("Status", "Success")
                ),

                new Document("$match",
                        new Document("FechaProceso", new Document("$gte", new java.util.Date(new java.util.Date().getTime() - 31536000000L)))
                                .append("DateTimeSend", new Document("$gte", new java.util.Date(new java.util.Date().getTime() - 31536000000L)))
                ),

                new Document("$group",
                        new Document("_id", "$campaignId")
                                .append("value", new Document("$sum", 1))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id"))
                                .append("value", new Document("$toDouble", "$value"))
                )
        );

        return reactiveMongoTemplate.getCollection("bq_ds_campanias_salesforce_push")
                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class))
                .map(document -> {
                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(document.getString("campaignId"));
                    kpi.setCampaignSubId(document.getString("campaignId"));
                    kpi.setKpiId("PA-A");
                    kpi.setKpiDescription("Alcance (Envíos)");
                    kpi.setValue(document.getDouble("value"));
                    kpi.setType("cantidad");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setStatus("A");
                    return kpi;
                }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiSalesParents() {
        Flux<Document> results = prepareQueryGa4Parent ("$total_revenue");

        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId("MP-V");
            kpi.setKpiDescription("Venta - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi

    }

    @Override
    public Flux<Kpi> generateKpiTransactionsParents() {
        Flux<Document> results = prepareQueryGa4Parent ("$transactions");

        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId("MP-T");
            kpi.setKpiDescription("Transacciones - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiSessionsParents() {
        Flux<Document> results = prepareQueryGa4Parent ("$sessions");

        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId("MP-S");
            kpi.setKpiDescription("Sesiones - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiSalesPushParents() {
        Flux<Document> results = prepareQueryGa4PushParent ("$total_revenue");

        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId(document.getString("format") + "-V");
            kpi.setKpiDescription("Venta - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiTransactionsPushParents() {
        Flux<Document> results = prepareQueryGa4PushParent("$transactions");

        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId(document.getString("format") + "-T");
            kpi.setKpiDescription("Transacciones - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiSessionsPushParents() {
        Flux<Document> results = prepareQueryGa4PushParent("$sessions");

        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignId"));
            kpi.setKpiId(document.getString("format") + "-S");
            kpi.setKpiDescription("Sesiones - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiClicksByFormat() {
        List<Document> pipeline = Arrays.asList(
                new Document("$match", new Document("campaignSubId", new Document("$exists", true).append("$ne", null))),

                new Document("$lookup", new Document("from", "bq_ds_campanias_salesforce_sendjobs")
                        .append("localField", "SendID")
                        .append("foreignField", "SendID")
                        .append("as", "sendjobs")
                ),

                new Document("$unwind", "$sendjobs"),

                new Document("$lookup", new Document("from", "campaigns")
                        .append("localField", "campaignSubId")
                        .append("foreignField", "campaignSubId")
                        .append("as", "campaign")
                ),

                new Document("$unwind", "$campaign"),

                new Document("$match", new Document("$expr", new Document("$in", Arrays.asList(
                        new Document("$arrayElemAt", Arrays.asList("$campaign.format", 0)),
                        Arrays.asList("MC", "MF", "MB")
                )))),

                new Document("$group",
                        new Document("_id", new Document("campaignId", "$campaign.campaignId")
                                .append("campaignSubId", "$campaign.campaignSubId")
                                .append("format", new Document("$arrayElemAt", Arrays.asList("$campaign.format", 0))))
                                .append("value", new Document("$sum", 1))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id.campaignId"))
                                .append("campaignSubId", new Document("$toString", "$_id.campaignSubId"))
                                .append("format", "$_id.format")
                                .append("value", new Document("$toDouble", "$value"))
                )
        );

        return reactiveMongoTemplate.getCollection("bq_ds_campanias_salesforce_clicks")
                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class))
                .map(document -> {
                    Kpi kpi = new Kpi();
                    kpi.setCampaignId(document.getString("campaignId"));
                    kpi.setCampaignSubId(document.getString("campaignSubId"));
                    kpi.setKpiId(document.getString("format") + "C");
                    kpi.setKpiDescription("Clics");
                    kpi.setValue(document.getDouble("value"));
                    kpi.setType("cantidad");
                    kpi.setCreatedUser("-");
                    kpi.setCreatedDate(LocalDateTime.now());
                    kpi.setUpdatedDate(LocalDateTime.now());
                    kpi.setStatus("A");
                    return kpi;
                }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiSalesByFormat() {
        Flux<Document> results = prepareQueryGa4ByFormat("$total_revenue");
        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignSubId"));
            kpi.setKpiId(document.getString("format") + "V");
            kpi.setKpiDescription("Venta - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiTransactionsByFormat() {
        Flux<Document> results = prepareQueryGa4ByFormat("$transactions");
        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignSubId"));
            kpi.setKpiId(document.getString("format") + "T");
            kpi.setKpiDescription("Transacciones - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiSessionsByFormat() {
        Flux<Document> results = prepareQueryGa4ByFormat("$sessions");
        return results.map(document -> {
            Kpi kpi = new Kpi();
            kpi.setCampaignId(document.getString("campaignId"));
            kpi.setCampaignSubId(document.getString("campaignSubId"));
            kpi.setKpiId(document.getString("format") + "S");
            kpi.setKpiDescription("Sesiones - GA4");
            kpi.setValue(document.getDouble("value"));
            kpi.setType("cantidad");
            kpi.setCreatedUser("-");
            kpi.setCreatedDate(LocalDateTime.now());
            kpi.setUpdatedDate(LocalDateTime.now());
            kpi.setStatus("A");
            return kpi;
        }).flatMap(this::saveOrUpdateKpiStartOfDay);//saveOrUpdateKpi
    }

    @Override
    public Flux<Kpi> generateKpiOpenRateParents() {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("kpiId").in("MP-I", "MP-A")),
                Aggregation.group("campaignId")
                        .sum(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                             $cond: [
                                { $eq: ["$kpiId", "MP-I"] },
                                "$value",
                                0.0
                            ]
                        """)
                                )
                        ).as("sum_MP_I")
                        .sum(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                             $cond: [
                                { $eq: ["$kpiId", "MP-A"] },
                                "$value",
                                0.0
                            ]
                        """)
                                )
                        ).as("sum_MP_A"),
                Aggregation.addFields()
                        .addField("value")
                        .withValue(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                             $cond: {

                                     if: { $eq: [{ $ifNull: ["$sum_MP_A", 0] }, 0] },
                                         then: 0.0,
                                   else: {
                                      $divide: [
                                                { $ifNull: ["$sum_MP_I", 0] },
                                                { $ifNull: ["$sum_MP_A", 0] }
                                               ]
                                          }

                            }
                        """)
                                )
                        )
                        .build(),
                Aggregation.project()
                        .andExclude("_id")
                        .and("$_id").as("campaignId")
                        .andInclude("value")
        );
        return reactiveMongoTemplate.aggregate(aggregation, "kpi", Kpi.class)
                .flatMap(c -> {
                    c.setKpiId("MP-OR");
                    c.setKpiDescription("Open Rate (OR)");
                    c.setType("porcentaje");
                    c.setCampaignSubId(c.getCampaignId().toString());
                    return this.upsertKpi(c);
                });
    }

    @Override
    public Flux<Kpi> generateKpiCRParents() {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("kpiId").in("MP-C", "MP-I")),
                Aggregation.group("campaignId")
                        .sum(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                             $cond: [
                                { $eq: ["$kpiId", "MP-C"] },
                                "$value",
                                0.0
                            ]
                        """)
                                )
                        ).as("sum_MP_C")
                        .sum(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                            $cond: [
                                { $eq: ["$kpiId", "MP-I"] },
                                "$value",
                                0.0
                            ]
                        """)
                                )
                        ).as("sum_MP_I"),
                Aggregation.addFields()
                        .addField("value")
                        .withValue(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                            $cond: {

                                     if: { $eq: [{ $ifNull: ["$sum_MP_I", 0] }, 0] },
                                         then: 0.0,
                                   else: {
                                      $divide: [
                                                { $ifNull: ["$sum_MP_C", 0.0] },
                                                { $ifNull: ["$sum_MP_I", 0.0] }
                                               ]
                                          }

                            }
                        """)
                                )
                        )
                        .build(),
                Aggregation.project()
                        .andExclude("_id")
                        .and("$_id").as("campaignId")
                        .andInclude("value")
        );
        return reactiveMongoTemplate.aggregate(aggregation, "kpi", Kpi.class)
                .flatMap(c -> {
                    c.setKpiId("MP-CR");
                    c.setKpiDescription("CTR (CR)");
                    c.setType("porcentaje");
                    c.setCampaignSubId(c.getCampaignId().toString());
                    return this.upsertKpi(c);
                });
    }

    @Override
    public Flux<Kpi> generateKpiClickRateByFormat() {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("kpiId").in("MCC", "MFC", "MBC")),
                Aggregation.lookup()
                        .from("kpi")
                        .localField("campaignId")
                        .foreignField("campaignId")
                        .pipeline(
                                Aggregation.match(Criteria.where("kpiId").is("MP-A")),
                                Aggregation.group("campaignId")
                                        .sum("value").as("totalMPA")
                        )
                        .as("b"),
                Aggregation.unwind("b", true),
                Aggregation.group(
                                Fields.fields("campaignId", "campaignSubId", "format", "sumMPA")
                                        .and("campaignId", "$campaignId")
                                        .and("campaignSubId", "$campaignSubId")
                                        .and("format", "$kpiId")
                                        .and("sumMPA", "$b.totalMPA")
                        )
                        .sum("value").as("sumValue"),
                Aggregation.addFields()
                        .addField("sumMPA")
                        .withValue(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                             $ifNull: ["$_id.sumMPA", 0.0] 
                        """)
                                )
                        )
                        .addField("value")
                        .withValue(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                             $cond: {

                                     if: { $eq: [{ $ifNull: ["$_id.sumMPA", 0] }, 0] },
                                         then: 0.0,
                                   else: {
                                      $divide: [
                                                { $ifNull: ["$sumValue", 0.0] },
                                                { $ifNull: ["$_id.sumMPA", 0.0] }
                                               ]
                                          }

                            }
                        """)
                                )
                        )
                        .build(),
                Aggregation.project()
                        .andExclude("_id")
                        .and("$_id.campaignId").as("campaignId")
                        .and("$_id.campaignSubId").as("campaignSubId")
                        .and("$_id.format").as("format")
                        .andInclude("value")
        );

        return reactiveMongoTemplate.aggregate(aggregation, "kpi", Kpi.class)
                .flatMap(c -> {
                        try {
                    c.setKpiId(c.getFormat() + "R");
                    c.setKpiDescription("Click Rate");
                    c.setType("porcentaje");
                    return this.upsertKpi(c);
                } catch (Exception e) {
                        // Manejo de la excepción
                        log.error("Error procesando el KPI: " + c, e);
                        // Dependiendo de tus necesidades, puedes devolver un Mono vacío, un valor por defecto, etc.
                        return Mono.error(new RuntimeException("Error al procesar el KPI", e));
                    }
                });
    }

    @Override
    public Flux<Kpi> generateKpiOpenRatePushParents() {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("kpiId").in("PA-I", "PA-A")),

                Aggregation.group("campaignId")
                        .sum(
                                AggregationExpression.from(
                                     MongoExpression.create("""                         
                                         $cond: [
                                                { $eq: ["$kpiId", "PA-I"] },
                                                "$value",
                                                0.0
                                          ]
                        """)
                        )
                        ).as("sumPAI")
                        .sum(
                                AggregationExpression.from(
                                        MongoExpression.create("""
                                         $cond: [
                                                  { $eq: ["$kpiId", "PA-A"] },
                                                  "$value",
                                                  0.0
                                                ]
                        """)
                        )
                        ).as("sumPAA"),
                Aggregation.addFields()
                        .addField("value")
                        .withValue(
                                AggregationExpression.from(                                   
                                        MongoExpression.create("""                                       
                                        $cond: {

                                                 if: { $eq: [{ $ifNull: ["$sumPAA", 0] }, 0] },
                                                then: 0.0,
                                                else: {
                                                        $divide: [
                                                                  { $ifNull: ["$sumPAI", 0.0] },
                                                                  { $ifNull: ["$sumPAA", 0.0] }
                                                                 ]
                                                      }

                                                }                                       
                                   """)
                                )
                        )
                        .build(),

                Aggregation.project()
                        .andExclude("_id")
                        .and("$_id").as("campaignId")
                        .andInclude("value")
        );

        return reactiveMongoTemplate.aggregate(aggregation, "kpi", Kpi.class)
                .flatMap(c -> {
                    c.setKpiId("PA-OR");
                    c.setKpiDescription("Open Rate (OR)");
                    c.setType("porcentaje");
                    c.setCampaignSubId(c.getCampaignId().toString());
                    return this.upsertKpi(c);
                });
    }

    @Override
    public Flux<Kpi> generateKpiRoasGeneral() {
        List<String> kpiIds = Arrays.asList("MP-V", "MCV", "MFV", "MBV", "PA-V", "PW-V");

        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("kpiId").in(kpiIds)),

                Aggregation.lookup("campaigns", "campaignId", "campaignId", "campaign"),

                Aggregation.unwind("campaign", true),

                Aggregation.group(
                                Fields.fields("campaignId", "campaignSubId", "kpiId", "campaign.investment")
                        )
                        .sum("value").as("sumValue"),

                Aggregation.addFields()
                        .addField("investment")
                        .withValue(ConditionalOperators.ifNull("$_id.investment").then(0))
                        .addField("value")
                        .withValue(ConditionalOperators.when(Criteria.where("_id.investment").is(0))
                                .then(0)
                                .otherwise(ArithmeticOperators.Divide.valueOf("$sumValue").divideBy("$_id.investment"))
                        ).build(),
                Aggregation.project()
                        .andExclude("_id")
                        .and("$_id.campaignId").as("campaignId")
                        .and("$_id.campaignSubId").as("campaignSubId")
                        .and("$_id.kpiId").as("format")
                        .and("$value").as("value")
        );


        return reactiveMongoTemplate.aggregate(aggregation, "kpi", Kpi.class)
                .flatMap(c -> {
                    c.setKpiId(c.getFormat() + "RA");
                    c.setKpiDescription("ROAS");
                    c.setType("porcentaje");
                    c.setCampaignSubId(c.getCampaignId().toString());
                    return this.upsertKpi(c);
                });
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
                    Query campaignsQuery = Query.query(Criteria.where("providerId").is(providerId));

                    return reactiveMongoTemplate.find(campaignsQuery, Campaign.class)
                            .collectList()
                            .flatMap(campaigns -> {
                                if (campaigns.isEmpty()) {
                                    System.out.println("No se encontraron campañas para el proveedor: " + providerId);
                                    return createEmptyMetrics(providerId);
                                }

                                // Extraemos los campaignId de las campañas encontradas
                                List<String> campaignIds = campaigns.stream()
                                        .map(Campaign::getCampaignId)
                                        .filter(id -> id != null && !id.isBlank())
                                        .collect(Collectors.toList());

                                System.out.println("Proveedor: " + providerId + " | Campañas encontradas: " +
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

    private Mono<Kpi> saveOrUpdateKpi(Kpi kpi) {
        // No guardar KPIs con valor 0
        if (kpi.getValue() != null && kpi.getValue() == 0) {
            return Mono.empty();
        }
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
                .set("updatedDate", kpi.getUpdatedDate());

        return reactiveMongoTemplate.upsert(query, update, Kpi.class)
                .thenReturn(kpi);
    }

private Mono<Kpi> saveOrUpdateKpiStartOfDay(Kpi kpi) {
    // No guardar KPIs con valor 0
    if (kpi.getValue() != null && kpi.getValue() == 0) {
        return Mono.empty();
    }
    // Obtenemos la fecha actual sin la parte de la hora para comparar solo la fecha
    LocalDate currentDate = LocalDate.now();
    
    // Consulta para encontrar el documento con el mismo kpiId, campaignId y campaignSubId
    Query query = new Query()
            .addCriteria(Criteria.where("kpiId").is(kpi.getKpiId())
                    .and("campaignId").is(kpi.getCampaignId())
                    .and("campaignSubId").is(kpi.getCampaignSubId())
                    .and("createdDate").gte(currentDate.atStartOfDay()) // Filtrar por fecha actual (sin la parte de hora)
                    .lt(currentDate.plusDays(1).atStartOfDay())); // Filtrar por el final del día actual (hasta las 23:59)

    // Si el registro ya existe para hoy, se actualiza. Si no, se inserta un nuevo registro.
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
            .set("updatedDate", LocalDateTime.now()); // Asigna la fecha y hora actuales a updatedDate

    // Usamos upsert para que se actualice si existe o se cree un nuevo documento si no existe
    return reactiveMongoTemplate.upsert(query, update, Kpi.class)
            .thenReturn(kpi);
}

    private Mono<Kpi> upsertKpi(Kpi kpi) {
        Query query = Query.query(Criteria.where("kpiId").is(kpi.getKpiId())
                .and("campaignId").is(kpi.getCampaignId())
                .and("campaignSubId").is(kpi.getCampaignSubId()));

        kpi.setStatus("A");
        kpi.setCreatedUser("-");
        LocalDateTime now = LocalDateTime.now();
        kpi.setCreatedDate(now);
        kpi.setUpdatedDate(now);

        Update update = new Update()
                .set("kpiDescription", kpi.getKpiDescription())
                .set("type", kpi.getType())
                .set("value", kpi.getValue())
                .set("status", kpi.getStatus())
                .set("createdUser", kpi.getCreatedUser())
                .set("createdDate", kpi.getCreatedDate())
                .set("updatedDate", kpi.getUpdatedDate());

        return reactiveMongoTemplate.update(Kpi.class)
                .matching(query)
                .apply(update)
                .withOptions(FindAndModifyOptions.options().upsert(true).returnNew(true))
                .findAndModify();
    }

    private Flux<Document> prepareQueryParent(String collectionParam) {
        List<Document> pipeline = Arrays.asList(
                new Document("$lookup", new Document("from", "bq_ds_campanias_salesforce_sendjobs")
                        .append("localField", "SendID")
                        .append("foreignField", "SendID")
                        .append("as", "sendjobs")
                ),

                new Document("$unwind", "$sendjobs"),

                new Document("$match",
                        new Document("sendjobs.campaignId", new Document("$exists", true).append("$ne", null))
                ),

                new Document("$lookup", new Document("from", "campaigns")
                        .append("localField", "sendjobs.campaignId")
                        .append("foreignField", "campaignId")
                        .append("as", "campaign")
                ),

                new Document("$match",
                        new Document("campaign.campaignId", new Document("$exists", true)
                                .append("$ne", null)
                                .append("$ne", "")
                        )
                ),

                new Document("$group",
                        new Document("_id", "$sendjobs.campaignId")
                                .append("value", new Document("$sum", 1))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id"))
                                .append("value", new Document("$toDouble", "$value"))
                )
        );

        return reactiveMongoTemplate.getCollection(collectionParam)
                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class));
    }

    private Flux<Document> prepareQueryGa4Parent(String fieldSum) {
        List<Document> pipeline = Arrays.asList(
                new Document("$match", new Document("campaignSubId", new Document("$exists", true).append("$ne", null))),

                new Document("$lookup", new Document("from", "campaigns")
                        .append("localField", "campaignSubId")
                        .append("foreignField", "campaignSubId")
                        .append("as", "campaign")
                ),

                new Document("$unwind", "$campaign"),

                new Document("$match", new Document("campaign.format", new Document("$in", Arrays.asList("MC", "MF", "MB")))),

                new Document("$group",
                        new Document("_id", "$campaign.campaignId")
                                .append("value", new Document("$sum", fieldSum))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id"))
                                .append("value", new Document("$toDouble", "$value"))
                )
        );

        return reactiveMongoTemplate.getCollection("ga4_own_media")
                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class));
    }

    private Flux<Document> prepareQueryGa4PushParent(String fieldSum) {
        List<Document> pipeline = Arrays.asList(
                new Document("$match", new Document("campaignId", new Document("$exists", true).append("$ne", null))),

                new Document("$lookup", new Document("from", "campaigns")
                        .append("localField", "campaignId")
                        .append("foreignField", "campaignId")
                        .append("as", "campaign")
                ),

                new Document("$unwind", "$campaign"),

                new Document("$match", new Document("$expr", new Document("$in", Arrays.asList(
                        new Document("$arrayElemAt", Arrays.asList("$campaign.format", 0)),
                        Arrays.asList("PA", "PW")
                )))),

                new Document("$group",
                        new Document("_id", new Document("campaignId", "$campaign.campaignId")
                                .append("format", new Document("$arrayElemAt", Arrays.asList("$campaign.format", 0))))
                                .append("value", new Document("$sum", fieldSum))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id.campaignId"))
                                .append("format", "$_id.format")
                                .append("value", new Document("$toDouble", "$value"))
                )
        );

        return reactiveMongoTemplate.getCollection("ga4_own_media")
                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class));
    }

    private  Flux<Document> prepareQueryGa4ByFormat (String fieldSum) {

        List<Document> pipeline = Arrays.asList(
                new Document("$match", new Document("campaignSubId", new Document("$exists", true).append("$ne", null))),

                new Document("$lookup", new Document("from", "campaigns")
                        .append("localField", "campaignSubId")
                        .append("foreignField", "campaignSubId")
                        .append("as", "campaign")
                ),

                new Document("$unwind", "$campaign"),

                new Document("$match", new Document("$expr", new Document("$in", Arrays.asList(
                        new Document("$arrayElemAt", Arrays.asList("$campaign.format", 0)),
                        Arrays.asList("MC", "MF", "MB")
                )))),

                new Document("$group",
                        new Document("_id", new Document("campaignId", "$campaign.campaignId")
                                .append("campaignSubId", "$campaign.campaignSubId")
                                .append("format", new Document("$arrayElemAt", Arrays.asList("$campaign.format", 0))))
                                .append("value", new Document("$sum", fieldSum))
                ),

                new Document("$project",
                        new Document("_id", 0)
                                .append("campaignId", new Document("$toString", "$_id.campaignId"))
                                .append("campaignSubId", new Document("$toString", "$_id.campaignSubId"))
                                .append("format", "$_id.format")
                                .append("value", new Document("$toDouble", "$value"))
                )
        );

        return reactiveMongoTemplate.getCollection("ga4_own_media")
                .flatMapMany(collection -> collection.aggregate(pipeline, Document.class));
    }
}
    // Método para generar un ID de lote único basado en timestamp
    private String generateBatchId() {
        return "BATCH-" + System.currentTimeMillis();
    }

    // Método para establecer los campos batchId y mediaType en los KPIs de medios propios
    private Kpi setOwnedMediaBatchFields(Kpi kpi, String batchId) {
        kpi.setBatchId(batchId);
        kpi.setMediaType("OWNED");
        return kpi;
    }
