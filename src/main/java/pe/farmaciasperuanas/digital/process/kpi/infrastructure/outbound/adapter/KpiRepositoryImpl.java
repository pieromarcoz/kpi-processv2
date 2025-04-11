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
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.KpiRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

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

    @Override
    public Flux<Metrics> generateMetricsGeneral() {


        Flux<Document> providerResults = getProviderResults();
        return providerResults.flatMap(document -> {
            Metrics metrics = new Metrics();
            //metrics.setProviderId(document.getObjectId("providerId"));
            //metrics.setProviderId(document.getObjectId("providerId").toString());
            metrics.setProviderId(document.getString("providerId"));
            metrics.setTotalActiveCampaigns(document.getInteger("totalActiveCampaigns"));
            metrics.setTotalInvestmentPeriod(document.getDouble("totalInvestmentPeriod"));
            metrics.setTotalSales(document.getDouble("totalSales"));
            metrics.setCreatedUser("-");
            metrics.setCreatedDate(LocalDateTime.now());
            metrics.setUpdatedDate(LocalDateTime.now());

            return reactiveMongoTemplate.save(metrics);
        });
    }

    private Flux<Document> getProviderResults() {

        List<Document> pipeline = Arrays.asList(
                new Document("$lookup",
                        new Document("from", "campaigns")
                                .append("localField", "providerId")
                                .append("foreignField", "providerId")
                                .append("as", "activeCampaigns")
                ),
                new Document("$addFields",
                        new Document("totalActiveCampaigns",
                                new Document("$size", "$activeCampaigns")
                        )
                ),
                new Document("$lookup",
                        new Document("from", "campaigns")
                                .append("let",
                                        new Document("providerId", "$providerId")
                                )
                                .append("pipeline", Arrays.asList(
                                        new Document("$match",
                                                new Document("$expr",
                                                        new Document("$and", Arrays.asList(
                                                                //new Document("$eq", Arrays.asList("$providerId", "$$providerId")),
                                                                // Convertir ambos a String para la comparación
                                                                new Document("$eq", Arrays.asList(
                                                                                new Document("$toString", "$providerId"),  // Convertir providerId de campaña a String
                                                                                new Document("$toString", "$$providerId")  // Convertir el providerId de la variable let a String
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
                new Document("$lookup",
                        new Document("from", "kpi")
                                .append("let",
                                        new Document("providerId", "$providerId")
                                )
                                .append("pipeline", Arrays.asList(
                                        new Document("$lookup",
                                                new Document("from", "campaigns")
                                                        .append("localField", "campaignId")
                                                        .append("foreignField", "campaignId")
                                                        .append("as", "campaign")
                                        ),
                                        new Document("$unwind", "$campaign"),
                                        new Document("$match",
                                                new Document("$expr",
                                                        new Document("$and", Arrays.asList(
                                                                new Document("$lte", Arrays.asList("$campaign.startDate", new Date())),
                                                                new Document("$gte", Arrays.asList("$campaign.endDate", new Date())),
                                                                //new Document("$eq", Arrays.asList("$campaign.providerId", "$$providerId")),
                                                                // Convertir a String ambos providerId
                                                                new Document("$eq", Arrays.asList(
                                                                              new Document("$toString", "$campaign.providerId"), // Convertir a String
                                                                              new Document("$toString", "$$providerId")         // Convertir a String
                                                                )),
                                                                new Document("$in", Arrays.asList("$kpiId", Arrays.asList("MP-V", "PW-V", "PA-V")))
                                                        ))
                                                )
                                        ),
                                        new Document("$group",
                                                new Document("_id", "$campaign.providerId")
                                                        .append("totalSales", new Document("$sum", "$value"))
                                        )
                                ))
                                .append("as", "salesData")
                ),
                new Document("$addFields",
                        new Document("totalInvestmentPeriod",
                                new Document("$ifNull", Arrays.asList(
                                        new Document("$arrayElemAt", Arrays.asList("$investmentData.totalInvestmentPeriod", 0)), 0))
                        )
                                .append("totalSales",
                                        new Document("$ifNull", Arrays.asList(
                                                new Document("$arrayElemAt", Arrays.asList("$salesData.totalSales", 0)), 0))
                                )
                ),
                new Document("$group",
                        //new Document("_id", "$providerId")
                        new Document("_id", new Document("$toString", "$providerId"))  // Convierte providerId a String para _id
                                .append("totalActiveCampaigns", new Document("$first", "$totalActiveCampaigns"))
                                .append("totalInvestmentPeriod", new Document("$first", "$totalInvestmentPeriod"))
                                .append("totalSales", new Document("$first", "$totalSales"))
                ),
                new Document("$project",
                        new Document("_id", 0)
                                .append("providerId", "$_id")
                                //.append("providerId", new Document("$toString", "$_id"))  // Convierte _id a String y lo renombra como providerId
                                .append("totalActiveCampaigns", 1)
                                .append("totalInvestmentPeriod", new Document("$toDouble", "$totalInvestmentPeriod"))
                                .append("totalSales", new Document("$toDouble", "$totalSales"))
                )
        );
        return reactiveMongoTemplate.getCollection("campaigns")
                .flatMapMany(collection -> collection.aggregate(pipeline,  Document.class));

    }

    private Mono<Kpi> saveOrUpdateKpi(Kpi kpi) {
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