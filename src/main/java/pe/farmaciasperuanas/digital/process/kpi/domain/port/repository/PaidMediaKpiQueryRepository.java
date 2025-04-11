package pe.farmaciasperuanas.digital.process.kpi.domain.port.repository;
import org.springframework.stereotype.Repository;
import pe.farmaciasperuanas.digital.process.kpi.infrastructure.PaidMediaKpiQuery;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Repositorio que contiene las consultas SQL necesarias para los KPIs de medios pagados.
 * En un entorno real, estas consultas podrían estar en una base de datos o
 * en archivos de configuración.
 */
@Repository
public class PaidMediaKpiQueryRepository {

    // Lista de consultas SQL para KPIs
    private final List<PaidMediaKpiQuery> kpiQueries;

    // Mapa para acceso rápido por ID de KPI
    private final Map<String, PaidMediaKpiQuery> kpiQueriesMap;

    public PaidMediaKpiQueryRepository() {
        this.kpiQueries = initializeKpiQueries();
        this.kpiQueriesMap = kpiQueries.stream()
                .collect(Collectors.toMap(PaidMediaKpiQuery::getKpiId, query -> query));
    }

    /**
     * Encuentra una consulta por ID de KPI
     * @param kpiId Identificador del KPI (e.g., "CM-INV", "VM-ROA")
     * @return La consulta SQL o null si no existe
     */
    public PaidMediaKpiQuery findByKpiId(String kpiId) {
        return kpiQueriesMap.get(kpiId);
    }

    /**
     * Obtiene todas las consultas para un formato específico
     * @param format Formato de medio (CM, VM, GS, GD)
     * @return Lista de consultas para el formato
     */
    public List<PaidMediaKpiQuery> findByFormat(String format) {
        return kpiQueries.stream()
                .filter(query -> query.getMediaFormat().equals(format))
                .collect(Collectors.toList());
    }

    /**
     * Inicializa las consultas SQL para los KPIs de medios pagados.
     * En un entorno real, estas podrían cargarse desde archivos o desde una base de datos.
     */
    private List<PaidMediaKpiQuery> initializeKpiQueries() {
        return Arrays.asList(
                // CARRUSEL META (CM)
                createKpiQuery("CM-INV", "Inversión", "Monetario", "S/ X,XXX.XX",
                        "SELECT SUM(amount) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?",
                        "CM"),

                createKpiQuery("CM-ROA", "ROAS", "Número", "X.X",
                        "SELECT (SELECT SUM(revenue) FROM `project.dataset.sales` WHERE campaign_id = ? AND date BETWEEN ? AND ?) / " +
                                "(SELECT SUM(amount) FROM `project.dataset.meta_ads` WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?) as value",
                        "CM"),

                createKpiQuery("CM-ALC", "Alcance", "Número", "Número con formato",
                        "SELECT SUM(reach) as value FROM `bq_analytics_slayer_ads_google_alcance` " +
                                "WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?",
                        "CM"),

                createKpiQuery("CM-IMP", "Impresiones", "Número", "Número con formato",
                        "SELECT SUM(impressions) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?",
                        "CM"),

                createKpiQuery("CM-FRE", "Frecuencia", "Número", "X.X",
                        "SELECT SUM(impressions)/SUM(reach) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?",
                        "CM"),

                createKpiQuery("CM-CLE", "Clics en el enlace", "Número", "Número con formato",
                        "SELECT SUM(clicks) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?",
                        "CM"),

                createKpiQuery("CM-CPC", "CPC", "Monetario", "S/ X.XX",
                        "SELECT SUM(amount)/SUM(clicks) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?",
                        "CM"),

                createKpiQuery("CM-CTR", "CTR", "Porcentaje", "X.X%",
                        "SELECT SUM(clicks)/SUM(impressions)*100 as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?",
                        "CM"),

                createKpiQuery("CM-VIP", "Visitas a la página", "Número", "Número con formato",
                        "SELECT SUM(page_views) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?",
                        "CM"),

                createKpiQuery("CM-COV", "Costo por Visita", "Monetario", "S/ X.XX",
                        "SELECT SUM(amount)/SUM(page_views) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?",
                        "CM"),

                createKpiQuery("CM-CPM", "CPM", "Monetario", "S/ X.XX",
                        "SELECT SUM(amount)/SUM(impressions)*1000 as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?",
                        "CM"),

                createKpiQuery("CM-COM", "Compras", "Monetario", "S/ X,XXX.XX",
                        "SELECT SUM(revenue) as value FROM `project.dataset.sales` " +
                                "WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?",
                        "CM"),

                createKpiQuery("CM-CPA", "CPA", "Monetario", "S/ X.XX",
                        "SELECT SUM(amount)/SUM(conversions) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?",
                        "CM"),

                createKpiQuery("CM-CON", "Conversiones", "Número", "Número con formato",
                        "SELECT SUM(conversions) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?",
                        "CM"),

                createKpiQuery("CM-SES", "Sesiones", "Número", "Número con formato",
                        "SELECT SUM(sessions) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'carousel' AND date BETWEEN ? AND ?",
                        "CM"),

                // VIDEO META (VM)
                createKpiQuery("VM-INV", "Inversión", "Monetario", "S/ X,XXX.XX",
                        "SELECT SUM(amount) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-ROA", "ROAS", "Número", "X.X",
                        "SELECT (SELECT SUM(revenue) FROM `project.dataset.sales` WHERE campaign_id = ? AND date BETWEEN ? AND ?) / " +
                                "(SELECT SUM(amount) FROM `project.dataset.meta_ads` WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?) as value",
                        "VM"),

                createKpiQuery("VM-ALC", "Alcance", "Número", "Número con formato",
                        "SELECT SUM(reach) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-IMP", "Impresiones", "Número", "Número con formato",
                        "SELECT SUM(impressions) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-FRE", "Frecuencia", "Número", "X.X",
                        "SELECT SUM(impressions)/SUM(reach) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-CLE", "Clics en el enlace", "Número", "Número con formato",
                        "SELECT SUM(clicks) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-CPC", "CPC", "Monetario", "S/ X.XX",
                        "SELECT SUM(amount)/SUM(clicks) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-CTR", "CTR", "Porcentaje", "X.X%",
                        "SELECT SUM(clicks)/SUM(impressions)*100 as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-VIP", "Visitas a la página", "Número", "Número con formato",
                        "SELECT SUM(page_views) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-COV", "Costo por Visita", "Monetario", "S/ X.XX",
                        "SELECT SUM(amount)/SUM(page_views) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-CPM", "CPM", "Monetario", "S/ X.XX",
                        "SELECT SUM(amount)/SUM(impressions)*1000 as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-THR", "ThruPlay", "Número", "Número con formato",
                        "SELECT SUM(thruplay) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-CTH", "Costo por ThruPlay", "Monetario", "S/ X.XX",
                        "SELECT SUM(amount)/SUM(thruplay) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-COM", "Compras", "Monetario", "S/ X,XXX.XX",
                        "SELECT SUM(revenue) as value FROM `project.dataset.sales` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-CPA", "CPA", "Monetario", "S/ X.XX",
                        "SELECT SUM(amount)/SUM(conversions) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-CON", "Conversiones", "Número", "Número con formato",
                        "SELECT SUM(conversions) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                createKpiQuery("VM-SES", "Sesiones", "Número", "Número con formato",
                        "SELECT SUM(sessions) as value FROM `project.dataset.meta_ads` " +
                                "WHERE campaign_id = ? AND format = 'video' AND date BETWEEN ? AND ?",
                        "VM"),

                // GOOGLE SEARCH (GS)
                createKpiQuery("GS-INV", "Inversión", "Monetario", "S/ X,XXX.XX",
                        "SELECT SUM(cost) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'search' AND date BETWEEN ? AND ?",
                        "GS"),

                createKpiQuery("GS-ROA", "ROAS", "Número", "X.X",
                        "SELECT (SELECT SUM(revenue) FROM `project.dataset.sales` WHERE campaign_id = ? AND date BETWEEN ? AND ?) / " +
                                "(SELECT SUM(cost) FROM `project.dataset.google_ads` WHERE campaign_id = ? AND ad_type = 'search' AND date BETWEEN ? AND ?) as value",
                        "GS"),

                createKpiQuery("GS-ALC", "Alcance", "Número", "Número con formato",
                        "SELECT COUNT(DISTINCT user_id) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'search' AND date BETWEEN ? AND ?",
                        "GS"),

                createKpiQuery("GS-IMP", "Impresiones", "Número", "Número con formato",
                        "SELECT SUM(impressions) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'search' AND date BETWEEN ? AND ?",
                        "GS"),

                createKpiQuery("GS-CLE", "Clics", "Número", "Número con formato",
                        "SELECT SUM(clicks) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'search' AND date BETWEEN ? AND ?",
                        "GS"),

                createKpiQuery("GS-CPC", "CPC", "Monetario", "S/ X.XX",
                        "SELECT SUM(cost)/SUM(clicks) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'search' AND date BETWEEN ? AND ?",
                        "GS"),

                createKpiQuery("GS-CTR", "CTR", "Porcentaje", "X.X%",
                        "SELECT SUM(clicks)/SUM(impressions)*100 as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'search' AND date BETWEEN ? AND ?",
                        "GS"),

                createKpiQuery("GS-CPM", "CPM", "Monetario", "S/ X.XX",
                        "SELECT SUM(cost)/SUM(impressions)*1000 as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'search' AND date BETWEEN ? AND ?",
                        "GS"),

                createKpiQuery("GS-IMS", "Impression Share %", "Porcentaje", "XX.X%",
                        "SELECT AVG(impression_share)*100 as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'search' AND date BETWEEN ? AND ?",
                        "GS"),

                createKpiQuery("GS-SES", "Sesiones", "Número", "Número con formato",
                        "SELECT SUM(sessions) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'search' AND date BETWEEN ? AND ?",
                        "GS"),

                createKpiQuery("GS-COM", "Ventas", "Monetario", "S/ X,XXX.XX",
                        "SELECT SUM(revenue) as value FROM `project.dataset.sales` " +
                                "WHERE campaign_id = ? AND source = 'google_search' AND date BETWEEN ? AND ?",
                        "GS"),

                createKpiQuery("GS-CON", "Conversiones", "Número", "Número con formato",
                        "SELECT SUM(conversions) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'search' AND date BETWEEN ? AND ?",
                        "GS"),

                // GOOGLE DISPLAY (GD)
                createKpiQuery("GD-INV", "Inversión", "Monetario", "S/ X,XXX.XX",
                        "SELECT SUM(cost) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'display' AND date BETWEEN ? AND ?",
                        "GD"),

                createKpiQuery("GD-ROA", "ROAS", "Número", "X.X",
                        "SELECT (SELECT SUM(revenue) FROM `project.dataset.sales` WHERE campaign_id = ? AND date BETWEEN ? AND ?) / " +
                                "(SELECT SUM(cost) FROM `project.dataset.google_ads` WHERE campaign_id = ? AND ad_type = 'display' AND date BETWEEN ? AND ?) as value",
                        "GD"),

                createKpiQuery("GD-ALC", "Alcance", "Número", "Número con formato",
                        "SELECT COUNT(DISTINCT user_id) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'display' AND date BETWEEN ? AND ?",
                        "GD"),

                createKpiQuery("GD-IMP", "Impresiones", "Número", "Número con formato",
                        "SELECT SUM(impressions) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'display' AND date BETWEEN ? AND ?",
                        "GD"),

                createKpiQuery("GD-FRE", "Frecuencia", "Número", "X.X",
                        "SELECT SUM(impressions)/COUNT(DISTINCT user_id) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'display' AND date BETWEEN ? AND ?",
                        "GD"),

                createKpiQuery("GD-CLE", "Clics", "Número", "Número con formato",
                        "SELECT SUM(clicks) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'display' AND date BETWEEN ? AND ?",
                        "GD"),

                createKpiQuery("GD-CPC", "CPC", "Monetario", "S/ X.XX",
                        "SELECT SUM(cost)/SUM(clicks) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'display' AND date BETWEEN ? AND ?",
                        "GD"),

                createKpiQuery("GD-CTR", "CTR", "Porcentaje", "X.X%",
                        "SELECT SUM(clicks)/SUM(impressions)*100 as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'display' AND date BETWEEN ? AND ?",
                        "GD"),

                createKpiQuery("GD-CPM", "CPM", "Monetario", "S/ X.XX",
                        "SELECT SUM(cost)/SUM(impressions)*1000 as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'display' AND date BETWEEN ? AND ?",
                        "GD"),

                createKpiQuery("GD-SES", "Sesiones", "Número", "Número con formato",
                        "SELECT SUM(sessions) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'display' AND date BETWEEN ? AND ?",
                        "GD"),

                createKpiQuery("GD-COM", "Compras", "Monetario", "S/ X,XXX.XX",
                        "SELECT SUM(revenue) as value FROM `project.dataset.sales` " +
                                "WHERE campaign_id = ? AND source = 'google_display' AND date BETWEEN ? AND ?",
                        "GD"),

                createKpiQuery("GD-CON", "Conversiones", "Número", "Número con formato",
                        "SELECT SUM(conversions) as value FROM `project.dataset.google_ads` " +
                                "WHERE campaign_id = ? AND ad_type = 'display' AND date BETWEEN ? AND ?",
                        "GD")
        );
    }

    /**
     * Método para facilitar la creación de objetos PaidMediaKpiQuery
     */
    private PaidMediaKpiQuery createKpiQuery(String kpiId, String description, String type,
                                             String format, String sqlQuery, String mediaFormat) {
        return PaidMediaKpiQuery.builder()
                .kpiId(kpiId)
                .description(description)
                .type(type)
                .format(format)
                .sqlQuery(sqlQuery)
                .mediaFormat(mediaFormat)
                .incremental(true) // La mayoría de las métricas son incrementales
                .targetCollection("KPIs") // Como se especifica en el requisito
                .allowNullParameters(false)
                .build();
    }
}