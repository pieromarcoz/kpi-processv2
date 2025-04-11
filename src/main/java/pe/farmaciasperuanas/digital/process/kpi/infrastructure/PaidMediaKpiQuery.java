package pe.farmaciasperuanas.digital.process.kpi.infrastructure;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Modelo para una consulta de KPI de medios pagados
 * que incluye la consulta SQL y metadatos relacionados
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaidMediaKpiQuery {

    /**
     * Identificador del KPI
     */
    private String kpiId;

    /**
     * Descripción del KPI
     */
    private String description;

    /**
     * Tipo de KPI (número, monetario, porcentaje)
     */
    private String type;

    /**
     * Formato de presentación
     */
    private String format;

    /**
     * Consulta SQL para obtener los datos desde BigQuery
     */
    private String sqlQuery;

    /**
     * Indica si la consulta es incremental (acumula valores)
     */
    private boolean incremental;

    /**
     * Unidad de medida (si aplica)
     */
    private String unit;

    /**
     * Nombre de la colección MongoDB donde se deben guardar los resultados
     */
    private String targetCollection;

    /**
     * Formato de medios al que aplica (CM, VM, GS, GD)
     */
    private String mediaFormat;

    /**
     * Indica si la consulta puede ejecutarse con parámetros nulos
     */
    private boolean allowNullParameters;

    /**
     * Rango de valores aceptables mínimo
     */
    private Double minAcceptableValue;

    /**
     * Rango de valores aceptables máximo
     */
    private Double maxAcceptableValue;
}