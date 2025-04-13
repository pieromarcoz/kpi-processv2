package pe.farmaciasperuanas.digital.process.kpi.infrastructure;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Componente para formatear los resultados de KPIs según los requisitos
 * especificados en las tablas de codificación de la historia de usuario.
 */
@Component
@Slf4j
public class KpiResultFormatter {

    // Mapa de formatos por tipo de métrica
    private static final Map<String, FormatType> FORMAT_TYPES = new HashMap<>();

    // Configuración de formatos por prefijo de KPI
    static {
        // Formatos para Meta Carrusel (CM)
        configureFormats("CM-INV", FormatType.MONETARY, "S/ #,##0.00");
        configureFormats("CM-ROA", FormatType.NUMBER, "0.0");
        configureFormats("CM-ALC", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("CM-IMP", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("CM-FRE", FormatType.NUMBER, "0.0");
        configureFormats("CM-CLE", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("CM-CPC", FormatType.MONETARY, "S/ 0.00");
        configureFormats("CM-CTR", FormatType.PERCENTAGE, "0.0%");
        configureFormats("CM-VIP", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("CM-COV", FormatType.MONETARY, "S/ 0.00");
        configureFormats("CM-CPM", FormatType.MONETARY, "S/ 0.00");
        configureFormats("CM-COM", FormatType.MONETARY, "S/ #,##0.00");
        configureFormats("CM-CPA", FormatType.MONETARY, "S/ 0.00");
        configureFormats("CM-CON", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("CM-SES", FormatType.NUMBER_FORMAT, "#,###");

        // Formatos para Meta Video (VM)
        configureFormats("VM-INV", FormatType.MONETARY, "S/ #,##0.00");
        configureFormats("VM-ROA", FormatType.NUMBER, "0.0");
        configureFormats("VM-ALC", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("VM-IMP", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("VM-FRE", FormatType.NUMBER, "0.0");
        configureFormats("VM-CLE", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("VM-CPC", FormatType.MONETARY, "S/ 0.00");
        configureFormats("VM-CTR", FormatType.PERCENTAGE, "0.0%");
        configureFormats("VM-VIP", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("VM-COV", FormatType.MONETARY, "S/ 0.00");
        configureFormats("VM-CPM", FormatType.MONETARY, "S/ 0.00");
        configureFormats("VM-THR", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("VM-CTH", FormatType.MONETARY, "S/ 0.00");
        configureFormats("VM-COM", FormatType.MONETARY, "S/ #,##0.00");
        configureFormats("VM-CPA", FormatType.MONETARY, "S/ 0.00");
        configureFormats("VM-CON", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("VM-SES", FormatType.NUMBER_FORMAT, "#,###");

        // Formatos para Google Search (GS)
        configureFormats("GS-INV", FormatType.MONETARY, "S/ #,##0.00");
        configureFormats("GS-ROA", FormatType.NUMBER, "0.0");
        configureFormats("GS-ALC", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("GS-IMP", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("GS-CLE", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("GS-CPC", FormatType.MONETARY, "S/ 0.00");
        configureFormats("GS-CTR", FormatType.PERCENTAGE, "0.0%");
        configureFormats("GS-CPM", FormatType.MONETARY, "S/ 0.00");
        configureFormats("GS-IMS", FormatType.PERCENTAGE, "##0.0%");
        configureFormats("GS-SES", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("GS-COM", FormatType.MONETARY, "S/ #,##0.00");
        configureFormats("GS-CON", FormatType.NUMBER_FORMAT, "#,###");

        // Formatos para Google Display (GD)
        configureFormats("GD-INV", FormatType.MONETARY, "S/ #,##0.00");
        configureFormats("GD-ROA", FormatType.NUMBER, "0.0");
        configureFormats("GD-ALC", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("GD-IMP", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("GD-FRE", FormatType.NUMBER, "0.0");
        configureFormats("GD-CLE", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("GD-CPC", FormatType.MONETARY, "S/ 0.00");
        configureFormats("GD-CTR", FormatType.PERCENTAGE, "0.0%");
        configureFormats("GD-CPM", FormatType.MONETARY, "S/ 0.00");
        configureFormats("GD-SES", FormatType.NUMBER_FORMAT, "#,###");
        configureFormats("GD-COM", FormatType.MONETARY, "S/ #,##0.00");
        configureFormats("GD-CON", FormatType.NUMBER_FORMAT, "#,###");
    }

    private static void configureFormats(String kpiCode, FormatType type, String pattern) {
        FORMAT_TYPES.put(kpiCode, type);
    }

    /**
     * Formatea un valor de KPI según su tipo y patrón de formato
     * @param kpi KPI con el valor a formatear
     * @return Valor formateado como cadena
     */
    public String formatKpiValue(Kpi kpi) {
        String kpiId = kpi.getKpiId();
        double value = kpi.getValue();

        FormatType formatType = FORMAT_TYPES.getOrDefault(kpiId, FormatType.NUMBER);

        try {
            switch (formatType) {
                case MONETARY:
                    return formatMonetary(value);
                case PERCENTAGE:
                    return formatPercentage(value);
                case NUMBER:
                    return formatNumber(value);
                case NUMBER_FORMAT:
                    return formatNumberWithFormat(value);
                default:
                    return String.valueOf(value);
            }
        } catch (Exception e) {
            log.error("Error al formatear valor para KPI {}: {}", kpiId, e.getMessage());
            return String.valueOf(value);
        }
    }

    /**
     * Formatea un valor monetario (S/ X,XXX.XX)
     */
    private String formatMonetary(double value) {
        NumberFormat formatter = NumberFormat.getCurrencyInstance(new Locale("es", "PE"));
        formatter.setMinimumFractionDigits(2);
        formatter.setMaximumFractionDigits(2);
        return formatter.format(value).replace("PEN", "S/");
    }

    /**
     * Formatea un valor porcentual (X.X%)
     */
    private String formatPercentage(double value) {
        // Convertir a porcentaje si es decimal (0.05 -> 5%)
        double percentValue = value;
        if (value < 1 && value > 0) {
            percentValue = value * 100;
        }

        DecimalFormat df = new DecimalFormat("0.0%");
        return df.format(percentValue / 100);
    }

    /**
     * Formatea un número simple (X.X)
     */
    private String formatNumber(double value) {
        DecimalFormat df = new DecimalFormat("0.0");
        return df.format(value);
    }

    /**
     * Formatea un número con formato (X,XXX)
     */
    private String formatNumberWithFormat(double value) {
        DecimalFormat df = new DecimalFormat("#,###");
        return df.format(value);
    }

    /**
     * Tipos de formato para KPIs
     */
    private enum FormatType {
        MONETARY,      // S/ X,XXX.XX
        NUMBER,        // X.X
        NUMBER_FORMAT, // Número con formato de miles
        PERCENTAGE     // X.X%
    }
}