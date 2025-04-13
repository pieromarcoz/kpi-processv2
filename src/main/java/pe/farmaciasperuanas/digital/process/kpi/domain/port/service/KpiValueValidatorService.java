package pe.farmaciasperuanas.digital.process.kpi.domain.port.service;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.KpiRepositoryCustom;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Servicio para validar los valores de los KPIs
 * verificando que estén dentro de rangos aceptables y que no presenten cambios bruscos
 * respecto a valores históricos.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class KpiValueValidatorService {

    private final KpiRepositoryCustom kpiRepository;

    // Rangos aceptables para diferentes tipos de KPI
    private static final Map<String, double[]> KPI_VALID_RANGES = Map.of(
            "ROA", new double[]{0.1, 20.0},
            "CTR", new double[]{0.001, 0.20},
            "CPC", new double[]{0.01, 100.0},
            "CPM", new double[]{0.5, 50.0},
            "FRE", new double[]{1.0, 10.0}
    );

    // Umbral para marcar un cambio como sospechoso (200% como se indica en los requisitos)
    private static final double CHANGE_THRESHOLD = 2.0;

    /**
     * Valida un valor de KPI contra rangos aceptables
     * @param kpiType Tipo de KPI (ROA, CTR, etc.)
     * @param value Valor a validar
     * @return true si está dentro del rango válido, false en caso contrario
     */
    public boolean isValueInAcceptableRange(String kpiType, double value) {
        // Extraer el tipo de KPI sin el prefijo del formato si existe
        String baseType = extractBaseType(kpiType);

        // Si hay un rango definido para este tipo, validarlo
        if (KPI_VALID_RANGES.containsKey(baseType)) {
            double[] range = KPI_VALID_RANGES.get(baseType);
            if (value < range[0] || value > range[1]) {
                log.warn("Valor de KPI fuera de rango: {} = {} (rango: {} - {})",
                        kpiType, value, range[0], range[1]);
                return false;
            }
        }

        return true;
    }

    /**
     * Extrae el tipo base de un KPI sin el prefijo del formato
     * Por ejemplo: CM-ROA -> ROA
     */
    private String extractBaseType(String kpiType) {
        int dashIndex = kpiType.indexOf('-');
        if (dashIndex > 0 && dashIndex < kpiType.length() - 1) {
            return kpiType.substring(dashIndex + 1);
        }
        return kpiType;
    }

    /**
     * Verifica si un valor de KPI representa un cambio brusco respecto a valores históricos
     * @param kpi KPI a validar
     * @return Mono que emite true si el cambio es sospechoso, false en caso contrario
     */
    public Mono<Boolean> isSuspiciousChange(Kpi kpi) {
        // Obtener valores históricos de los últimos 7 días (excluyendo el actual)
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime sevenDaysAgo = now.minusDays(7);

        return kpiRepository.findByCampaignIdAndKpiIdInAndCreatedDateBetween(
                        kpi.getCampaignId(),
                        List.of(kpi.getKpiId()),
                        sevenDaysAgo,
                        now
                )
                .filter(historicKpi -> !historicKpi.getId().equals(kpi.getId())) // Excluir el KPI actual
                .collectList()
                .map(historicKpis -> {
                    if (historicKpis.isEmpty()) {
                        // No hay datos históricos para comparar
                        return false;
                    }

                    // Calcular el promedio de los valores históricos
                    double historicAverage = historicKpis.stream()
                            .mapToDouble(Kpi::getValue)
                            .average()
                            .orElse(0);

                    if (historicAverage == 0) {
                        // No se puede calcular un cambio relativo si el promedio es cero
                        return false;
                    }

                    // Calcular el cambio relativo
                    double currentValue = kpi.getValue();
                    double relativeChange = Math.abs(currentValue - historicAverage) / historicAverage;

                    // Marcar como sospechoso si el cambio supera el umbral (200%)
                    boolean isSuspicious = relativeChange > CHANGE_THRESHOLD;

                    if (isSuspicious) {
                        log.warn("Cambio sospechoso detectado en KPI {}: valor actual={}, promedio histórico={}, cambio={}%",
                                kpi.getKpiId(), currentValue, historicAverage, relativeChange * 100);
                    }

                    return isSuspicious;
                });
    }

    /**
     * Verifica la consistencia entre métricas relacionadas
     * Por ejemplo, CTR = Clics / Impresiones
     * @param kpis Lista de KPIs a validar
     * @return true si las métricas son consistentes, false en caso contrario
     */
    public boolean validateMetricsConsistency(List<Kpi> kpis) {
        // Agrupar KPIs por campaignId
        Map<String, List<Kpi>> kpisByCampaign = kpis.stream()
                .collect(Collectors.groupingBy(Kpi::getCampaignId));

        boolean allConsistent = true;

        // Para cada campaña, verificar la consistencia de sus métricas
        for (Map.Entry<String, List<Kpi>> entry : kpisByCampaign.entrySet()) {
            String campaignId = entry.getKey();
            List<Kpi> campaignKpis = entry.getValue();

            // Convertir a un mapa para facilitar el acceso
            Map<String, Double> metrics = campaignKpis.stream()
                    .collect(Collectors.toMap(Kpi::getKpiId, Kpi::getValue));

            // Reglas de consistencia para diferentes tipos de métricas

            // Verificar CTR = Clics / Impresiones
            allConsistent &= checkConsistency(metrics,
                    "CLE", "IMP", "CTR",
                    (clics, imp) -> imp > 0 ? clics / imp : 0,
                    campaignId, "CTR"
            );

            // Verificar CPC = Inversión / Clics
            allConsistent &= checkConsistency(metrics,
                    "INV", "CLE", "CPC",
                    (inv, clics) -> clics > 0 ? inv / clics : 0,
                    campaignId, "CPC"
            );

            // Verificar ROAS = Ventas / Inversión
            allConsistent &= checkConsistency(metrics,
                    "COM", "INV", "ROA",
                    (ventas, inv) -> inv > 0 ? ventas / inv : 0,
                    campaignId, "ROAS"
            );
        }

        return allConsistent;
    }

    /**
     * Verifica una regla de consistencia específica entre métricas relacionadas
     */
    private boolean checkConsistency(
            Map<String, Double> metrics,
            String metric1Code,
            String metric2Code,
            String resultCode,
            BiFunction<Double, Double, Double> calculation,
            String campaignId,
            String metricName) {

        // Buscar las métricas por su código, probando con diferentes prefijos
        String[] prefixes = {"CM-", "VM-", "GS-", "GD-"};

        for (String prefix : prefixes) {
            String key1 = prefix + metric1Code;
            String key2 = prefix + metric2Code;
            String resultKey = prefix + resultCode;

            if (metrics.containsKey(key1) && metrics.containsKey(key2) && metrics.containsKey(resultKey)) {
                double value1 = metrics.get(key1);
                double value2 = metrics.get(key2);
                double storedResult = metrics.get(resultKey);

                // Calcular el resultado esperado
                double expectedResult = calculation.apply(value1, value2);

                // Permitir un pequeño margen de error por redondeo (1%)
                double tolerance = 0.01;
                double relativeError = Math.abs(expectedResult - storedResult) /
                        (storedResult != 0 ? storedResult : 1);

                if (relativeError > tolerance) {
                    log.warn("Inconsistencia en {} para campaña {}: calculado={}, almacenado={}, error={}%",
                            metricName, campaignId, expectedResult, storedResult, relativeError * 100);
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Interfaz funcional para cálculos de consistencia
     */
    @FunctionalInterface
    private interface BiFunction<T, U, R> {
        R apply(T t, U u);
    }
}