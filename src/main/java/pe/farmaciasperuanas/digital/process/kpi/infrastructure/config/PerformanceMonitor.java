package pe.farmaciasperuanas.digital.process.kpi.infrastructure.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Utilidad para monitorear el rendimiento de métodos críticos.
 * Esta es una alternativa más simple que no requiere AspectJ.
 */
@Component
@Slf4j
public class PerformanceMonitor {

    // Almacenamiento de métricas acumuladas por método
    private final Map<String, MethodMetrics> methodMetrics = new ConcurrentHashMap<>();

    // Clase para almacenar métricas de un método
    private static class MethodMetrics {
        final String methodName;
        final AtomicInteger invocationCount = new AtomicInteger(0);
        final AtomicLong totalTimeMs = new AtomicLong(0);
        final AtomicLong maxTimeMs = new AtomicLong(0);
        final AtomicLong minTimeMs = new AtomicLong(Long.MAX_VALUE);

        public MethodMetrics(String methodName) {
            this.methodName = methodName;
        }

        public void addExecution(long timeMs) {
            invocationCount.incrementAndGet();
            totalTimeMs.addAndGet(timeMs);
            updateMax(timeMs);
            updateMin(timeMs);
        }

        private void updateMax(long timeMs) {
            long current;
            do {
                current = maxTimeMs.get();
                if (timeMs <= current) return;
            } while (!maxTimeMs.compareAndSet(current, timeMs));
        }

        private void updateMin(long timeMs) {
            long current;
            do {
                current = minTimeMs.get();
                if (timeMs >= current) return;
            } while (!minTimeMs.compareAndSet(current, timeMs));
        }

        public double getAvgTimeMs() {
            int count = invocationCount.get();
            return count > 0 ? (double) totalTimeMs.get() / count : 0;
        }

        public Map<String, Object> getMetricsMap() {
            Map<String, Object> metrics = new HashMap<>();
            metrics.put("methodName", methodName);
            metrics.put("invocationCount", invocationCount.get());
            metrics.put("totalTimeMs", totalTimeMs.get());
            metrics.put("avgTimeMs", getAvgTimeMs());
            metrics.put("maxTimeMs", maxTimeMs.get());
            metrics.put("minTimeMs", minTimeMs.get() == Long.MAX_VALUE ? 0 : minTimeMs.get());
            return metrics;
        }
    }

    /**
     * Monitorea la ejecución de un método.
     *
     * @param <T> Tipo de retorno del método
     * @param methodName Nombre del método a monitorear
     * @param execution Función que ejecuta el método
     * @return El resultado del método
     */
    public <T> T monitor(String methodName, Supplier<T> execution) {
        StopWatch stopWatch = new StopWatch();

        try {
            stopWatch.start();
            return execution.get();
        } finally {
            stopWatch.stop();
            long timeMs = stopWatch.getTotalTimeMillis();

            // Obtener o crear métricas para este método
            MethodMetrics metrics = methodMetrics.computeIfAbsent(
                    methodName,
                    PerformanceMonitor.MethodMetrics::new
            );

            // Actualizar métricas
            metrics.addExecution(timeMs);

            // Log detallado si el tiempo excede umbrales
            if (timeMs > 5000) {
                log.warn("¡ALERTA DE RENDIMIENTO! Método {} tardó {} ms", methodName, timeMs);
            } else if (timeMs > 1000) {
                log.info("Rendimiento: Método {} tardó {} ms", methodName, timeMs);
            } else if (log.isDebugEnabled()) {
                log.debug("Método {} ejecutado en {} ms", methodName, timeMs);
            }
        }
    }

    /**
     * Versión sin retorno para métodos void
     */
    public void monitorVoid(String methodName, Runnable execution) {
        monitor(methodName, () -> {
            execution.run();
            return null;
        });
    }

    /**
     * Obtiene estadísticas de rendimiento de todos los métodos monitoreados
     * @return Mapa con estadísticas por método
     */
    public Map<String, Object> getPerformanceStatistics() {
        Map<String, Object> statistics = new HashMap<>();

        // Lista de métricas para todos los métodos
        statistics.put("methodCount", methodMetrics.size());
        statistics.put("methods", methodMetrics.values().stream()
                .map(MethodMetrics::getMetricsMap)
                .toArray());

        // Métricas globales
        long totalInvocations = methodMetrics.values().stream()
                .mapToInt(m -> m.invocationCount.get())
                .sum();
        long totalTime = methodMetrics.values().stream()
                .mapToLong(m -> m.totalTimeMs.get())
                .sum();

        statistics.put("totalInvocations", totalInvocations);
        statistics.put("totalExecutionTimeMs", totalTime);

        // Top 5 métodos más lentos (promedio)
        statistics.put("top5SlowestMethods", methodMetrics.values().stream()
                .filter(m -> m.invocationCount.get() > 0)
                .sorted((m1, m2) -> Double.compare(m2.getAvgTimeMs(), m1.getAvgTimeMs()))
                .limit(5)
                .map(MethodMetrics::getMetricsMap)
                .toArray());

        return statistics;
    }

    /**
     * Resetea todas las estadísticas de rendimiento
     */
    public void resetStatistics() {
        methodMetrics.clear();
        log.info("Estadísticas de rendimiento reseteadas");
    }
}