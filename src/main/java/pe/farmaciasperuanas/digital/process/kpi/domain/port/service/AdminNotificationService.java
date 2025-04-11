package pe.farmaciasperuanas.digital.process.kpi.domain.port.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Servicio para notificar a administradores sobre errores críticos
 * en el proceso de cálculo de KPIs.
 *
 * Nota: Esta implementación es un esqueleto que registra las notificaciones en logs.
 * En un entorno de producción, se implementaría con envío real de notificaciones
 * mediante correo electrónico, SMS, Slack, etc.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class AdminNotificationService {

    @Value("${notification.admin.enabled:false}")
    private boolean notificationsEnabled;

    @Value("${notification.admin.recipients:admin@example.com}")
    private String[] adminRecipients;

    /**
     * Notifica a los administradores sobre un error crítico
     * @param subject Asunto de la notificación
     * @param message Mensaje detallado
     */
    public void notifyCriticalError(String subject, String message) {
        if (!notificationsEnabled) {
            log.info("Notificaciones deshabilitadas. No se enviará notificación para: {}", subject);
            return;
        }

        log.info("Enviando notificación de error crítico a administradores");
        log.info("Asunto: {}", subject);
        log.info("Mensaje: {}", message);
        log.info("Destinatarios: {}", String.join(", ", adminRecipients));

        // Aquí iría la implementación real de envío de notificaciones
        // Por ejemplo, envío de correo electrónico, integración con Slack, etc.
    }

    /**
     * Notifica a los administradores sobre valores anómalos en KPIs
     * @param kpiId Identificador del KPI
     * @param campaignId Identificador de la campaña
     * @param value Valor anómalo
     * @param details Detalles adicionales sobre la anomalía
     */
    public void notifyAnomalousValue(String kpiId, String campaignId, double value, String details) {
        String subject = "Valor anómalo detectado en KPI: " + kpiId;

        StringBuilder message = new StringBuilder();
        message.append("Se ha detectado un valor anómalo en el cálculo de KPIs:\n\n");
        message.append("- KPI: ").append(kpiId).append("\n");
        message.append("- Campaña: ").append(campaignId).append("\n");
        message.append("- Valor: ").append(value).append("\n");

        if (details != null && !details.isEmpty()) {
            message.append("- Detalles: ").append(details).append("\n");
        }

        message.append("\nPor favor, verifique los datos y la configuración para asegurar la calidad de las métricas.");

        notifyCriticalError(subject, message.toString());
    }
}