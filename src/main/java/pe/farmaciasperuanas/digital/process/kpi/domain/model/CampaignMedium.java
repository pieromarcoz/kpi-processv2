package pe.farmaciasperuanas.digital.process.kpi.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CampaignMedium {
    private String medium;      // mail, push, medios pagados
    private String platform;    // Salesforce, Meta, Google, etc.
    private String format;      // Formato específico (Mail cabecera, Reels, etc.)
    private String subFormat;   // Card1, Card2, etc.
    private String kpiReference; // Referencia al KPI para este medio específico (MP-I, MC-C, etc.)
}