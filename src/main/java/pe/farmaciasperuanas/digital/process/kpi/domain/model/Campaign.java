package pe.farmaciasperuanas.digital.process.kpi.domain.model;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Document(collection = "campaigns")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Campaign {
    @Id
    private String id;
    private String campaignId;      // ID de la campaña (202502052236)
    private String campaignSubId;   // Sub ID de la campaña (202502052236MC)
    private String name;            // Nombre de la campaña
    private String flag;            // Compañía asociada (inkafarma / mifarma)
    private String subCampaignType; // Subtipo de campaña
    private String campaignName;    // Duplicado?
    private String title;           // Título de la comunicación
    private String subtitle;        // Sub título de la comunicación
    private String funnel;          // Funnel de etapa de cliente
    private String subcategory;     // Subcategoría de la campaña
    private List<CampaignMedium> media;  // Reemplaza String medium
    private String purchaseType;    // Tipo de compra
    private String providerId;
    private String optimization;    // Optimización de la campaña
    private String division;        // División de la campaña
    private String segmentation;    // Segmentación demográfica
    private String brand;           // Marca de la campaña
    private List<Map<String, Object>> product;  // Productos asociados (Array de strings)
    private String JQ2;             // Jerarquía de categoría nivel2
    private String JQ3;             // Jerarquía de categoría nivel3
    private Date startDate;         // Fecha de inicio (Date)
    private Date endDate;           // Fecha de fin (Date)
    private Integer duration;       // Duración en días
    private Date shippingTime;      // Hora de envío
    private String campaignObjective; // Objetivo de la campaña
    private String status;          // Estado de la campaña
    private Integer investment;     // Inversión total asignada
    private String claimProm;       // Claim promocional
    private String card;            // Tipo de tarjeta promocional
    private String targetPublico;   // Público objetivo
    private List<String> recommendation; // Recomendaciones (Array)
    private List<String> insight;   // Insights (Array)
    private String createdUser;     // Usuario de creación
    private Date createdDate;       // Fecha de creación (Date)
    private Date updatedDate;       // Fecha de última actualización (Date)
    private String type;
    private String target;
    private String category;
    private Double roas;

}