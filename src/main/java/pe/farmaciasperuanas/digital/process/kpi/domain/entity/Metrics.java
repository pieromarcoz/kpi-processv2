package pe.farmaciasperuanas.digital.process.kpi.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
//import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Document(collection = "metrics")
public class Metrics {
    @Id
    private String id;
    //NEC
    //hay un intento de convertir un String en un ObjectId de MongoDB
    //el valor de provider Id es un string del tipo: PROV12123
    //private ObjectId providerId;
    private String providerId;
    private int totalActiveCampaigns;
    private Double totalInvestmentPeriod;
    private Double totalSales;
    private String createdUser;
    private LocalDateTime createdDate;
    private LocalDateTime updatedDate;
}
