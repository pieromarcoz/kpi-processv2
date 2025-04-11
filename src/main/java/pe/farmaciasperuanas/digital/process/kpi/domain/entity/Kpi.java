package pe.farmaciasperuanas.digital.process.kpi.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.LocalDateTime;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Document(collection = "kpi_des")
public class Kpi {
    @Id
    private String id;
    private String campaignId;
    private String campaignSubId;
    private String kpiId;
    private String kpiDescription;
    private String type;
    private Double value;
    private String status;
    private String createdUser;
    private LocalDateTime createdDate;
    private LocalDateTime updatedDate;
    private String format;
}
