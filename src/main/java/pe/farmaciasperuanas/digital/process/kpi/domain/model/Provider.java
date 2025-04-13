package pe.farmaciasperuanas.digital.process.kpi.domain.model;

import lombok.*;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;
import java.time.LocalDate;
import java.util.Date;

@Document(collection = "provider")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Provider {
    @Id
    private String id;
    @Indexed(name = "providerId_idx")
    private String providerId;
    private String codSap;
    private String name;
    private String companyName;
    private String fiscalAddress;
    private List<Roles> roles;
    private String status;
    private String createdUser;
    @CreatedDate
    private LocalDate createdDate;
    @LastModifiedDate
    private LocalDate updatedDate;
    private SubscriptionPeriod subscriptionPeriod;

}
