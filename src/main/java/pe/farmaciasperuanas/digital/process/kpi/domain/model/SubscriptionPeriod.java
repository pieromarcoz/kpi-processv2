package pe.farmaciasperuanas.digital.process.kpi.domain.model;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SubscriptionPeriod {
    private LocalDate startDate;
    private LocalDate endDate;
}
