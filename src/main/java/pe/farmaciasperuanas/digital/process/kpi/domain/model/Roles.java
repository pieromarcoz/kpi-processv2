package pe.farmaciasperuanas.digital.process.kpi.domain.model;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Roles {

    private String email;
    private String roleName;
}
