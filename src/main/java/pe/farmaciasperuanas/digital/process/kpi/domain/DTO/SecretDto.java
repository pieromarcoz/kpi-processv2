package pe.farmaciasperuanas.digital.process.kpi.domain.DTO;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SecretDto {
    private String uri;
    private String database;
    private String username;
    private String password;
}