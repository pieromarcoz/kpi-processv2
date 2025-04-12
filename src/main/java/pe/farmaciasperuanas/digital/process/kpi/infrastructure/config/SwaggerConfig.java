package pe.farmaciasperuanas.digital.process.kpi.infrastructure.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Configuración de Swagger/OpenAPI para documentar los endpoints de la API
 */
@Configuration
public class SwaggerConfig {

    @Value("${spring.application.name}")
    private String applicationName;

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("KPI Process API Documentation")
                        .description(
                                "API para generación y monitoreo de KPIs de campañas de marketing." +
                                        "\n\n**Características principales:**" +
                                        "\n- Generación de KPIs para medios propios (mail, push)" +
                                        "\n- Validación de cobertura de KPIs" +
                                        "\n- Monitoreo de rendimiento de las operaciones" +
                                        "\n- Consulta de métricas de rendimiento"
                        )
                        .version("1.1.0")
                        .contact(new Contact()
                                .name("Equipo de Desarrollo de KPIs")
                                .email("kpi-team@farmaciasperuanas.pe")
                                .url("https://www.farmaciasperuanas.pe"))
                        .license(new License()
                                .name("Propietario")
                                .url("https://www.farmaciasperuanas.pe"))
                )
                .servers(List.of(
                        new Server()
                                .url("/")
                                .description("Servidor actual"),
                        new Server()
                                .url("https://dev-api.farmaciasperuanas.pe")
                                .description("Entorno de desarrollo"),
                        new Server()
                                .url("https://api.farmaciasperuanas.pe")
                                .description("Entorno de producción")
                ));
    }
}