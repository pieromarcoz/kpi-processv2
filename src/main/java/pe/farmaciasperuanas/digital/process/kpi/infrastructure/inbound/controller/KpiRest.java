package pe.farmaciasperuanas.digital.process.kpi.infrastructure.inbound.controller;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import pe.farmaciasperuanas.digital.process.kpi.application.service.KpiServiceImpl;
import pe.farmaciasperuanas.digital.process.kpi.domain.entity.Kpi;
import pe.farmaciasperuanas.digital.process.kpi.domain.port.repository.KpiRepository;
import pe.farmaciasperuanas.digital.process.kpi.infrastructure.outbound.adapter.KpiRepositoryImpl;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

/**
 * Controlador principal que expone el servicio a trav&eacute;s de HTTP/Rest para
 * las operaciones del recurso Kpi<br/>
 * <b>Class</b>: KpiController<br/>
 * <b>Copyright</b>: 2025 Farmacias Peruanas.<br/>
 * <b>Company</b>:Farmacias Peruanas.<br/>
 *
 * <u>Developed by</u>: <br/>
 * <ul>
 * <li>Jorge Triana</li>
 * </ul>
 * <u>Changes</u>:<br/>
 * <ul>
 * <li>Feb 28, 2025 Creaci&oacute;n de Clase.</li>
 * </ul>
 * @version 1.0
 */
@Slf4j
@RestController
@RequestMapping("/api/kpi")
public class KpiRest {

  @Autowired
  private KpiServiceImpl kpiService;
    @Autowired
    private KpiRepository kpiRepository;

  @GetMapping(value = {"/generate"})
  public Mono<Map<String, Object>> generateKpi() {
    return kpiService.generateKpiFromSalesforceData();
  }

  @GetMapping(value = {"/complete-kpis"})
  public Mono<Map<String, Object>> completeKpis() {
    log.info("Solicitud para completar KPIs para todas las campañas");
    return ((KpiRepositoryImpl)kpiRepository).verifyAndCompleteKpis()
            .then(Mono.just(new HashMap() {{
              put("message", "Verificación y completado de KPIs finalizado");
              put("status", "success");
            }}));
  }
}