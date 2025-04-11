package pe.farmaciasperuanas.digital.process.kpi.domain.port.service;

import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * Interface for running Spring Boot framework.<br/>
 * <b>Class</b>: Application<br/>
 * <b>Copyright</b>: &copy; 2025 Digital.<br/>
 * <b>Company</b>: Digital.<br/>
 *

 * <u>Developed by</u>: <br/>
 * <ul>
 * <li>Jorge Triana</li>
 * </ul>
 * <u>Base interface</u>:<br/>
 * <ul>
 * <li>Feb 28, 2025 KpiService Interface.</li>
 * </ul>
 * @version 1.0
 */

public interface KpiService {
    Mono<Map<String, Object>> generateKpiFromSalesforceData();
}


