package pe.farmaciasperuanas.digital.process.kpi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main class for running Spring Boot framework.<br/>
 * <b>Class</b>: Application<br/>
 * <b>Copyright</b>: 2025 Farmacias Peruanas.<br/>
 * <b>Company</b>: Farmacias Peruanas.<br/>
 *

 * <u>Developed by</u>: <br/>
 * <ul>
 * <li>Jorge Triana</li>
 * </ul>
 * <u>Changes</u>:<br/>
 * <ul>
 * <li>Feb 28, 2025 KpiApplication Class</li>
 * </ul>
 * @version 1.0
 */

@SpringBootApplication
@EnableScheduling
public class KpiApplication {

  /**
   * Main method.
   */
  public static void main(String[] args) {
    new SpringApplication(KpiApplication.class).run(args);
  }
}
