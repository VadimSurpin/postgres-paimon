// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import pro.surpin.pgpaimon.writer.dto.*;

/** Spring Boot entry point for the standalone replication writer. */
@SpringBootApplication(proxyBeanMethods = false)
public class Main {

  static void main(
      String[] args
  ) {
    SpringApplication application = new SpringApplication(Main.class);
    application.setWebApplicationType(WebApplicationType.NONE);
    try (ConfigurableApplicationContext context = application.run(args)) {
      // The runner blocks until replication stops. Spring owns resource cleanup.
    }
  }
}
