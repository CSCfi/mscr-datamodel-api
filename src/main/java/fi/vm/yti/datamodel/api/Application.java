package fi.vm.yti.datamodel.api;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.servers.Server;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "fi.vm.yti.datamodel.api")
@EnableScheduling
@OpenAPIDefinition(
        info = @Info(
                description = "MSCR Service",
                version = "1.0",
                title = "MSCR Service",
                termsOfService = "https://opensource.org/licenses/EUPL-1.1",
                contact = @Contact(
                        name = "CSC"                        
                ),
                license = @License(
                        name = "EUPL-1.2",
                        url = "https://opensource.org/licenses/EUPL-1.1"
                )
        ),
        servers = {
                @Server(
                        description = "Datamodel API",
                        url = "/datamodel-api")
        }
)
public class Application {

    public static void main(String[] args) {
        System.setProperty("jdk.httpclient.redirects.retrylimit", "15");
        SpringApplication.run(Application.class, args);
    }
}
