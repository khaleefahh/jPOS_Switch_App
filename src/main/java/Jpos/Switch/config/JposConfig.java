package Jpos.Switch.config;

import org.jpos.q2.Q2;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JposConfig {

    @Bean
    public Q2 q2() {
        Q2 q2 = new Q2();
        q2.start();
        return q2;
    }
}
