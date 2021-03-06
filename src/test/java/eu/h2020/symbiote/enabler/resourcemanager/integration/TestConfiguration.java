package eu.h2020.symbiote.enabler.resourcemanager.integration;

import eu.h2020.symbiote.enabler.resourcemanager.utils.AuthorizationManager;

import org.mockito.Mockito;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.web.client.RestTemplate;

/**
 * @author Vasileios Glykantzis (ICOM)
 * @since 9/17/2017.
 */
@Profile("test")
@Configuration
public class TestConfiguration {

    @Bean
    @Primary
    public AuthorizationManager authorizationManager() {
        return Mockito.mock(AuthorizationManager.class);
    }

    @Bean
    @Primary
    public RestTemplate restTemplate() { return Mockito.mock(RestTemplate.class); }
}
