package io.github.marianciuc.streamingservice.payment.config;


import com.nimbusds.jose.JWSVerifier;
import io.github.marianciuc.streamingservice.payment.security.AccessJWETokenStringDeserializer;
import io.github.marianciuc.streamingservice.payment.security.JWTFilter;
import lombok.RequiredArgsConstructor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.csrf.CsrfFilter;

@EnableWebSecurity
@RequiredArgsConstructor
@Configuration
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http, JWSVerifier verifier) throws Exception {
        http
                .cors(AbstractHttpConfigurer::disable)
                .csrf(csrf -> csrf.ignoringRequestMatchers("/api/v1/payments/webhook"))
                .authorizeHttpRequests(auth -> auth.requestMatchers("/error").permitAll().anyRequest().authenticated())
                .addFilterBefore(new JWTFilter(new AccessJWETokenStringDeserializer(verifier)),
                        CsrfFilter.class)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS));
        return http.build();
    }

    @Bean
    public AuthenticationManager authenticationManager(AuthenticationConfiguration authenticationConfiguration) throws Exception {
        return authenticationConfiguration.getAuthenticationManager();
    }
}
