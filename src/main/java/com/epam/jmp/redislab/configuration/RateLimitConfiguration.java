package com.epam.jmp.redislab.configuration;

import com.epam.jmp.redislab.configuration.ratelimit.RateLimitRule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Configuration
public class RateLimitConfiguration {

    private final ObjectMapper objectMapper;

    public RateLimitConfiguration() {
        this.objectMapper = new ObjectMapper(new YAMLFactory());
        this.objectMapper.registerModule(new Jdk8Module());
    }

    @Bean
    public Set<RateLimitRule> rateLimitRules() throws IOException {
        ClassPathResource ratelimitConfigurationResource = new ClassPathResource("ratelimitRules.yaml");
        InputStream inputStream = ratelimitConfigurationResource.getInputStream();
        return new HashSet<>(Arrays.asList(objectMapper.readValue(inputStream, RateLimitRule[].class)));
    }

}
