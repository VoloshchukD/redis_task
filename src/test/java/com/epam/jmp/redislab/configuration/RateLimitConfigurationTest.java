package com.epam.jmp.redislab.configuration;

import com.epam.jmp.redislab.configuration.ratelimit.RateLimitRule;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitTimeInterval;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;


public class RateLimitConfigurationTest {

    @Test
    public void testConfigurationParsing() throws IOException {
        RateLimitConfiguration rateLimitConfiguration = new RateLimitConfiguration();
        Set<RateLimitRule> rateLimitRules = rateLimitConfiguration.rateLimitRules();

        Set<RateLimitRule> expectedRules = new HashSet<>();
        expectedRules.add(new RateLimitRule(Optional.of(""), Optional.empty(), Optional.empty(), 2, RateLimitTimeInterval.MINUTE));
        expectedRules.add(new RateLimitRule(Optional.of(""), Optional.empty(), Optional.of("SLOW"), 1, RateLimitTimeInterval.MINUTE));
        expectedRules.add(new RateLimitRule(Optional.of("ImportantCustomerId"), Optional.empty(), Optional.empty(), 10, RateLimitTimeInterval.MINUTE));
        expectedRules.add(new RateLimitRule(Optional.empty(), Optional.of("192.168.100.150"), Optional.empty(), 1, RateLimitTimeInterval.HOUR));

        // We check for inclusion of set here to allow add non-default Rate Limit Rules
        assertTrue(rateLimitRules.containsAll(expectedRules));
    }

}