package com.epam.jmp.redislab.service;

import com.epam.jmp.redislab.api.RequestDescriptor;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitRule;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitTimeInterval;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class JedisRateLimitService implements RateLimitService {

    private static final String REDIS_KEY_PREFIX = "request_count:";
    public static final String EMPTY = "-empty-";
    public static final String DELIMITER = ":";
    public static final String ACCOUNT_ID = "accountId";
    public static final String CLIENT_IP = "clientIp";
    public static final String REQUEST_TYPE = "requestType";
    public static final String ALLOWED_NUMBER_OF_REQUESTS = "allowedNumberOfRequests";
    public static final String TIME_INTERVAL = "timeInterval";
    public static final String ALL_ACCOUNTS = "allAccounts";

    @Autowired
    private StatefulRedisClusterConnection<String, String> lettuceRedisClusterConnection;

    @Autowired
    private Set<RateLimitRule> rateLimitRules;

    @PostConstruct
    public void saveRules() {
        saveRateLimitRules();
    }

    private void saveRateLimitRules() {
        RedisAdvancedClusterCommands<String, String> syncCommands = lettuceRedisClusterConnection.sync();

        for (RateLimitRule rule : rateLimitRules) {
            String accountId = rule.getAccountId().orElse(EMPTY);
            String clientIp = rule.getClientIp().orElse(EMPTY);
            String requestType = rule.getRequestType().orElse(EMPTY);
            Integer allowedNumberOfRequests = rule.getAllowedNumberOfRequests();
            RateLimitTimeInterval timeInterval = rule.getTimeInterval();

            String key = buildKeyString(accountId, clientIp, requestType);

            Map<String, String> rules = buildRuleMap(accountId, clientIp, requestType, allowedNumberOfRequests, timeInterval);

            syncCommands.hmset(key, rules);

            if (accountId.isEmpty() && EMPTY.equals(requestType)) {
                syncCommands.hmset(ALL_ACCOUNTS, rules);
            }
            if (!EMPTY.equals(requestType)) {
                syncCommands.hmset(requestType, rules);
            }
        }
    }

    @Override
    public Mono<Boolean> shouldLimit(Set<RequestDescriptor> requestDescriptors) {
        RedisAdvancedClusterAsyncCommands<String, String> asyncCommands = lettuceRedisClusterConnection.async();

        return Flux.fromIterable(requestDescriptors)
                .flatMap(descriptor -> {
                    String descriptorAccountId = descriptor.getAccountId().orElse(EMPTY);
                    String descriptorClientIp = descriptor.getClientIp().orElse(EMPTY);
                    String descriptorRequestType = descriptor.getRequestType().orElse(EMPTY);

                    return findRequestRuleReactive(descriptorAccountId, descriptorClientIp, descriptorRequestType)
                            .map(values -> new AbstractMap.SimpleEntry<>(descriptor, values));
                })
                .flatMap(requestRule -> {
                    RequestDescriptor descriptor = requestRule.getKey();
                    Map<String, String> values = requestRule.getValue();

                    if (!values.isEmpty()) {
                        int allowedNumberOfRequests = Integer.parseInt(values.get(ALLOWED_NUMBER_OF_REQUESTS));
                        String timeInterval = values.get(TIME_INTERVAL);

                        String rateCountKey = initRateCountKey(descriptor.getAccountId().orElse(EMPTY), descriptor.getClientIp().orElse(EMPTY), descriptor.getRequestType().orElse(EMPTY));

                        return Mono.fromFuture(() -> asyncCommands.incr(rateCountKey).toCompletableFuture())
                                .flatMap(count -> {
                                    if (count == 1L) {
                                        return Mono.fromFuture(() -> asyncCommands.expire(rateCountKey, getTimeValueInSeconds(timeInterval)).toCompletableFuture())
                                                .map(value -> count);
                                    } else {
                                        return Mono.just(count);
                                    }
                                })
                                .map(count -> count > allowedNumberOfRequests);
                    } else {
                        return Mono.just(false);
                    }
                })
                .reduce(Boolean::logicalOr)
                .defaultIfEmpty(false);

    }

    private Mono<Map<String, String>> findRequestRuleReactive(String descriptorAccountId, String descriptorClientIp, String descriptorRequestType) {
        String key = buildKeyString(descriptorAccountId, descriptorClientIp, descriptorRequestType);
        RedisAdvancedClusterAsyncCommands<String, String> asyncCommands = lettuceRedisClusterConnection.async();

        return Flux.fromIterable(Arrays.asList(key, descriptorRequestType, ALL_ACCOUNTS))
                .flatMap(possibleKey -> Mono.fromFuture(() -> asyncCommands.hgetall(possibleKey).toCompletableFuture()))
                .filter(values -> !values.isEmpty())
                .next()
                .switchIfEmpty(Mono.just(Collections.emptyMap()));
    }

    private Map<String, String> buildRuleMap(String accountId, String clientIp, String requestType, Integer allowedNumberOfRequests, RateLimitTimeInterval timeInterval) {
        Map<String, String> rules = new HashMap<>();
        rules.put(ACCOUNT_ID, accountId);
        rules.put(CLIENT_IP, clientIp);
        rules.put(REQUEST_TYPE, requestType);
        rules.put(ALLOWED_NUMBER_OF_REQUESTS, String.valueOf(allowedNumberOfRequests));
        rules.put(TIME_INTERVAL, timeInterval.name());
        return rules;
    }

    private String buildKeyString(String accountId, String clientIp, String requestType) {
        return Stream.of(accountId, clientIp, requestType)
                .filter(field -> !EMPTY.equals(field))
                .collect(Collectors.joining(DELIMITER));
    }


    private int getTimeValueInSeconds(String timeInterval) {
        int timeValueInSeconds = 0;
        if (timeInterval.equals(RateLimitTimeInterval.MINUTE.name())) {
            timeValueInSeconds = 60;
        } else if (timeInterval.equals(RateLimitTimeInterval.HOUR.name())) {
            timeValueInSeconds = 3600;
        }
        return timeValueInSeconds;
    }

    private String initRateCountKey(String descriptorAccountId,
                                    String descriptorClientIp, String descriptorRequestType) {
        return REDIS_KEY_PREFIX + descriptorAccountId
                + DELIMITER + descriptorClientIp
                + DELIMITER + descriptorRequestType;
    }

}
