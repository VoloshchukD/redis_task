package com.epam.jmp.redislab.service;

import com.epam.jmp.redislab.api.RequestDescriptor;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitRule;
import com.epam.jmp.redislab.configuration.ratelimit.RateLimitTimeInterval;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import redis.clients.jedis.JedisCluster;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
    private JedisCluster jedisCluster;

    @Autowired
    private Set<RateLimitRule> rateLimitRules;

    @PostConstruct
    public void saveRules() {
        saveRateLimitRules();
    }

    private void saveRateLimitRules() {
        for (RateLimitRule rule : rateLimitRules) {
            String accountId = rule.getAccountId().orElse(EMPTY);
            String clientIp = rule.getClientIp().orElse(EMPTY);
            String requestType = rule.getRequestType().orElse(EMPTY);
            Integer allowedNumberOfRequests = rule.getAllowedNumberOfRequests();
            RateLimitTimeInterval timeInterval = rule.getTimeInterval();

            String key = buildKeyString(accountId, clientIp, requestType);

            Map<String, String> rules = buildRuleMap(accountId, clientIp,
                    requestType, allowedNumberOfRequests, timeInterval);

            jedisCluster.hmset(key, rules);
            if (accountId.isEmpty() && EMPTY.equals(requestType)) {
                jedisCluster.hmset(ALL_ACCOUNTS, rules);
            }
            if (!EMPTY.equals(requestType)) {
                jedisCluster.hmset(requestType, rules);
            }
        }
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


    @Override
    public boolean shouldLimit(Set<RequestDescriptor> requestDescriptors) {
        boolean isRateLimitReached = false;
        for (RequestDescriptor descriptor : requestDescriptors) {
            String descriptorAccountId = descriptor.getAccountId().orElse(EMPTY);
            String descriptorClientIp = descriptor.getClientIp().orElse(EMPTY);
            String descriptorRequestType = descriptor.getRequestType().orElse(EMPTY);

            Map<String, String> values = findRequestRule(descriptorAccountId,
                    descriptorClientIp, descriptorRequestType);

            if (!CollectionUtils.isEmpty(values)) {
                int allowedNumberOfRequests = Integer.parseInt(values.get(ALLOWED_NUMBER_OF_REQUESTS));
                String timeInterval = values.get(TIME_INTERVAL);

                String rateCountKey = initRateCountKey(descriptorAccountId,
                        descriptorClientIp, descriptorRequestType);
                long count = jedisCluster.incr(rateCountKey);
                if (count == 1) {
                    jedisCluster.expire(rateCountKey, getTimeValueInSeconds(timeInterval));
                }
                isRateLimitReached = count > allowedNumberOfRequests;
            }
        }
        return isRateLimitReached;
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

    private Map<String, String> findRequestRule(String descriptorAccountId,
                                                String descriptorClientIp, String descriptorRequestType) {
        String key = buildKeyString(descriptorAccountId, descriptorClientIp, descriptorRequestType);
        Map<String, String> values = null;
        for (String possibleKey : Arrays.asList(key, descriptorRequestType, ALL_ACCOUNTS)) {
            values = jedisCluster.hgetAll(possibleKey);
            if (!CollectionUtils.isEmpty(values)) {
                break;
            }
        }
        return values;
    }
}
