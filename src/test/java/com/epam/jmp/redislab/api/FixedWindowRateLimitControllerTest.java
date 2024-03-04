package com.epam.jmp.redislab.api;

import com.epam.jmp.redislab.utils.RateLimitResponseStats;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class FixedWindowRateLimitControllerTest {

    @Container
    public static DockerComposeContainer environment =
            new DockerComposeContainer(new File("src/test/resources/redis/docker-compose.yaml"))
                    //this regexp identifies log entry for successfully cluster creation
                    .waitingFor("redis-init-cluster", Wait.forLogMessage(".*All 16384 slots covered.*", 1));

    private final static String IMPORTANT_CUSTOMER_ID = "ImportantCustomerId";

    private final RestTemplate restTemplate = new RestTemplate();

    @BeforeEach
    void setup() {
        restTemplate.setErrorHandler(new ResponseErrorHandler() {
            @Override
            public boolean hasError(ClientHttpResponse response) throws IOException {
                return false;
            }

            @Override
            public void handleError(ClientHttpResponse response) throws IOException {
            }
        });
    }

    @Value("http://localhost:${local.server.port}/api/v1/ratelimit/fixedwindow")
    private String apiUrl;

    // 2 requests per minute per accountId rule
    @Test
    public void testGenericAccountIdRule() throws InterruptedException {

        RequestDescriptor requestDescriptor = RequestDescriptor.of("1", null, null);
        // Fixed window rate limit worst case: 2 requests arrived from 10:40:59 to 10:41:00 and was counted in 10:40-10:41 bucket,
        // other requests arrived from 10:41:00 to 10:41:59 and was counted in 10:41-10:42 bucket.
        // So we need to send at least 5 requests to be sure that at least one is blocked and at least 2 pass.
        // We assume that 5 requests will be processed faster than in 1 minute.
        // For fixed window rate limit common rule is to send 2N+1 requests, where N is the max allowed number of requests.
        RateLimitResponseStats stats = this.sendRatelimitRequests(5, requestDescriptor);
        assertTrue(stats.getTooManyRequestsCount() >= 1);
        assertTrue(stats.getOkCount() >= 2);
    }


    @Test
    public void testSpecificAccountIdRuleTakePrecedenceOverGeneric() throws InterruptedException {
        RequestDescriptor requestDescriptor = RequestDescriptor.of(IMPORTANT_CUSTOMER_ID, null, null);
        RateLimitResponseStats stats = this.sendRatelimitRequests(21, requestDescriptor);
        assertTrue(stats.getTooManyRequestsCount() >= 1);
        assertTrue(stats.getOkCount() >= 10);
    }


    // 2 requests per minute per account Id
    //   AND
    // 1 Slow request per account per minute
    @Test
    public void testMultipleRuleMatch() throws InterruptedException {
        RateLimitResponseStats stats = this.sendRatelimitRequests(5,
                RequestDescriptor.of("2", null, "SLOW"),
                RequestDescriptor.of("2", null, null)
        );
        assertTrue(stats.getTooManyRequestsCount() >= 3);
        assertTrue(stats.getOkCount() <= 2);
    }

    // 2 requests per minute per accountId rule + one minute pause
    @Test
    public void testRatelimitIsResetInOneMinute() throws InterruptedException {
        RequestDescriptor requestDescriptor = RequestDescriptor.of("3", null, null);
        RateLimitResponseStats stats = this.sendRatelimitRequests(5, requestDescriptor);
        //Checking that requests were blocked
        assertTrue(stats.getTooManyRequestsCount() >= 1);
        assertTrue(stats.getOkCount() >= 2);

        //Waiting one minute to reset request count
        Thread.sleep(60000);

        //Sending maxim allowed number of request
        RateLimitResponseStats statsForNextWindow = this.sendRatelimitRequests(2, requestDescriptor);
        // Checking that no request was blocked
        assertEquals(2, statsForNextWindow.getOkCount());
    }

    // 1 request for 192.168.100.150 per HOUR
    // You can temporary disable this test with @Disabled as this test requires for 3 mins of wall clock time to check.
    @Test
//     @Disabled
    public void testPerHourRateLimit() throws InterruptedException {
        RateLimitResponseStats stats = this.sendRatelimitRequests(3, 60 * 1000,
                RequestDescriptor.of(null, "192.168.100.150", null));
        assertTrue(stats.getTooManyRequestsCount() >= 1);
        assertTrue(stats.getOkCount() >= 1);
    }

    private RateLimitResponseStats sendRatelimitRequests(int numberOfRequests, RequestDescriptor... descriptors) throws InterruptedException {
        return this.sendRatelimitRequests(numberOfRequests, 0, descriptors);
    }

    private RateLimitResponseStats sendRatelimitRequests(int numberOfRequests, int delayInMillis, RequestDescriptor... descriptors) throws InterruptedException {
        RateLimitRequest request = new RateLimitRequest(new HashSet<>(Arrays.asList(descriptors)));
        RateLimitResponseStats.Builder statsBuilder = new RateLimitResponseStats.Builder();
        for (int i = 0; i < numberOfRequests; i++) {
            ResponseEntity<Void> response = restTemplate.postForEntity(this.apiUrl, request, Void.class);
            if (delayInMillis > 0) {
                Thread.sleep(delayInMillis);
            }
            switch (response.getStatusCode()) {
                case OK: {
                    statsBuilder.add200Request();
                    continue;
                }
                case TOO_MANY_REQUESTS: {
                    statsBuilder.add429Request();
                    continue;
                }
                default: {
                    fail("Unexpected response code " + response.getStatusCode().value());
                }
            }
        }
        return statsBuilder.build();
    }

}
