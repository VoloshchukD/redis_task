package com.epam.jmp.redislab.configuration;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.JedisCluster;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
/**
 *  This is example of integration test, which uses test containers to create Redis Cluster.
 *  This test basically check that Redis Cluster is properly configured with Docker Compose and
 *  accessible from host network.
 */
public class RedisClusterConnectionTest {

    @Container
    public static DockerComposeContainer environment =
            new DockerComposeContainer(new File("src/test/resources/redis/docker-compose.yaml"))
                         //this regexp identifies log entry for successfully cluster creation
                        .waitingFor("redis-init-cluster", Wait.forLogMessage(".*All 16384 slots covered.*", 1));
    @Autowired
    private JedisCluster jedisCluster;

    @Autowired
    private StatefulRedisClusterConnection<String,String> lettuceClusterConnection;

    @Test
    public void testJedisConnectionToCluster() {
        jedisCluster.set("test:test:jedis","testvalue");
        String result = jedisCluster.get("test:test:jedis");
        assertEquals("testvalue", result);
    }

    @Test
    public void testLettuceConnectionToCluster() {
        RedisAdvancedClusterCommands commands = lettuceClusterConnection.sync();
        commands.set("test:test:lettuce-sync", "testvalue");
        String result = jedisCluster.get("test:test:lettuce-sync");
        assertEquals("testvalue", result);
    }



}
