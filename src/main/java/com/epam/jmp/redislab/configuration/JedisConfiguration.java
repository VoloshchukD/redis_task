package com.epam.jmp.redislab.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.server.MediaTypeNotSupportedStatusException;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

@Configuration
public class JedisConfiguration {

    @Bean
    // https://www.baeldung.com/jedis-java-redis-client-library
    // https://stackoverflow.com/questions/30078034/redis-cluster-in-multiple-threads
    public JedisCluster jedisCluster(){
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort("127.0.0.1", 30000));
        JedisCluster jedis = new JedisCluster(jedisClusterNodes);
        return jedis;
    }

}
