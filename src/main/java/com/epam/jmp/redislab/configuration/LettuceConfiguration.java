package com.epam.jmp.redislab.configuration;

import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LettuceConfiguration {

    @Bean
    //https://lettuce.io/core/release/reference/#_connection_pooling
    public StatefulRedisClusterConnection<String,String> lettuceRedisClusterConnection(){
        RedisClusterClient redisClient = RedisClusterClient.create("redis://redis-master-0:30000");
        StatefulRedisClusterConnection<String, String> connection = redisClient.connect();
        return connection;
    }

}
