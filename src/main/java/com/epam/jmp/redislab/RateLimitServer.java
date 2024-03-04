package com.epam.jmp.redislab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class RateLimitServer {

    public static void main(String ...args){
        SpringApplication.run(RateLimitServer.class);
    }

}
