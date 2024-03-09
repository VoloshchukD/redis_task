package com.epam.jmp.redislab.service;

import com.epam.jmp.redislab.api.RequestDescriptor;
import reactor.core.publisher.Mono;

import java.util.Set;

public interface RateLimitService {

    Mono<Boolean> shouldLimit(Set<RequestDescriptor> requestDescriptors);

}
