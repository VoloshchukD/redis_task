package com.epam.jmp.redislab.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Set;

public class RateLimitRequest {

    private final Set<RequestDescriptor> descriptors;

    public RateLimitRequest(@JsonProperty("descriptors") Set<RequestDescriptor> descriptors) {
        this.descriptors = descriptors;
    }

    public Set<RequestDescriptor> getDescriptors() {
        return descriptors;
    }
}
