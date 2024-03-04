package com.epam.jmp.redislab.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

public class RequestDescriptor {

    private final Optional<String> accountId;

    private final Optional<String> clientIp;

    private final Optional<String> requestType;

    public static RequestDescriptor of(String accountId, String clientIp, String requestType) {
        return new RequestDescriptor(Optional.ofNullable(accountId), Optional.ofNullable(clientIp), Optional.ofNullable(requestType));
    }
    @JsonCreator
    public RequestDescriptor(@JsonProperty("accountId") Optional<String> accountId,
                             @JsonProperty("clientIp") Optional<String> clientIp,
                             @JsonProperty("requestType") Optional<String> requestType) {
        this.accountId = accountId;
        this.clientIp = clientIp;
        this.requestType = requestType;
    }

    public Optional<String> getAccountId() {
        return accountId;
    }

    public Optional<String> getClientIp() {
        return clientIp;
    }

    public Optional<String> getRequestType() {
        return requestType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestDescriptor that = (RequestDescriptor) o;
        return accountId.equals(that.accountId)
                && clientIp.equals(that.clientIp)
                && requestType.equals(that.requestType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountId, clientIp, requestType);
    }
}
