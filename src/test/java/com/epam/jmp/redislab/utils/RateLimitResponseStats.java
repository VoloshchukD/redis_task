package com.epam.jmp.redislab.utils;

import com.epam.jmp.redislab.api.RateLimitRequest;

public class RateLimitResponseStats {

    private final int okCount;
    private final int tooManyRequestsCount;

    private RateLimitResponseStats(int okCount, int tooManyRequestsCount) {
        this.okCount = okCount;
        this.tooManyRequestsCount = tooManyRequestsCount;
    }

    public int getOkCount() {
        return okCount;
    }

    public int getTooManyRequestsCount() {
        return tooManyRequestsCount;
    }

    public static class Builder {

        private int okCount = 0;
        private int tooManyRequestsCount = 0;

        public void add200Request() {
            this.okCount++;
        }

        public void add429Request() {
            this.tooManyRequestsCount++;
        }

        public RateLimitResponseStats build() {
            return new RateLimitResponseStats(this.okCount, this.tooManyRequestsCount);
        }

    }

}
