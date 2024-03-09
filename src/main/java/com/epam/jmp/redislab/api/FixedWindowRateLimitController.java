package com.epam.jmp.redislab.api;

import com.epam.jmp.redislab.service.RateLimitService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.function.ServerResponse;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/ratelimit/fixedwindow")
public class FixedWindowRateLimitController {

    private final RateLimitService rateLimitService;

    public FixedWindowRateLimitController(RateLimitService rateLimitService) {
        this.rateLimitService = rateLimitService;
    }

//    @PostMapping
//    public ResponseEntity<Void> shouldRateLimit(@RequestBody RateLimitRequest rateLimitRequest) {
//        if (rateLimitService.shouldLimit(rateLimitRequest.getDescriptors())) {
//            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build();
//        }
//        return ResponseEntity.ok().build();
//    }

    @PostMapping
    public Mono<ResponseEntity<Void>> shouldRateLimit(@RequestBody RateLimitRequest rateLimitRequest) {
        return rateLimitService.shouldLimit(rateLimitRequest.getDescriptors())
                .map(isRateLimited -> {
                    if (isRateLimited) {
                        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build();
                    } else {
                        return ResponseEntity.ok().build();
                    }
                });
    }

}
