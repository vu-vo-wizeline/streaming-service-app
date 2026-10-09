/*
 * Copyright (c) 2024  Vladimir Marianciuc. All Rights Reserved.
 *
 * Project: STREAMING SERVICE APP
 * File: TransactionsController.java
 *
 */

package io.github.marianciuc.streamingservice.payment.controller;

import io.github.marianciuc.streamingservice.payment.dto.common.TransactionDto;
import io.github.marianciuc.streamingservice.payment.enums.PaymentStatus;
import io.github.marianciuc.streamingservice.payment.service.TransactionService;
import io.github.marianciuc.streamingservice.payment.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/payments/transactions")
@RequiredArgsConstructor
public class TransactionsController {
    private final TransactionService transactionService;
    private final UserService userService;

    @GetMapping
    public ResponseEntity<List<TransactionDto>> getTransactions(
            @RequestParam(value = "page", defaultValue = "0") Integer page,
            @RequestParam(value = "size", defaultValue = "10") Integer size,
            @RequestParam(value = "sort", defaultValue = "asc") String sort,
            @RequestParam(value = "status", required = false) PaymentStatus status,
            @RequestParam(value = "userId", required = false) UUID userId
    ) {
        UUID authenticatedUserId = userService.extractUserIdFromAuth();
        UUID idToFetch = (userId != null) ? userId : authenticatedUserId;
        
        // AUTHORIZATION CHECK: Deny access if user is not admin and userId != their own ID
        if (!userService.hasAdminRoles() && !idToFetch.equals(authenticatedUserId)) {
            throw new AccessDeniedException("You do not have permission to view these transactions");
        }
        
        return ResponseEntity.ok(transactionService.getTransactions(page, size, sort, status, idToFetch));
    }
}
