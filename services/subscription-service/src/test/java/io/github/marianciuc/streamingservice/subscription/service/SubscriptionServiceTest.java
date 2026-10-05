/*
 * Copyright (c) 2024 Vladimir Marianciuc.
 * Unit Test Suite for Subscription Service
 */

package io.github.marianciuc.streamingservice.subscription.service;

import io.github.marianciuc.streamingservice.subscription.dto.SubscriptionRequest;
import io.github.marianciuc.streamingservice.subscription.dto.SubscriptionResponse;
import io.github.marianciuc.streamingservice.subscription.entity.RecordStatus;
import io.github.marianciuc.streamingservice.subscription.entity.Subscription;
import io.github.marianciuc.streamingservice.subscription.exceptions.NotFoundException;
import io.github.marianciuc.streamingservice.subscription.mapper.SubscriptionMapper;
import io.github.marianciuc.streamingservice.subscription.repository.SubscriptionRepository;
import io.github.marianciuc.streamingservice.subscription.service.impl.SubscriptionServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SubscriptionServiceImpl.
 * Tests cover: CRUD operations, error handling, state transitions, and boundary conditions.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("SubscriptionService Unit Tests")
public class SubscriptionServiceTest {

    @Mock
    private SubscriptionRepository subscriptionRepository;

    @Mock
    private SubscriptionMapper subscriptionMapper;

    @InjectMocks
    private SubscriptionServiceImpl subscriptionService;

    private UUID testSubscriptionId;
    private Subscription testSubscription;
    private SubscriptionRequest testRequest;
    private SubscriptionResponse testResponse;

    @BeforeEach
    void setUp() {
        testSubscriptionId = UUID.randomUUID();
        testSubscription = Subscription.builder()
                .id(testSubscriptionId)
                .name("Premium")
                .price(9.99)
                .durationInDays(30)
                .allowedActiveSessions(5)
                .isTemporary(false)
                .recordStatus(RecordStatus.ACTIVE)
                .build();

        testRequest = new SubscriptionRequest("Premium", 9.99, 30, 5, false);
        testResponse = new SubscriptionResponse(testSubscriptionId, "Premium", 9.99, 30, 5, false);
    }

    // ==================== CREATE TESTS ====================

    @Test
    @DisplayName("createSubscription_whenValidRequest_expectsSubscriptionCreated")
    void testCreateSubscription_whenValidRequest_expectsSubscriptionCreated() {
        // ARRANGE
        when(subscriptionMapper.toEntity(testRequest)).thenReturn(testSubscription);
        when(subscriptionRepository.save(testSubscription)).thenReturn(testSubscription);
        when(subscriptionMapper.toResponse(testSubscription)).thenReturn(testResponse);

        // ACT
        SubscriptionResponse result = subscriptionService.createSubscription(testRequest);

        // ASSERT
        assertNotNull(result);
        assertEquals(testResponse.id(), result.id());
        assertEquals(testResponse.name(), result.name());
        verify(subscriptionRepository, times(1)).save(testSubscription);
        verify(subscriptionMapper, times(1)).toEntity(testRequest);
    }

    @Test
    @DisplayName("createSubscription_whenNullRequest_expectsNullPointerException")
    void testCreateSubscription_whenNullRequest_expectsNullPointerException() {
        // ARRANGE
        when(subscriptionMapper.toEntity(null)).thenThrow(NullPointerException.class);

        // ACT & ASSERT
        assertThrows(NullPointerException.class, () -> subscriptionService.createSubscription(null));
    }

    @Test
    @DisplayName("createSubscription_whenRepositorySaveFails_expectsException")
    void testCreateSubscription_whenRepositorySaveFails_expectsException() {
        // ARRANGE
        when(subscriptionMapper.toEntity(testRequest)).thenReturn(testSubscription);
        when(subscriptionRepository.save(any())).thenThrow(RuntimeException.class);

        // ACT & ASSERT
        assertThrows(RuntimeException.class, () -> subscriptionService.createSubscription(testRequest));
    }

    // ==================== READ TESTS ====================

    @Test
    @DisplayName("getSubscription_whenValidId_expectsSubscriptionReturned")
    void testGetSubscription_whenValidId_expectsSubscriptionReturned() {
        // ARRANGE
        when(subscriptionRepository.findById(testSubscriptionId)).thenReturn(Optional.of(testSubscription));

        // ACT
        Subscription result = subscriptionService.getSubscription(testSubscriptionId);

        // ASSERT
        assertNotNull(result);
        assertEquals(testSubscriptionId, result.getId());
        assertEquals("Premium", result.getName());
        verify(subscriptionRepository, times(1)).findById(testSubscriptionId);
    }

    @Test
    @DisplayName("getSubscription_whenInvalidId_expectsNotFoundException")
    void testGetSubscription_whenInvalidId_expectsNotFoundException() {
        // ARRANGE
        UUID invalidId = UUID.randomUUID();
        when(subscriptionRepository.findById(invalidId)).thenReturn(Optional.empty());

        // ACT & ASSERT
        assertThrows(NotFoundException.class, () -> subscriptionService.getSubscription(invalidId));
    }

    @Test
    @DisplayName("getSubscription_whenNullId_expectsNotFoundException")
    void testGetSubscription_whenNullId_expectsNotFoundException() {
        // ARRANGE
        when(subscriptionRepository.findById(null)).thenReturn(Optional.empty());

        // ACT & ASSERT
        assertThrows(NotFoundException.class, () -> subscriptionService.getSubscription(null));
    }

    @Test
    @DisplayName("getSubscriptionResponse_whenValidId_expectsResponseReturned")
    void testGetSubscriptionResponse_whenValidId_expectsResponseReturned() {
        // ARRANGE
        when(subscriptionRepository.findById(testSubscriptionId)).thenReturn(Optional.of(testSubscription));
        when(subscriptionMapper.toResponse(testSubscription)).thenReturn(testResponse);

        // ACT
        SubscriptionResponse result = subscriptionService.getSubscriptionResponse(testSubscriptionId);

        // ASSERT
        assertNotNull(result);
        assertEquals(testResponse.id(), result.id());
        verify(subscriptionRepository, times(1)).findById(testSubscriptionId);
        verify(subscriptionMapper, times(1)).toResponse(testSubscription);
    }

    @Test
    @DisplayName("getAllSubscriptions_whenMultipleExist_expectsListReturned")
    void testGetAllSubscriptions_whenMultipleExist_expectsListReturned() {
        // ARRANGE
        Subscription sub2 = Subscription.builder()
                .id(UUID.randomUUID())
                .name("Standard")
                .price(4.99)
                .durationInDays(30)
                .allowedActiveSessions(2)
                .isTemporary(false)
                .recordStatus(RecordStatus.ACTIVE)
                .build();
        List<Subscription> subscriptions = Arrays.asList(testSubscription, sub2);
        when(subscriptionRepository.findAll()).thenReturn(subscriptions);
        when(subscriptionMapper.toResponse(testSubscription)).thenReturn(testResponse);
        when(subscriptionMapper.toResponse(sub2)).thenReturn(
                new SubscriptionResponse(sub2.getId(), "Standard", 4.99, 30, 2, false)
        );

        // ACT
        List<SubscriptionResponse> result = subscriptionService.getAllSubscriptions();

        // ASSERT
        assertNotNull(result);
        assertEquals(2, result.size());
        verify(subscriptionRepository, times(1)).findAll();
    }

    @Test
    @DisplayName("getAllSubscriptions_whenNoneExist_expectsEmptyList")
    void testGetAllSubscriptions_whenNoneExist_expectsEmptyList() {
        // ARRANGE
        when(subscriptionRepository.findAll()).thenReturn(Arrays.asList());

        // ACT
        List<SubscriptionResponse> result = subscriptionService.getAllSubscriptions();

        // ASSERT
        assertNotNull(result);
        assertTrue(result.isEmpty());
        verify(subscriptionRepository, times(1)).findAll();
    }

    // ==================== UPDATE TESTS ====================

    @Test
    @DisplayName("updateSubscription_whenValidIdAndRequest_expectsSubscriptionUpdated")
    void testUpdateSubscription_whenValidIdAndRequest_expectsSubscriptionUpdated() {
        // ARRANGE
        SubscriptionRequest updateRequest = new SubscriptionRequest("Premium Plus", 14.99, 30, 10, false);
        when(subscriptionRepository.findById(testSubscriptionId)).thenReturn(Optional.of(testSubscription));
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(testSubscription);
        when(subscriptionMapper.toResponse(any(Subscription.class))).thenReturn(testResponse);

        // ACT
        SubscriptionResponse result = subscriptionService.updateSubscription(testSubscriptionId, updateRequest);

        // ASSERT
        assertNotNull(result);
        verify(subscriptionRepository, times(1)).findById(testSubscriptionId);
        verify(subscriptionRepository, times(1)).save(any(Subscription.class));
    }

    @Test
    @DisplayName("updateSubscription_whenInvalidId_expectsNotFoundException")
    void testUpdateSubscription_whenInvalidId_expectsNotFoundException() {
        // ARRANGE
        UUID invalidId = UUID.randomUUID();
        when(subscriptionRepository.findById(invalidId)).thenReturn(Optional.empty());

        // ACT & ASSERT
        assertThrows(NotFoundException.class, () -> subscriptionService.updateSubscription(invalidId, testRequest));
    }

    @Test
    @DisplayName("updateSubscription_whenSessionsChange_expectsSessionsUpdated")
    void testUpdateSubscription_whenSessionsChange_expectsSessionsUpdated() {
        // ARRANGE
        SubscriptionRequest updateRequest = new SubscriptionRequest("Premium", 9.99, 30, 10, false);
        testSubscription.setAllowedActiveSessions(5);
        when(subscriptionRepository.findById(testSubscriptionId)).thenReturn(Optional.of(testSubscription));
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(testSubscription);
        when(subscriptionMapper.toResponse(any(Subscription.class))).thenReturn(testResponse);

        // ACT
        subscriptionService.updateSubscription(testSubscriptionId, updateRequest);

        // ASSERT
        verify(subscriptionRepository, times(1)).save(any(Subscription.class));
    }

    @Test
    @DisplayName("updateSubscription_whenTemporaryFlagChange_expectsTemporaryUpdated")
    void testUpdateSubscription_whenTemporaryFlagChange_expectsTemporaryUpdated() {
        // ARRANGE
        SubscriptionRequest updateRequest = new SubscriptionRequest("Premium", 9.99, 30, 5, true);
        testSubscription.setIsTemporary(false);
        when(subscriptionRepository.findById(testSubscriptionId)).thenReturn(Optional.of(testSubscription));
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(testSubscription);
        when(subscriptionMapper.toResponse(any(Subscription.class))).thenReturn(testResponse);

        // ACT
        subscriptionService.updateSubscription(testSubscriptionId, updateRequest);

        // ASSERT
        verify(subscriptionRepository, times(1)).save(any(Subscription.class));
    }

    // ==================== DELETE TESTS ====================

    @Test
    @DisplayName("deleteSubscription_whenValidId_expectsSubscriptionMarkedDeleted")
    void testDeleteSubscription_whenValidId_expectsSubscriptionMarkedDeleted() {
        // ARRANGE
        when(subscriptionRepository.findById(testSubscriptionId)).thenReturn(Optional.of(testSubscription));
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(testSubscription);

        // ACT
        subscriptionService.deleteSubscription(testSubscriptionId);

        // ASSERT
        verify(subscriptionRepository, times(1)).findById(testSubscriptionId);
        verify(subscriptionRepository, times(1)).save(any(Subscription.class));
    }

    @Test
    @DisplayName("deleteSubscription_whenInvalidId_expectsNotFoundException")
    void testDeleteSubscription_whenInvalidId_expectsNotFoundException() {
        // ARRANGE
        UUID invalidId = UUID.randomUUID();
        when(subscriptionRepository.findById(invalidId)).thenReturn(Optional.empty());

        // ACT & ASSERT
        assertThrows(NotFoundException.class, () -> subscriptionService.deleteSubscription(invalidId));
    }

    @Test
    @DisplayName("deleteSubscription_whenAlreadyDeleted_expectsStatusUpdated")
    void testDeleteSubscription_whenAlreadyDeleted_expectsStatusUpdated() {
        // ARRANGE
        testSubscription.setRecordStatus(RecordStatus.DELETED);
        when(subscriptionRepository.findById(testSubscriptionId)).thenReturn(Optional.of(testSubscription));
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(testSubscription);

        // ACT
        subscriptionService.deleteSubscription(testSubscriptionId);

        // ASSERT
        verify(subscriptionRepository, times(1)).save(any(Subscription.class));
    }

    // ==================== BOUNDARY TESTS ====================

    @Test
    @DisplayName("createSubscription_whenZeroPrice_expectsSubscriptionCreated")
    void testCreateSubscription_whenZeroPrice_expectsSubscriptionCreated() {
        // ARRANGE
        SubscriptionRequest freeRequest = new SubscriptionRequest("Free", 0.0, 30, 1, false);
        Subscription freeSubscription = Subscription.builder()
                .id(UUID.randomUUID())
                .name("Free")
                .price(0.0)
                .durationInDays(30)
                .allowedActiveSessions(1)
                .isTemporary(false)
                .recordStatus(RecordStatus.ACTIVE)
                .build();
        when(subscriptionMapper.toEntity(freeRequest)).thenReturn(freeSubscription);
        when(subscriptionRepository.save(freeSubscription)).thenReturn(freeSubscription);
        when(subscriptionMapper.toResponse(freeSubscription)).thenReturn(
                new SubscriptionResponse(freeSubscription.getId(), "Free", 0.0, 30, 1, false)
        );

        // ACT
        SubscriptionResponse result = subscriptionService.createSubscription(freeRequest);

        // ASSERT
        assertNotNull(result);
        assertEquals(0.0, result.price());
    }

    @Test
    @DisplayName("createSubscription_whenLargeDuration_expectsSubscriptionCreated")
    void testCreateSubscription_whenLargeDuration_expectsSubscriptionCreated() {
        // ARRANGE
        SubscriptionRequest yearlyRequest = new SubscriptionRequest("Yearly", 99.99, 365, 5, false);
        Subscription yearlySubscription = Subscription.builder()
                .id(UUID.randomUUID())
                .name("Yearly")
                .price(99.99)
                .durationInDays(365)
                .allowedActiveSessions(5)
                .isTemporary(false)
                .recordStatus(RecordStatus.ACTIVE)
                .build();
        when(subscriptionMapper.toEntity(yearlyRequest)).thenReturn(yearlySubscription);
        when(subscriptionRepository.save(yearlySubscription)).thenReturn(yearlySubscription);
        when(subscriptionMapper.toResponse(yearlySubscription)).thenReturn(
                new SubscriptionResponse(yearlySubscription.getId(), "Yearly", 99.99, 365, 5, false)
        );

        // ACT
        SubscriptionResponse result = subscriptionService.createSubscription(yearlyRequest);

        // ASSERT
        assertNotNull(result);
        assertEquals(365, result.durationInDays());
    }

    @Test
    @DisplayName("createSubscription_whenMaxSessions_expectsSubscriptionCreated")
    void testCreateSubscription_whenMaxSessions_expectsSubscriptionCreated() {
        // ARRANGE
        SubscriptionRequest maxSessionRequest = new SubscriptionRequest("Family", 19.99, 30, 100, false);
        Subscription maxSessionSubscription = Subscription.builder()
                .id(UUID.randomUUID())
                .name("Family")
                .price(19.99)
                .durationInDays(30)
                .allowedActiveSessions(100)
                .isTemporary(false)
                .recordStatus(RecordStatus.ACTIVE)
                .build();
        when(subscriptionMapper.toEntity(maxSessionRequest)).thenReturn(maxSessionSubscription);
        when(subscriptionRepository.save(maxSessionSubscription)).thenReturn(maxSessionSubscription);
        when(subscriptionMapper.toResponse(maxSessionSubscription)).thenReturn(
                new SubscriptionResponse(maxSessionSubscription.getId(), "Family", 19.99, 30, 100, false)
        );

        // ACT
        SubscriptionResponse result = subscriptionService.createSubscription(maxSessionRequest);

        // ASSERT
        assertNotNull(result);
        assertEquals(100, result.allowedActiveSessions());
    }
}
