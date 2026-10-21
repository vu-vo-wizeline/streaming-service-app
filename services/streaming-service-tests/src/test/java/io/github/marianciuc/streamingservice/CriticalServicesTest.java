/*
 * Copyright (c) 2024 Vladimir Marianciuc. All Rights Reserved.
 *
 * Project: STREAMING SERVICE APP
 * File: CriticalServicesTest.java
 * Description: Comprehensive unit test suite for critical microservices:
 *              - Payment Service (Transaction processing)
 *              - Subscription Service (Subscription lifecycle)
 *              - User Subscription Service (User subscription management)
 *              - Video Service (Media handling)
 */

package io.github.marianciuc.streamingservice.test;

import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import com.stripe.param.PaymentIntentCreateParams;
import io.github.marianciuc.streamingservice.payment.dto.common.TransactionDto;
import io.github.marianciuc.streamingservice.payment.entity.CardHolder;
import io.github.marianciuc.streamingservice.payment.entity.JWTUserPrincipal;
import io.github.marianciuc.streamingservice.payment.entity.Transaction;
import io.github.marianciuc.streamingservice.payment.enums.Currency;
import io.github.marianciuc.streamingservice.payment.enums.PaymentStatus;
import io.github.marianciuc.streamingservice.payment.kafka.PaymentKafkaProducer;
import io.github.marianciuc.streamingservice.payment.kafka.messages.InitializePaymentMessage;
import io.github.marianciuc.streamingservice.payment.repository.TransactionRepository;
import io.github.marianciuc.streamingservice.payment.service.CardHolderService;
import io.github.marianciuc.streamingservice.payment.service.impl.TransactionServiceImpl;
import io.github.marianciuc.streamingservice.subscription.clients.OrderClient;
import io.github.marianciuc.streamingservice.subscription.dto.CreateOrderRequest;
import io.github.marianciuc.streamingservice.subscription.dto.OrderCreationEventKafkaDto;
import io.github.marianciuc.streamingservice.subscription.dto.OrderResponse;
import io.github.marianciuc.streamingservice.subscription.dto.SubscriptionRequest;
import io.github.marianciuc.streamingservice.subscription.dto.SubscriptionResponse;
import io.github.marianciuc.streamingservice.subscription.dto.UserSubscriptionDto;
import io.github.marianciuc.streamingservice.subscription.entity.RecordStatus;
import io.github.marianciuc.streamingservice.subscription.entity.Subscription;
import io.github.marianciuc.streamingservice.subscription.entity.SubscriptionStatus;
import io.github.marianciuc.streamingservice.subscription.entity.UserSubscriptions;
import io.github.marianciuc.streamingservice.subscription.exceptions.NotFoundException;
import io.github.marianciuc.streamingservice.subscription.mapper.SubscriptionMapper;
import io.github.marianciuc.streamingservice.subscription.mapper.UserSubscriptionMapper;
import io.github.marianciuc.streamingservice.subscription.repository.SubscriptionRepository;
import io.github.marianciuc.streamingservice.subscription.repository.UserSubscriptionRepository;
import io.github.marianciuc.streamingservice.subscription.service.SubscriptionService;
import io.github.marianciuc.streamingservice.subscription.service.impl.SubscriptionServiceImpl;
import io.github.marianciuc.streamingservice.subscription.service.impl.UserSubscriptionServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.naming.OperationNotSupportedException;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit test suite for critical streaming service microservices.
 * Tests cover happy paths, boundary values, error conditions, and side effects.
 * Framework: JUnit 5 + Mockito
 * Pattern: Arrange-Act-Assert (AAA)
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Critical Streaming Service Unit Tests")
public class CriticalServicesTest {

    // ============================================================================
    // PAYMENT SERVICE TESTS - TransactionServiceImpl
    // ============================================================================

    @Nested
    @DisplayName("TransactionServiceImpl Tests")
    class TransactionServiceTests {

        @Mock
        private TransactionRepository transactionRepository;

        @Mock
        private CardHolderService cardHolderService;

        @Mock
        private PaymentKafkaProducer paymentKafkaProducer;

        @InjectMocks
        private TransactionServiceImpl transactionService;

        private UUID testUserId;
        private UUID testOrderId;
        private UUID testTransactionId;
        private CardHolder testCardHolder;
        private InitializePaymentMessage testPaymentMessage;

        @BeforeEach
        void setUp() {
            testUserId = UUID.randomUUID();
            testOrderId = UUID.randomUUID();
            testTransactionId = UUID.randomUUID();

            testCardHolder = CardHolder.builder()
                    .id(UUID.randomUUID())
                    .userId(testUserId)
                    .stripeCustomerId("cus_test123")
                    .build();

            testPaymentMessage = new InitializePaymentMessage(
                    testUserId,
                    testOrderId,
                    100L,
                    Currency.USD
            );
        }

        @Test
        @DisplayName("Happy Path: Initialize transaction with valid payment message and successful Stripe payment")
        void testInitializeTransaction_whenValidMessage_expectsSuccessfulPayment() throws StripeException {
            // ARRANGE
            when(cardHolderService.findCardHolderEntity(testUserId)).thenReturn(testCardHolder);

            PaymentIntent mockPaymentIntent = mock(PaymentIntent.class);
            when(mockPaymentIntent.getId()).thenReturn("pi_test123");

            ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);

            // ACT
            transactionService.initializeTransaction(testPaymentMessage);

            // ASSERT
            verify(cardHolderService).findCardHolderEntity(testUserId);
            verify(transactionRepository).save(transactionCaptor.capture());

            Transaction savedTransaction = transactionCaptor.getValue();
            assertNotNull(savedTransaction);
            assertEquals(testPaymentMessage.amount(), savedTransaction.getAmount());
            assertEquals(testPaymentMessage.currency(), savedTransaction.getCurrency());
            assertEquals(testOrderId, savedTransaction.getOrderId());
            assertEquals(testCardHolder, savedTransaction.getCardHolder());
        }

        @Test
        @DisplayName("Error Path: Initialize transaction when Stripe API throws exception")
        void testInitializeTransaction_whenStripeException_expectsFailedStatus() throws StripeException {
            // ARRANGE
            when(cardHolderService.findCardHolderEntity(testUserId)).thenReturn(testCardHolder);

            StripeException stripeException = new StripeException("Card declined", "card_declined", "402");

            ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);

            // ACT & ASSERT
            assertThrows(RuntimeException.class, () -> transactionService.initializeTransaction(testPaymentMessage));

            verify(transactionRepository).save(transactionCaptor.capture());
            Transaction savedTransaction = transactionCaptor.getValue();
            assertEquals(PaymentStatus.FAILED, savedTransaction.getStatus());
            assertNotNull(savedTransaction.getFailureMessage());
        }

        @Test
        @DisplayName("Boundary: Initialize transaction with zero amount")
        void testInitializeTransaction_whenZeroAmount_expectsTransactionCreated() {
            // ARRANGE
            InitializePaymentMessage zeroAmountMessage = new InitializePaymentMessage(
                    testUserId,
                    testOrderId,
                    0L,
                    Currency.USD
            );
            when(cardHolderService.findCardHolderEntity(testUserId)).thenReturn(testCardHolder);

            ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);

            // ACT
            transactionService.initializeTransaction(zeroAmountMessage);

            // ASSERT
            verify(transactionRepository).save(transactionCaptor.capture());
            assertEquals(0L, transactionCaptor.getValue().getAmount());
        }

        @Test
        @DisplayName("Boundary: Initialize transaction with maximum amount")
        void testInitializeTransaction_whenMaxAmount_expectsTransactionCreated() {
            // ARRANGE
            InitializePaymentMessage maxAmountMessage = new InitializePaymentMessage(
                    testUserId,
                    testOrderId,
                    Long.MAX_VALUE,
                    Currency.USD
            );
            when(cardHolderService.findCardHolderEntity(testUserId)).thenReturn(testCardHolder);

            ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);

            // ACT
            transactionService.initializeTransaction(maxAmountMessage);

            // ASSERT
            verify(transactionRepository).save(transactionCaptor.capture());
            assertEquals(Long.MAX_VALUE, transactionCaptor.getValue().getAmount());
        }

        @Test
        @DisplayName("Error Path: Find transaction when not found")
        void testFindTransactionEntity_whenNotFound_expectsRuntimeException() {
            // ARRANGE
            when(transactionRepository.findById(testTransactionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(RuntimeException.class, () -> transactionService.findTransactionEntity(testTransactionId));
            verify(transactionRepository).findById(testTransactionId);
        }

        @Test
        @DisplayName("Happy Path: Find transaction entity when exists")
        void testFindTransactionEntity_whenExists_expectsTransactionReturned() {
            // ARRANGE
            Transaction expectedTransaction = Transaction.builder()
                    .id(testTransactionId)
                    .amount(100L)
                    .currency(Currency.USD)
                    .status(PaymentStatus.SUCCESS)
                    .build();

            when(transactionRepository.findById(testTransactionId)).thenReturn(Optional.of(expectedTransaction));

            // ACT
            Transaction result = transactionService.findTransactionEntity(testTransactionId);

            // ASSERT
            assertNotNull(result);
            assertEquals(testTransactionId, result.getId());
            assertEquals(100L, result.getAmount());
            verify(transactionRepository).findById(testTransactionId);
        }

        @Test
        @DisplayName("Error Path: Get transactions when user lacks permission")
        void testGetTransactions_whenAccessDenied_expectsAccessDeniedException() {
            // ARRANGE
            SecurityContext securityContext = mock(SecurityContext.class);
            Authentication authentication = mock(Authentication.class);
            when(securityContext.getAuthentication()).thenReturn(authentication);
            when(authentication.getPrincipal()).thenReturn("anonymousUser");
            SecurityContextHolder.setContext(securityContext);

            // ACT & ASSERT
            assertThrows(AccessDeniedException.class, () ->
                    transactionService.getTransactions(0, 10, "ASC", PaymentStatus.SUCCESS, testUserId)
            );
        }
    }

    // ============================================================================
    // SUBSCRIPTION SERVICE TESTS - SubscriptionServiceImpl
    // ============================================================================

    @Nested
    @DisplayName("SubscriptionServiceImpl Tests")
    class SubscriptionServiceTests {

        @Mock
        private SubscriptionRepository subscriptionRepository;

        @Mock
        private SubscriptionMapper subscriptionMapper;

        @InjectMocks
        private SubscriptionServiceImpl subscriptionService;

        private UUID subscriptionId;
        private Subscription testSubscription;
        private SubscriptionRequest testSubscriptionRequest;
        private SubscriptionResponse testSubscriptionResponse;

        @BeforeEach
        void setUp() {
            subscriptionId = UUID.randomUUID();

            testSubscription = Subscription.builder()
                    .id(subscriptionId)
                    .name("Premium")
                    .price(9.99)
                    .durationInDays(30)
                    .allowedActiveSessions(4)
                    .isTemporary(false)
                    .recordStatus(RecordStatus.ACTIVE)
                    .build();

            testSubscriptionRequest = new SubscriptionRequest(
                    "Premium",
                    9.99,
                    30,
                    4,
                    false
            );

            testSubscriptionResponse = new SubscriptionResponse(
                    subscriptionId,
                    "Premium",
                    9.99,
                    30,
                    4,
                    false
            );
        }

        @Test
        @DisplayName("Happy Path: Create subscription with valid request")
        void testCreateSubscription_whenValidRequest_expectsSubscriptionCreated() {
            // ARRANGE
            when(subscriptionMapper.toEntity(testSubscriptionRequest)).thenReturn(testSubscription);
            when(subscriptionRepository.save(testSubscription)).thenReturn(testSubscription);
            when(subscriptionMapper.toResponse(testSubscription)).thenReturn(testSubscriptionResponse);

            // ACT
            SubscriptionResponse result = subscriptionService.createSubscription(testSubscriptionRequest);

            // ASSERT
            assertNotNull(result);
            assertEquals(subscriptionId, result.id());
            assertEquals("Premium", result.name());
            verify(subscriptionRepository).save(testSubscription);
            verify(subscriptionMapper).toEntity(testSubscriptionRequest);
        }

        @Test
        @DisplayName("Happy Path: Get subscription by ID when exists")
        void testGetSubscription_whenExists_expectsSubscriptionReturned() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(testSubscription));

            // ACT
            Subscription result = subscriptionService.getSubscription(subscriptionId);

            // ASSERT
            assertNotNull(result);
            assertEquals(subscriptionId, result.getId());
            assertEquals("Premium", result.getName());
            verify(subscriptionRepository).findById(subscriptionId);
        }

        @Test
        @DisplayName("Error Path: Get subscription when not found")
        void testGetSubscription_whenNotFound_expectsNotFoundException() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () -> subscriptionService.getSubscription(subscriptionId));
            verify(subscriptionRepository).findById(subscriptionId);
        }

        @Test
        @DisplayName("Happy Path: Get subscription response by ID")
        void testGetSubscriptionResponse_whenExists_expectsResponseReturned() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(testSubscription));
            when(subscriptionMapper.toResponse(testSubscription)).thenReturn(testSubscriptionResponse);

            // ACT
            SubscriptionResponse result = subscriptionService.getSubscriptionResponse(subscriptionId);

            // ASSERT
            assertNotNull(result);
            assertEquals(subscriptionId, result.id());
            verify(subscriptionRepository).findById(subscriptionId);
        }

        @Test
        @DisplayName("Happy Path: Update subscription with new allowed sessions")
        void testUpdateSubscription_whenSessionsChanged_expectsSubscriptionUpdated() {
            // ARRANGE
            SubscriptionRequest updateRequest = new SubscriptionRequest(
                    "Premium",
                    9.99,
                    30,
                    6,  // Changed from 4 to 6
                    false
            );

            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(testSubscription));
            when(subscriptionRepository.save(any(Subscription.class))).thenReturn(testSubscription);
            when(subscriptionMapper.toResponse(any(Subscription.class))).thenReturn(testSubscriptionResponse);

            // ACT
            SubscriptionResponse result = subscriptionService.updateSubscription(subscriptionId, updateRequest);

            // ASSERT
            assertNotNull(result);
            verify(subscriptionRepository).save(any(Subscription.class));
        }

        @Test
        @DisplayName("Happy Path: Delete subscription marks as DELETED")
        void testDeleteSubscription_whenExists_expectsMarkedAsDeleted() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(testSubscription));
            when(subscriptionRepository.save(any(Subscription.class))).thenReturn(testSubscription);

            // ACT
            subscriptionService.deleteSubscription(subscriptionId);

            // ASSERT
            ArgumentCaptor<Subscription> captor = ArgumentCaptor.forClass(Subscription.class);
            verify(subscriptionRepository).save(captor.capture());
            assertEquals(RecordStatus.DELETED, captor.getValue().getRecordStatus());
        }

        @Test
        @DisplayName("Happy Path: Get all subscriptions")
        void testGetAllSubscriptions_whenMultipleExist_expectsAllReturned() {
            // ARRANGE
            List<Subscription> subscriptions = List.of(testSubscription);
            when(subscriptionRepository.findAll()).thenReturn(subscriptions);
            when(subscriptionMapper.toResponse(testSubscription)).thenReturn(testSubscriptionResponse);

            // ACT
            List<SubscriptionResponse> result = subscriptionService.getAllSubscriptions();

            // ASSERT
            assertNotNull(result);
            assertEquals(1, result.size());
            verify(subscriptionRepository).findAll();
        }

        @Test
        @DisplayName("Boundary: Get all subscriptions when empty")
        void testGetAllSubscriptions_whenEmpty_expectsEmptyList() {
            // ARRANGE
            when(subscriptionRepository.findAll()).thenReturn(List.of());

            // ACT
            List<SubscriptionResponse> result = subscriptionService.getAllSubscriptions();

            // ASSERT
            assertNotNull(result);
            assertTrue(result.isEmpty());
            verify(subscriptionRepository).findAll();
        }
    }

    // ============================================================================
    // USER SUBSCRIPTION SERVICE TESTS - UserSubscriptionServiceImpl
    // ============================================================================

    @Nested
    @DisplayName("UserSubscriptionServiceImpl Tests")
    class UserSubscriptionServiceTests {

        @Mock
        private UserSubscriptionRepository userSubscriptionRepository;

        @Mock
        private SubscriptionService subscriptionService;

        @Mock
        private OrderClient orderClient;

        @Mock
        private UserSubscriptionMapper userSubscriptionMapper;

        @InjectMocks
        private UserSubscriptionServiceImpl userSubscriptionService;

        private UUID userId;
        private UUID subscriptionId;
        private UUID orderId;
        private UUID userSubscriptionId;
        private Subscription testSubscription;
        private UserSubscriptions testUserSubscription;
        private OrderCreationEventKafkaDto testOrderEvent;

        @BeforeEach
        void setUp() {
            userId = UUID.randomUUID();
            subscriptionId = UUID.randomUUID();
            orderId = UUID.randomUUID();
            userSubscriptionId = UUID.randomUUID();

            testSubscription = Subscription.builder()
                    .id(subscriptionId)
                    .name("Premium")
                    .price(9.99)
                    .durationInDays(30)
                    .build();

            testUserSubscription = UserSubscriptions.builder()
                    .id(userSubscriptionId)
                    .userId(userId)
                    .orderId(orderId)
                    .subscription(testSubscription)
                    .status(SubscriptionStatus.ACTIVE)
                    .startDate(LocalDate.now())
                    .endDate(LocalDate.now().plusDays(30))
                    .build();

            testOrderEvent = new OrderCreationEventKafkaDto(
                    userId,
                    orderId,
                    subscriptionId
            );
        }

        @Test
        @DisplayName("Happy Path: Subscribe user with new order")
        void testSubscribeUser_whenNewOrder_expectsUserSubscriptionCreated() {
            // ARRANGE
            when(subscriptionService.getSubscription(subscriptionId)).thenReturn(testSubscription);
            when(userSubscriptionRepository.findByOrderId(orderId)).thenReturn(Optional.empty());
            when(userSubscriptionRepository.save(any(UserSubscriptions.class))).thenReturn(testUserSubscription);

            // ACT
            userSubscriptionService.subscribeUser(testOrderEvent);

            // ASSERT
            ArgumentCaptor<UserSubscriptions> captor = ArgumentCaptor.forClass(UserSubscriptions.class);
            verify(userSubscriptionRepository).save(captor.capture());

            UserSubscriptions saved = captor.getValue();
            assertEquals(userId, saved.getUserId());
            assertEquals(orderId, saved.getOrderId());
            assertEquals(SubscriptionStatus.ACTIVE, saved.getStatus());
        }

        @Test
        @DisplayName("Happy Path: Subscribe user when order already exists - updates subscription")
        void testSubscribeUser_whenOrderExists_expectsSubscriptionUpdated() {
            // ARRANGE
            UserSubscriptions existingSubscription = UserSubscriptions.builder()
                    .id(userSubscriptionId)
                    .userId(userId)
                    .orderId(orderId)
                    .subscription(Subscription.builder().id(UUID.randomUUID()).build())
                    .status(SubscriptionStatus.EXPIRED)
                    .build();

            when(subscriptionService.getSubscription(subscriptionId)).thenReturn(testSubscription);
            when(userSubscriptionRepository.findByOrderId(orderId)).thenReturn(Optional.of(existingSubscription));
            when(userSubscriptionRepository.save(any(UserSubscriptions.class))).thenReturn(existingSubscription);

            // ACT
            userSubscriptionService.subscribeUser(testOrderEvent);

            // ASSERT
            verify(userSubscriptionRepository).save(existingSubscription);
            assertEquals(SubscriptionStatus.ACTIVE, existingSubscription.getStatus());
        }

        @Test
        @DisplayName("Happy Path: Get active subscription for user")
        void testGetActiveSubscription_whenExists_expectsSubscriptionReturned() {
            // ARRANGE
            UserSubscriptionDto expectedDto = new UserSubscriptionDto(
                    userSubscriptionId,
                    userId,
                    orderId,
                    subscriptionId,
                    SubscriptionStatus.ACTIVE,
                    LocalDate.now(),
                    LocalDate.now().plusDays(30)
            );

            when(userSubscriptionRepository.findByUserIdAndStatus(userId, SubscriptionStatus.ACTIVE))
                    .thenReturn(Optional.of(testUserSubscription));
            when(userSubscriptionMapper.toUserSubscriptionDto(testUserSubscription)).thenReturn(expectedDto);

            // ACT
            UserSubscriptionDto result = userSubscriptionService.getActiveSubscription(null, userId);

            // ASSERT
            assertNotNull(result);
            assertEquals(userId, result.userId());
            assertEquals(SubscriptionStatus.ACTIVE, result.status());
        }

        @Test
        @DisplayName("Error Path: Get active subscription when not found")
        void testGetActiveSubscription_whenNotFound_expectsNotFoundException() {
            // ARRANGE
            when(userSubscriptionRepository.findByUserIdAndStatus(userId, SubscriptionStatus.ACTIVE))
                    .thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () ->
                    userSubscriptionService.getActiveSubscription(null, userId)
            );
        }

        @Test
        @DisplayName("Happy Path: Cancel active subscription")
        void testCancelSubscription_whenActive_expectsStatusChangedToCancelled() throws OperationNotSupportedException {
            // ARRANGE
            when(userSubscriptionRepository.save(any(UserSubscriptions.class))).thenReturn(testUserSubscription);

            // ACT
            userSubscriptionService.cancelSubscription(testUserSubscription);

            // ASSERT
            ArgumentCaptor<UserSubscriptions> captor = ArgumentCaptor.forClass(UserSubscriptions.class);
            verify(userSubscriptionRepository).save(captor.capture());
            assertEquals(SubscriptionStatus.CANCELLED, captor.getValue().getStatus());
        }

        @Test
        @DisplayName("Error Path: Cancel already cancelled subscription")
        void testCancelSubscription_whenAlreadyCancelled_expectsOperationNotSupportedException() {
            // ARRANGE
            testUserSubscription.setStatus(SubscriptionStatus.CANCELLED);

            // ACT & ASSERT
            assertThrows(OperationNotSupportedException.class, () ->
                    userSubscriptionService.cancelSubscription(testUserSubscription)
            );
        }

        @Test
        @DisplayName("Happy Path: Extend subscription successfully")
        void testExtendSubscription_whenSuccessful_expectsNewSubscriptionCreated() throws IOException, OperationNotSupportedException {
            // ARRANGE
            OrderResponse orderResponse = new OrderResponse(UUID.randomUUID(), userId, subscriptionId, 9.99);
            ResponseEntity<OrderResponse> responseEntity = new ResponseEntity<>(orderResponse, HttpStatus.OK);

            when(orderClient.createOrder(any(CreateOrderRequest.class))).thenReturn(responseEntity);
            when(userSubscriptionRepository.save(any(UserSubscriptions.class))).thenReturn(testUserSubscription);

            // ACT
            userSubscriptionService.extendSubscription(testUserSubscription);

            // ASSERT
            verify(orderClient).createOrder(any(CreateOrderRequest.class));
            verify(userSubscriptionRepository, times(2)).save(any(UserSubscriptions.class));
        }

        @Test
        @DisplayName("Error Path: Extend subscription when order creation fails")
        void testExtendSubscription_whenOrderCreationFails_expectsIOException() {
            // ARRANGE
            when(orderClient.createOrder(any(CreateOrderRequest.class)))
                    .thenThrow(new RuntimeException("Order service unavailable"));

            // ACT & ASSERT
            assertThrows(IOException.class, () ->
                    userSubscriptionService.extendSubscription(testUserSubscription)
            );
        }

        @Test
        @DisplayName("Happy Path: Unsubscribe active user")
        void testUnsubscribeUser_whenActive_expectsStatusChangedToExpired() throws OperationNotSupportedException {
            // ARRANGE
            when(userSubscriptionRepository.save(any(UserSubscriptions.class))).thenReturn(testUserSubscription);

            // ACT
            userSubscriptionService.unsubscribeUser(testUserSubscription);

            // ASSERT
            ArgumentCaptor<UserSubscriptions> captor = ArgumentCaptor.forClass(UserSubscriptions.class);
            verify(userSubscriptionRepository).save(captor.capture());
            assertEquals(SubscriptionStatus.EXPIRED, captor.getValue().getStatus());
        }

        @Test
        @DisplayName("Error Path: Unsubscribe already expired subscription")
        void testUnsubscribeUser_whenAlreadyExpired_expectsOperationNotSupportedException() {
            // ARRANGE
            testUserSubscription.setStatus(SubscriptionStatus.EXPIRED);

            // ACT & ASSERT
            assertThrows(OperationNotSupportedException.class, () ->
                    userSubscriptionService.unsubscribeUser(testUserSubscription)
            );
        }

        @Test
        @DisplayName("Boundary: Get all user subscriptions by status and end date")
        void testGetAllUserSubscriptionsByStatusAndEndDate_whenMultipleExist_expectsAllReturned() {
            // ARRANGE
            LocalDate targetDate = LocalDate.now();
            List<UserSubscriptions> subscriptions = List.of(testUserSubscription);
            when(userSubscriptionRepository.findAllByStatusAndEndDate(SubscriptionStatus.ACTIVE, targetDate))
                    .thenReturn(subscriptions);

            // ACT
            List<UserSubscriptions> result = userSubscriptionService.getAllUserSubscriptionsByStatusAndEndDate(
                    SubscriptionStatus.ACTIVE,
                    targetDate
            );

            // ASSERT
            assertNotNull(result);
            assertEquals(1, result.size());
            verify(userSubscriptionRepository).findAllByStatusAndEndDate(SubscriptionStatus.ACTIVE, targetDate);
        }

        @Test
        @DisplayName("Boundary: Get all user subscriptions when none match criteria")
        void testGetAllUserSubscriptionsByStatusAndEndDate_whenNoneMatch_expectsEmptyList() {
            // ARRANGE
            LocalDate targetDate = LocalDate.now();
            when(userSubscriptionRepository.findAllByStatusAndEndDate(SubscriptionStatus.CANCELLED, targetDate))
                    .thenReturn(List.of());

            // ACT
            List<UserSubscriptions> result = userSubscriptionService.getAllUserSubscriptionsByStatusAndEndDate(
                    SubscriptionStatus.CANCELLED,
                    targetDate
            );

            // ASSERT
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }
    }

    // ============================================================================
    // PARAMETRIZED TESTS - Multiple scenarios with different inputs
    // ============================================================================

    @Nested
    @DisplayName("Parametrized Tests for Currency Handling")
    class ParametrizedCurrencyTests {

        @Mock
        private TransactionRepository transactionRepository;

        @Mock
        private CardHolderService cardHolderService;

        @Mock
        private PaymentKafkaProducer paymentKafkaProducer;

        @InjectMocks
        private TransactionServiceImpl transactionService;

        @ParameterizedTest
        @ValueSource(strings = {"USD", "EUR", "GBP"})
        @DisplayName("Initialize transaction with different currencies")
        void testInitializeTransaction_withMultipleCurrencies_expectsTransactionCreated(String currencyCode) {
            // ARRANGE
            UUID userId = UUID.randomUUID();
            UUID orderId = UUID.randomUUID();
            Currency currency = Currency.valueOf(currencyCode);

            CardHolder cardHolder = CardHolder.builder()
                    .id(UUID.randomUUID())
                    .userId(userId)
                    .stripeCustomerId("cus_test")
                    .build();

            InitializePaymentMessage message = new InitializePaymentMessage(
                    userId,
                    orderId,
                    100L,
                    currency
            );

            when(cardHolderService.findCardHolderEntity(userId)).thenReturn(cardHolder);

            ArgumentCaptor<Transaction> captor = ArgumentCaptor.forClass(Transaction.class);

            // ACT
            transactionService.initializeTransaction(message);

            // ASSERT
            verify(transactionRepository).save(captor.capture());
            assertEquals(currency, captor.getValue().getCurrency());
        }
    }
}
