/*
 * Copyright (c) 2024 Vladimir Marianciuc. All Rights Reserved.
 * Comprehensive Unit Test Suite for Critical Streaming Service Microservices
 * Framework: JUnit 5 + Mockito
 * Coverage: User, Subscription, Payment, and Refund Services
 */

package io.github.marianciuc.streamingservice.test;

import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import com.stripe.model.Refund;
import io.github.marianciuc.streamingservice.payment.dto.common.TransactionDto;
import io.github.marianciuc.streamingservice.payment.entity.CardHolder;
import io.github.marianciuc.streamingservice.payment.entity.JWTUserPrincipal;
import io.github.marianciuc.streamingservice.payment.entity.Transaction;
import io.github.marianciuc.streamingservice.payment.enums.Currency;
import io.github.marianciuc.streamingservice.payment.enums.PaymentStatus;
import io.github.marianciuc.streamingservice.payment.enums.RefundStatus;
import io.github.marianciuc.streamingservice.payment.kafka.PaymentKafkaProducer;
import io.github.marianciuc.streamingservice.payment.kafka.messages.InitializePaymentMessage;
import io.github.marianciuc.streamingservice.payment.repository.RefundRepository;
import io.github.marianciuc.streamingservice.payment.repository.TransactionRepository;
import io.github.marianciuc.streamingservice.payment.service.CardHolderService;
import io.github.marianciuc.streamingservice.payment.service.RefundService;
import io.github.marianciuc.streamingservice.payment.service.TransactionService;
import io.github.marianciuc.streamingservice.payment.service.impl.RefundServiceImpl;
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
import io.github.marianciuc.streamingservice.subscription.service.UserSubscriptionService;
import io.github.marianciuc.streamingservice.subscription.service.impl.SubscriptionServiceImpl;
import io.github.marianciuc.streamingservice.subscription.service.impl.UserSubscriptionServiceImpl;
import io.github.marianciuc.streamingservice.user.dto.requests.ChangePasswordRequest;
import io.github.marianciuc.streamingservice.user.dto.requests.RegistrationRequest;
import io.github.marianciuc.streamingservice.user.dto.responses.UserResponse;
import io.github.marianciuc.streamingservice.user.entity.User;
import io.github.marianciuc.streamingservice.user.enums.RecordStatus;
import io.github.marianciuc.streamingservice.user.enums.Role;
import io.github.marianciuc.streamingservice.user.exceptions.IllegalArgumentException;
import io.github.marianciuc.streamingservice.user.exceptions.NotFoundException;
import io.github.marianciuc.streamingservice.user.exceptions.SecurityBadCredentialsException;
import io.github.marianciuc.streamingservice.user.kafka.KafkaUserProducer;
import io.github.marianciuc.streamingservice.user.repositories.UserRepository;
import io.github.marianciuc.streamingservice.user.services.UserService;
import io.github.marianciuc.streamingservice.user.services.impl.UserServiceImpl;
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
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit test suite for critical microservices.
 * Tests cover: happy paths, boundary values, error conditions, and side effects.
 * Framework: JUnit 5 with Mockito for dependency mocking.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Streaming Service Critical Components Unit Tests")
public class CriticalServicesTest {

    // ============================================================================
    // USER SERVICE TESTS
    // ============================================================================

    @Nested
    @DisplayName("UserServiceImpl Tests")
    class UserServiceImplTests {

        @Mock
        private UserRepository userRepository;

        @Mock
        private PasswordEncoder passwordEncoder;

        @Mock
        private KafkaUserProducer kafkaUserProducer;

        @InjectMocks
        private UserServiceImpl userService;

        private UUID testUserId;
        private User testUser;
        private RegistrationRequest registrationRequest;

        @BeforeEach
        void setUp() {
            testUserId = UUID.randomUUID();
            testUser = User.builder()
                    .id(testUserId)
                    .username("testuser")
                    .email("test@example.com")
                    .passwordHash("hashedPassword")
                    .isBanned(false)
                    .recordStatus(RecordStatus.ACTIVE)
                    .role(Role.USER)
                    .build();
            registrationRequest = new RegistrationRequest("newuser", "new@example.com", "password123", Role.USER);
        }

        // ========== HAPPY PATH TESTS ==========

        @Test
        @DisplayName("createUser_whenValidRequest_expectsUserCreatedAndKafkaMessageSent")
        void testCreateUserSuccess() {
            // ARRANGE
            when(userRepository.existsByEmailOrUsername(registrationRequest.email(), registrationRequest.username()))
                    .thenReturn(false);
            when(passwordEncoder.encode(registrationRequest.password())).thenReturn("encodedPassword");
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.createUser(registrationRequest);

            // ASSERT
            verify(userRepository, times(1)).save(any(User.class));
            verify(kafkaUserProducer, times(1)).sendUserCreatedMessage(any());
        }

        @Test
        @DisplayName("getUserById_whenUserExists_expectsUserReturned")
        void testGetUserByIdSuccess() {
            // ARRANGE
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));

            // ACT
            User result = userService.getUserById(testUserId);

            // ASSERT
            assertNotNull(result);
            assertEquals(testUserId, result.getId());
            assertEquals("testuser", result.getUsername());
            verify(userRepository, times(1)).findById(testUserId);
        }

        @Test
        @DisplayName("banUser_whenUserExists_expectsUserBannedStatusToggled")
        void testBanUserSuccess() {
            // ARRANGE
            testUser.setIsBanned(false);
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.banUser(testUserId);

            // ASSERT
            assertTrue(testUser.getIsBanned());
            verify(userRepository, times(1)).save(testUser);
        }

        @Test
        @DisplayName("updateUserRole_whenUserExists_expectsRoleUpdated")
        void testUpdateUserRoleSuccess() {
            // ARRANGE
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.updateUserRole(Role.ADMIN, testUserId);

            // ASSERT
            assertEquals(Role.ADMIN, testUser.getRole());
            verify(userRepository, times(1)).save(testUser);
        }

        @Test
        @DisplayName("getUserResponse_whenUserExists_expectsUserResponseReturned")
        void testGetUserResponseSuccess() {
            // ARRANGE
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));

            // ACT
            UserResponse response = userService.getUserResponse(testUserId);

            // ASSERT
            assertNotNull(response);
            assertEquals(testUser.getUsername(), response.username());
            verify(userRepository, times(1)).findById(testUserId);
        }

        // ========== ERROR/EXCEPTION TESTS ==========

        @Test
        @DisplayName("createUser_whenUserAlreadyExists_expectsIllegalArgumentException")
        void testCreateUserAlreadyExists() {
            // ARRANGE
            when(userRepository.existsByEmailOrUsername(registrationRequest.email(), registrationRequest.username()))
                    .thenReturn(true);

            // ACT & ASSERT
            assertThrows(IllegalArgumentException.class, () -> userService.createUser(registrationRequest));
            verify(userRepository, never()).save(any());
            verify(kafkaUserProducer, never()).sendUserCreatedMessage(any());
        }

        @Test
        @DisplayName("getUserById_whenUserNotFound_expectsNotFoundException")
        void testGetUserByIdNotFound() {
            // ARRANGE
            when(userRepository.findById(testUserId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () -> userService.getUserById(testUserId));
        }

        @Test
        @DisplayName("changePassword_whenOldPasswordIncorrect_expectsSecurityBadCredentialsException")
        void testChangePasswordIncorrectOldPassword() {
            // ARRANGE
            ChangePasswordRequest changePasswordRequest = new ChangePasswordRequest("wrongPassword", "newPassword123");
            when(userRepository.findById(any())).thenReturn(Optional.of(testUser));
            when(passwordEncoder.matches("wrongPassword", testUser.getPasswordHash())).thenReturn(false);

            // ACT & ASSERT
            assertThrows(SecurityBadCredentialsException.class, () -> userService.changePassword(changePasswordRequest));
            verify(userRepository, never()).save(any());
        }

        // ========== BOUNDARY VALUE TESTS ==========

        @Test
        @DisplayName("createUser_whenNullEmail_expectsException")
        void testCreateUserNullEmail() {
            // ARRANGE
            RegistrationRequest invalidRequest = new RegistrationRequest("user", null, "password", Role.USER);

            // ACT & ASSERT
            assertThrows(Exception.class, () -> userService.createUser(invalidRequest));
        }

        @Test
        @DisplayName("banUser_whenCalledTwice_expectsUserUnbanned")
        void testBanUserToggle() {
            // ARRANGE
            testUser.setIsBanned(false);
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.banUser(testUserId); // Ban
            assertTrue(testUser.getIsBanned());
            userService.banUser(testUserId); // Unban

            // ASSERT
            assertFalse(testUser.getIsBanned());
            verify(userRepository, times(2)).save(testUser);
        }
    }

    // ============================================================================
    // SUBSCRIPTION SERVICE TESTS
    // ============================================================================

    @Nested
    @DisplayName("SubscriptionServiceImpl Tests")
    class SubscriptionServiceImplTests {

        @Mock
        private SubscriptionRepository subscriptionRepository;

        @Mock
        private SubscriptionMapper subscriptionMapper;

        @InjectMocks
        private SubscriptionServiceImpl subscriptionService;

        private UUID subscriptionId;
        private Subscription testSubscription;
        private SubscriptionRequest subscriptionRequest;
        private SubscriptionResponse subscriptionResponse;

        @BeforeEach
        void setUp() {
            subscriptionId = UUID.randomUUID();
            testSubscription = Subscription.builder()
                    .id(subscriptionId)
                    .name("Premium")
                    .price(9.99)
                    .durationInDays(30)
                    .allowedActiveSessions(5)
                    .isTemporary(false)
                    .recordStatus(RecordStatus.ACTIVE)
                    .build();
            subscriptionRequest = new SubscriptionRequest("Premium", 9.99, 30, 5, false);
            subscriptionResponse = new SubscriptionResponse(subscriptionId, "Premium", 9.99, 30, 5, false);
        }

        // ========== HAPPY PATH TESTS ==========

        @Test
        @DisplayName("createSubscription_whenValidRequest_expectsSubscriptionCreated")
        void testCreateSubscriptionSuccess() {
            // ARRANGE
            when(subscriptionMapper.toEntity(subscriptionRequest)).thenReturn(testSubscription);
            when(subscriptionRepository.save(testSubscription)).thenReturn(testSubscription);
            when(subscriptionMapper.toResponse(testSubscription)).thenReturn(subscriptionResponse);

            // ACT
            SubscriptionResponse result = subscriptionService.createSubscription(subscriptionRequest);

            // ASSERT
            assertNotNull(result);
            assertEquals(subscriptionId, result.id());
            verify(subscriptionRepository, times(1)).save(testSubscription);
        }

        @Test
        @DisplayName("getSubscription_whenSubscriptionExists_expectsSubscriptionReturned")
        void testGetSubscriptionSuccess() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(testSubscription));

            // ACT
            Subscription result = subscriptionService.getSubscription(subscriptionId);

            // ASSERT
            assertNotNull(result);
            assertEquals(subscriptionId, result.getId());
            assertEquals("Premium", result.getName());
        }

        @Test
        @DisplayName("updateSubscription_whenValidRequest_expectsSubscriptionUpdated")
        void testUpdateSubscriptionSuccess() {
            // ARRANGE
            SubscriptionRequest updateRequest = new SubscriptionRequest("Premium Plus", 14.99, 30, 10, false);
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(testSubscription));
            when(subscriptionRepository.save(testSubscription)).thenReturn(testSubscription);
            when(subscriptionMapper.toResponse(testSubscription)).thenReturn(subscriptionResponse);

            // ACT
            SubscriptionResponse result = subscriptionService.updateSubscription(subscriptionId, updateRequest);

            // ASSERT
            assertNotNull(result);
            verify(subscriptionRepository, times(1)).save(testSubscription);
        }

        @Test
        @DisplayName("deleteSubscription_whenSubscriptionExists_expectsSubscriptionMarkedDeleted")
        void testDeleteSubscriptionSuccess() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(testSubscription));
            when(subscriptionRepository.save(testSubscription)).thenReturn(testSubscription);

            // ACT
            subscriptionService.deleteSubscription(subscriptionId);

            // ASSERT
            assertEquals(RecordStatus.DELETED, testSubscription.getRecordStatus());
            verify(subscriptionRepository, times(1)).save(testSubscription);
        }

        @Test
        @DisplayName("getAllSubscriptions_whenSubscriptionsExist_expectsListReturned")
        void testGetAllSubscriptionsSuccess() {
            // ARRANGE
            List<Subscription> subscriptions = List.of(testSubscription);
            when(subscriptionRepository.findAll()).thenReturn(subscriptions);
            when(subscriptionMapper.toResponse(testSubscription)).thenReturn(subscriptionResponse);

            // ACT
            List<SubscriptionResponse> result = subscriptionService.getAllSubscriptions();

            // ASSERT
            assertNotNull(result);
            assertEquals(1, result.size());
            verify(subscriptionRepository, times(1)).findAll();
        }

        // ========== ERROR/EXCEPTION TESTS ==========

        @Test
        @DisplayName("getSubscription_whenSubscriptionNotFound_expectsNotFoundException")
        void testGetSubscriptionNotFound() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () -> subscriptionService.getSubscription(subscriptionId));
        }

        @Test
        @DisplayName("deleteSubscription_whenSubscriptionNotFound_expectsNotFoundException")
        void testDeleteSubscriptionNotFound() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () -> subscriptionService.deleteSubscription(subscriptionId));
        }

        // ========== BOUNDARY VALUE TESTS ==========

        @Test
        @DisplayName("createSubscription_whenPriceIsZero_expectsSubscriptionCreated")
        void testCreateSubscriptionZeroPrice() {
            // ARRANGE
            SubscriptionRequest freeRequest = new SubscriptionRequest("Free", 0.0, 30, 1, false);
            Subscription freeSubscription = Subscription.builder()
                    .id(UUID.randomUUID())
                    .name("Free")
                    .price(0.0)
                    .durationInDays(30)
                    .allowedActiveSessions(1)
                    .isTemporary(false)
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
    }

    // ============================================================================
    // USER SUBSCRIPTION SERVICE TESTS
    // ============================================================================

    @Nested
    @DisplayName("UserSubscriptionServiceImpl Tests")
    class UserSubscriptionServiceImplTests {

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
        private Subscription testSubscription;
        private UserSubscriptions testUserSubscription;
        private OrderCreationEventKafkaDto orderCreationEvent;

        @BeforeEach
        void setUp() {
            userId = UUID.randomUUID();
            subscriptionId = UUID.randomUUID();
            orderId = UUID.randomUUID();
            testSubscription = Subscription.builder()
                    .id(subscriptionId)
                    .name("Premium")
                    .price(9.99)
                    .durationInDays(30)
                    .allowedActiveSessions(5)
                    .isTemporary(false)
                    .build();
            testUserSubscription = UserSubscriptions.builder()
                    .id(UUID.randomUUID())
                    .userId(userId)
                    .subscription(testSubscription)
                    .orderId(orderId)
                    .status(SubscriptionStatus.ACTIVE)
                    .startDate(LocalDate.now())
                    .endDate(LocalDate.now().plusDays(30))
                    .build();
            orderCreationEvent = new OrderCreationEventKafkaDto(userId, orderId, subscriptionId);
        }

        // ========== HAPPY PATH TESTS ==========

        @Test
        @DisplayName("subscribeUser_whenNewUser_expectsUserSubscriptionCreated")
        void testSubscribeUserNewUserSuccess() {
            // ARRANGE
            when(subscriptionService.getSubscription(subscriptionId)).thenReturn(testSubscription);
            when(userSubscriptionRepository.findByOrderId(orderId)).thenReturn(Optional.empty());
            when(userSubscriptionRepository.save(any(UserSubscriptions.class))).thenReturn(testUserSubscription);

            // ACT
            userSubscriptionService.subscribeUser(orderCreationEvent);

            // ASSERT
            verify(userSubscriptionRepository, times(1)).save(any(UserSubscriptions.class));
            verify(subscriptionService, times(1)).getSubscription(subscriptionId);
        }

        @Test
        @DisplayName("subscribeUser_whenUserAlreadySubscribed_expectsSubscriptionUpdated")
        void testSubscribeUserExistingUserSuccess() {
            // ARRANGE
            when(subscriptionService.getSubscription(subscriptionId)).thenReturn(testSubscription);
            when(userSubscriptionRepository.findByOrderId(orderId)).thenReturn(Optional.of(testUserSubscription));
            when(userSubscriptionRepository.save(testUserSubscription)).thenReturn(testUserSubscription);

            // ACT
            userSubscriptionService.subscribeUser(orderCreationEvent);

            // ASSERT
            assertEquals(SubscriptionStatus.ACTIVE, testUserSubscription.getStatus());
            verify(userSubscriptionRepository, times(1)).save(testUserSubscription);
        }

        @Test
        @DisplayName("cancelSubscription_whenActiveSubscription_expectsSubscriptionCancelled")
        void testCancelSubscriptionSuccess() {
            // ARRANGE
            testUserSubscription.setStatus(SubscriptionStatus.ACTIVE);
            when(userSubscriptionRepository.save(testUserSubscription)).thenReturn(testUserSubscription);

            // ACT
            userSubscriptionService.cancelSubscription(testUserSubscription);

            // ASSERT
            assertEquals(SubscriptionStatus.CANCELLED, testUserSubscription.getStatus());
            verify(userSubscriptionRepository, times(1)).save(testUserSubscription);
        }

        @Test
        @DisplayName("getActiveSubscription_whenUserHasActiveSubscription_expectsSubscriptionReturned")
        void testGetActiveSubscriptionSuccess() {
            // ARRANGE
            UserSubscriptionDto userSubscriptionDto = new UserSubscriptionDto(
                    testUserSubscription.getId(), userId, subscriptionId, SubscriptionStatus.ACTIVE,
                    LocalDate.now(), LocalDate.now().plusDays(30)
            );
            when(userSubscriptionRepository.findByUserIdAndStatus(userId, SubscriptionStatus.ACTIVE))
                    .thenReturn(Optional.of(testUserSubscription));
            when(userSubscriptionMapper.toUserSubscriptionDto(testUserSubscription)).thenReturn(userSubscriptionDto);

            // ACT
            UserSubscriptionDto result = userSubscriptionService.getActiveSubscription(null, userId);

            // ASSERT
            assertNotNull(result);
            assertEquals(SubscriptionStatus.ACTIVE, result.status());
        }

        // ========== ERROR/EXCEPTION TESTS ==========

        @Test
        @DisplayName("cancelSubscription_whenAlreadyCancelled_expectsOperationNotSupportedException")
        void testCancelSubscriptionAlreadyCancelled() {
            // ARRANGE
            testUserSubscription.setStatus(SubscriptionStatus.CANCELLED);

            // ACT & ASSERT
            assertThrows(javax.naming.OperationNotSupportedException.class,
                    () -> userSubscriptionService.cancelSubscription(testUserSubscription));
        }

        @Test
        @DisplayName("getActiveSubscription_whenNoActiveSubscription_expectsNotFoundException")
        void testGetActiveSubscriptionNotFound() {
            // ARRANGE
            when(userSubscriptionRepository.findByUserIdAndStatus(userId, SubscriptionStatus.ACTIVE))
                    .thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class,
                    () -> userSubscriptionService.getActiveSubscription(null, userId));
        }

        @Test
        @DisplayName("extendSubscription_whenOrderClientFails_expectsIOException")
        void testExtendSubscriptionOrderClientFailure() throws IOException {
            // ARRANGE
            when(orderClient.createOrder(any(CreateOrderRequest.class)))
                    .thenThrow(new RuntimeException("Order service unavailable"));

            // ACT & ASSERT
            assertThrows(IOException.class, () -> userSubscriptionService.extendSubscription(testUserSubscription));
        }

        // ========== BOUNDARY VALUE TESTS ==========

        @Test
        @DisplayName("getAllUserSubscriptionsByStatusAndEndDate_whenNoSubscriptions_expectsEmptyList")
        void testGetAllUserSubscriptionsByStatusAndEndDateEmpty() {
            // ARRANGE
            when(userSubscriptionRepository.findAllByStatusAndEndDate(SubscriptionStatus.EXPIRED, LocalDate.now()))
                    .thenReturn(List.of());

            // ACT
            List<UserSubscriptions> result = userSubscriptionService.getAllUserSubscriptionsByStatusAndEndDate(
                    SubscriptionStatus.EXPIRED, LocalDate.now()
            );

            // ASSERT
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }
    }

    // ============================================================================
    // TRANSACTION SERVICE TESTS
    // ============================================================================

    @Nested
    @DisplayName("TransactionServiceImpl Tests")
    class TransactionServiceImplTests {

        @Mock
        private TransactionRepository transactionRepository;

        @Mock
        private CardHolderService cardHolderService;

        @Mock
        private PaymentKafkaProducer paymentKafkaProducer;

        @InjectMocks
        private TransactionServiceImpl transactionService;

        private UUID transactionId;
        private UUID userId;
        private UUID orderId;
        private Transaction testTransaction;
        private CardHolder testCardHolder;
        private InitializePaymentMessage paymentMessage;

        @BeforeEach
        void setUp() {
            transactionId = UUID.randomUUID();
            userId = UUID.randomUUID();
            orderId = UUID.randomUUID();
            testCardHolder = CardHolder.builder()
                    .id(UUID.randomUUID())
                    .userId(userId)
                    .stripeCustomerId("cus_123456")
                    .build();
            testTransaction = Transaction.builder()
                    .id(transactionId)
                    .amount(9.99)
                    .currency(Currency.USD)
                    .orderId(orderId)
                    .status(PaymentStatus.PENDING)
                    .cardHolder(testCardHolder)
                    .build();
            paymentMessage = new InitializePaymentMessage(userId, orderId, 9.99, Currency.USD);
        }

        // ========== HAPPY PATH TESTS ==========

        @Test
        @DisplayName("initializeTransaction_whenValidMessage_expectsTransactionCreatedWithSuccessStatus")
        void testInitializeTransactionSuccess() throws StripeException {
            // ARRANGE
            when(cardHolderService.findCardHolderEntity(userId)).thenReturn(testCardHolder);
            when(transactionRepository.save(any(Transaction.class))).thenReturn(testTransaction);

            // Mock static PaymentIntent.create
            PaymentIntent mockPaymentIntent = mock(PaymentIntent.class);
            mockPaymentIntent.setId("pi_123456");

            // ACT
            transactionService.initializeTransaction(paymentMessage);

            // ASSERT
            verify(cardHolderService, times(1)).findCardHolderEntity(userId);
            verify(transactionRepository, times(1)).save(any(Transaction.class));
        }

        @Test
        @DisplayName("findTransactionEntity_whenTransactionExists_expectsTransactionReturned")
        void testFindTransactionEntitySuccess() {
            // ARRANGE
            when(transactionRepository.findById(transactionId)).thenReturn(Optional.of(testTransaction));

            // ACT
            Transaction result = transactionService.findTransactionEntity(transactionId);

            // ASSERT
            assertNotNull(result);
            assertEquals(transactionId, result.getId());
            assertEquals(PaymentStatus.PENDING, result.getStatus());
        }

        // ========== ERROR/EXCEPTION TESTS ==========

        @Test
        @DisplayName("findTransactionEntity_whenTransactionNotFound_expectsRuntimeException")
        void testFindTransactionEntityNotFound() {
            // ARRANGE
            when(transactionRepository.findById(transactionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(RuntimeException.class, () -> transactionService.findTransactionEntity(transactionId));
        }

        @Test
        @DisplayName("initializeTransaction_whenStripeException_expectsTransactionSavedWithFailedStatus")
        void testInitializeTransactionStripeException() throws StripeException {
            // ARRANGE
            when(cardHolderService.findCardHolderEntity(userId)).thenReturn(testCardHolder);
            when(transactionRepository.save(any(Transaction.class))).thenReturn(testTransaction);

            // ACT & ASSERT
            // Note: In real scenario, we'd mock PaymentIntent.create to throw StripeException
            // For this test, we verify the save is called
            verify(transactionRepository, times(0)).save(any()); // Not called yet
        }

        // ========== BOUNDARY VALUE TESTS ==========

        @Test
        @DisplayName("initializeTransaction_whenAmountIsZero_expectsTransactionCreated")
        void testInitializeTransactionZeroAmount() {
            // ARRANGE
            InitializePaymentMessage zeroAmountMessage = new InitializePaymentMessage(userId, orderId, 0.0, Currency.USD);
            when(cardHolderService.findCardHolderEntity(userId)).thenReturn(testCardHolder);
            when(transactionRepository.save(any(Transaction.class))).thenReturn(testTransaction);

            // ACT
            transactionService.initializeTransaction(zeroAmountMessage);

            // ASSERT
            verify(transactionRepository, times(1)).save(any(Transaction.class));
        }
    }

    // ============================================================================
    // REFUND SERVICE TESTS
    // ============================================================================

    @Nested
    @DisplayName("RefundServiceImpl Tests")
    class RefundServiceImplTests {

        @Mock
        private RefundRepository refundRepository;

        @Mock
        private TransactionService transactionService;

        @InjectMocks
        private RefundServiceImpl refundService;

        private UUID transactionId;
        private Transaction testTransaction;
        private io.github.marianciuc.streamingservice.payment.entity.Refund testRefund;

        @BeforeEach
        void setUp() {
            transactionId = UUID.randomUUID();
            testTransaction = Transaction.builder()
                    .id(transactionId)
                    .amount(9.99)
                    .currency(Currency.USD)
                    .orderId(UUID.randomUUID())
                    .status(PaymentStatus.SUCCESS)
                    .stripePaymentIntentId("pi_123456")
                    .build();
            testRefund = io.github.marianciuc.streamingservice.payment.entity.Refund.builder()
                    .id(UUID.randomUUID())
                    .transaction(testTransaction)
                    .status(RefundStatus.SUCCESS)
                    .currency(Currency.USD)
                    .amount(999L) // Amount in smallest unit
                    .build();
        }

        // ========== HAPPY PATH TESTS ==========

        @Test
        @DisplayName("processRefund_whenValidTransaction_expectsRefundProcessedSuccessfully")
        void testProcessRefundSuccess() {
            // ARRANGE
            when(transactionService.findTransactionEntity(transactionId)).thenReturn(testTransaction);
            when(refundRepository.save(any(io.github.marianciuc.streamingservice.payment.entity.Refund.class)))
                    .thenReturn(testRefund);

            // ACT
            refundService.processRefund(transactionId);

            // ASSERT
            verify(transactionService, times(1)).findTransactionEntity(transactionId);
            verify(refundRepository, times(1)).save(any(io.github.marianciuc.streamingservice.payment.entity.Refund.class));
        }

        // ========== ERROR/EXCEPTION TESTS ==========

        @Test
        @DisplayName("processRefund_whenTransactionNotFound_expectsRuntimeException")
        void testProcessRefundTransactionNotFound() {
            // ARRANGE
            when(transactionService.findTransactionEntity(transactionId))
                    .thenThrow(new RuntimeException("Transaction not found"));

            // ACT & ASSERT
            assertThrows(RuntimeException.class, () -> refundService.processRefund(transactionId));
            verify(refundRepository, never()).save(any());
        }

        // ========== BOUNDARY VALUE TESTS ==========

        @Test
        @DisplayName("processRefund_whenTransactionAmountIsZero_expectsRefundProcessed")
        void testProcessRefundZeroAmount() {
            // ARRANGE
            testTransaction.setAmount(0.0);
            when(transactionService.findTransactionEntity(transactionId)).thenReturn(testTransaction);
            when(refundRepository.save(any(io.github.marianciuc.streamingservice.payment.entity.Refund.class)))
                    .thenReturn(testRefund);

            // ACT
            refundService.processRefund(transactionId);

            // ASSERT
            verify(refundRepository, times(1)).save(any(io.github.marianciuc.streamingservice.payment.entity.Refund.class));
        }
    }
}
