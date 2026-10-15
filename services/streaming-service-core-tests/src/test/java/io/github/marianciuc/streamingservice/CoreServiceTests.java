/*
 * Copyright (c) 2024 Vladimir Marianciuc. All Rights Reserved.
 *
 * Project: STREAMING SERVICE APP - CORE SERVICE UNIT TESTS
 * File: CoreServiceTests.java
 *
 * Comprehensive unit test suite for critical microservices:
 * - TransactionService (Payment Service)
 * - SubscriptionService (Subscription Service)
 * - UserService (User Service)
 *
 * Framework: JUnit 5 + Mockito
 * Coverage Target: 80%+ line, 75%+ branch
 */

package io.github.marianciuc.streamingservice;

import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import io.github.marianciuc.streamingservice.payment.dto.common.TransactionDto;
import io.github.marianciuc.streamingservice.payment.entity.CardHolder;
import io.github.marianciuc.streamingservice.payment.entity.JWTUserPrincipal;
import io.github.marianciuc.streamingservice.payment.entity.Transaction;
import io.github.marianciuc.streamingservice.payment.enums.PaymentStatus;
import io.github.marianciuc.streamingservice.payment.kafka.PaymentKafkaProducer;
import io.github.marianciuc.streamingservice.payment.kafka.messages.InitializePaymentMessage;
import io.github.marianciuc.streamingservice.payment.repository.TransactionRepository;
import io.github.marianciuc.streamingservice.payment.service.CardHolderService;
import io.github.marianciuc.streamingservice.payment.service.impl.TransactionServiceImpl;
import io.github.marianciuc.streamingservice.subscription.dto.SubscriptionRequest;
import io.github.marianciuc.streamingservice.subscription.dto.SubscriptionResponse;
import io.github.marianciuc.streamingservice.subscription.entity.RecordStatus;
import io.github.marianciuc.streamingservice.subscription.entity.Subscription;
import io.github.marianciuc.streamingservice.subscription.exceptions.NotFoundException;
import io.github.marianciuc.streamingservice.subscription.mapper.SubscriptionMapper;
import io.github.marianciuc.streamingservice.subscription.repository.SubscriptionRepository;
import io.github.marianciuc.streamingservice.subscription.service.impl.SubscriptionServiceImpl;
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
import io.github.marianciuc.streamingservice.user.kafka.messages.CreateUserMessage;
import io.github.marianciuc.streamingservice.user.repositories.UserRepository;
import io.github.marianciuc.streamingservice.user.security.utils.SecurityUtils;
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
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit test suite for streaming service core components.
 * Tests cover happy paths, boundary values, error conditions, and side effects.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Streaming Service Core Unit Tests")
public class CoreServiceTests {

    // ============================================================================
    // TRANSACTION SERVICE TESTS (Payment Service)
    // ============================================================================

    @Nested
    @DisplayName("TransactionService Tests")
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
        private UUID testTransactionId;
        private UUID testOrderId;
        private CardHolder testCardHolder;
        private InitializePaymentMessage testPaymentMessage;

        @BeforeEach
        void setUp() {
            testUserId = UUID.randomUUID();
            testTransactionId = UUID.randomUUID();
            testOrderId = UUID.randomUUID();

            testCardHolder = CardHolder.builder()
                    .id(UUID.randomUUID())
                    .stripeCustomerId("cus_test123")
                    .userId(testUserId)
                    .build();

            testPaymentMessage = new InitializePaymentMessage(
                    testUserId,
                    testOrderId,
                    BigDecimal.valueOf(99.99),
                    "USD"
            );
        }

        @Test
        @DisplayName("Happy Path: Initialize transaction with valid Stripe payment succeeds")
        void testInitializeTransaction_whenValidPayment_expectsSuccess() throws StripeException {
            // ARRANGE
            when(cardHolderService.findCardHolderEntity(testUserId)).thenReturn(testCardHolder);
            PaymentIntent mockPaymentIntent = mock(PaymentIntent.class);
            when(mockPaymentIntent.getId()).thenReturn("pi_test123");
            mockStatic(PaymentIntent.class);
            when(PaymentIntent.create(any())).thenReturn(mockPaymentIntent);

            // ACT
            transactionService.initializeTransaction(testPaymentMessage);

            // ASSERT
            ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);
            verify(transactionRepository).save(transactionCaptor.capture());
            Transaction savedTransaction = transactionCaptor.getValue();
            assertEquals(PaymentStatus.SUCCESS, savedTransaction.getStatus());
            assertEquals("pi_test123", savedTransaction.getStripePaymentIntentId());
            assertEquals(testCardHolder, savedTransaction.getCardHolder());
        }

        @Test
        @DisplayName("Error Path: Initialize transaction with Stripe exception sets FAILED status")
        void testInitializeTransaction_whenStripeException_expectsFailed() throws StripeException {
            // ARRANGE
            when(cardHolderService.findCardHolderEntity(testUserId)).thenReturn(testCardHolder);
            StripeException stripeException = new StripeException("Card declined");
            mockStatic(PaymentIntent.class);
            when(PaymentIntent.create(any())).thenThrow(stripeException);

            // ACT & ASSERT
            assertThrows(RuntimeException.class, () -> transactionService.initializeTransaction(testPaymentMessage));
            ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);
            verify(transactionRepository).save(transactionCaptor.capture());
            Transaction savedTransaction = transactionCaptor.getValue();
            assertEquals(PaymentStatus.FAILED, savedTransaction.getStatus());
            assertEquals("Card declined", savedTransaction.getFailureMessage());
        }

        @Test
        @DisplayName("Boundary: Initialize transaction with zero amount")
        void testInitializeTransaction_whenZeroAmount_expectsProcessing() throws StripeException {
            // ARRANGE
            InitializePaymentMessage zeroAmountMessage = new InitializePaymentMessage(
                    testUserId,
                    testOrderId,
                    BigDecimal.ZERO,
                    "USD"
            );
            when(cardHolderService.findCardHolderEntity(testUserId)).thenReturn(testCardHolder);
            PaymentIntent mockPaymentIntent = mock(PaymentIntent.class);
            when(mockPaymentIntent.getId()).thenReturn("pi_zero");
            mockStatic(PaymentIntent.class);
            when(PaymentIntent.create(any())).thenReturn(mockPaymentIntent);

            // ACT
            transactionService.initializeTransaction(zeroAmountMessage);

            // ASSERT
            ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);
            verify(transactionRepository).save(transactionCaptor.capture());
            assertEquals(BigDecimal.ZERO, transactionCaptor.getValue().getAmount());
        }

        @Test
        @DisplayName("Error Path: Find transaction entity when not found throws exception")
        void testFindTransactionEntity_whenNotFound_expectsException() {
            // ARRANGE
            when(transactionRepository.findById(testTransactionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(RuntimeException.class, () -> transactionService.findTransactionEntity(testTransactionId));
            verify(transactionRepository).findById(testTransactionId);
        }

        @Test
        @DisplayName("Happy Path: Find transaction entity returns valid transaction")
        void testFindTransactionEntity_whenExists_expectsReturn() {
            // ARRANGE
            Transaction expectedTransaction = Transaction.builder()
                    .id(testTransactionId)
                    .amount(BigDecimal.valueOf(99.99))
                    .status(PaymentStatus.SUCCESS)
                    .build();
            when(transactionRepository.findById(testTransactionId)).thenReturn(Optional.of(expectedTransaction));

            // ACT
            Transaction result = transactionService.findTransactionEntity(testTransactionId);

            // ASSERT
            assertEquals(expectedTransaction, result);
            verify(transactionRepository).findById(testTransactionId);
        }

        @Test
        @DisplayName("Error Path: Get transactions with null userId and non-admin user")
        void testGetTransactions_whenNonAdminUser_expectsAccessDenied() {
            // ARRANGE
            JWTUserPrincipal jwtUserPrincipal = mock(JWTUserPrincipal.class);
            when(jwtUserPrincipal.getUserId()).thenReturn(testUserId);
            when(jwtUserPrincipal.getAuthorities()).thenReturn(java.util.Collections.emptyList());

            Authentication authentication = mock(Authentication.class);
            when(authentication.getPrincipal()).thenReturn(jwtUserPrincipal);

            SecurityContext securityContext = mock(SecurityContext.class);
            when(securityContext.getAuthentication()).thenReturn(authentication);
            SecurityContextHolder.setContext(securityContext);

            // ACT & ASSERT
            assertThrows(org.springframework.security.access.AccessDeniedException.class,
                    () -> transactionService.getTransactions(0, 10, "ASC", PaymentStatus.SUCCESS, null));
        }
    }

    // ============================================================================
    // SUBSCRIPTION SERVICE TESTS (Subscription Service)
    // ============================================================================

    @Nested
    @DisplayName("SubscriptionService Tests")
    class SubscriptionServiceTests {

        @Mock
        private SubscriptionRepository subscriptionRepository;

        @Mock
        private SubscriptionMapper subscriptionMapper;

        @InjectMocks
        private SubscriptionServiceImpl subscriptionService;

        private UUID testSubscriptionId;
        private Subscription testSubscription;
        private SubscriptionRequest testSubscriptionRequest;
        private SubscriptionResponse testSubscriptionResponse;

        @BeforeEach
        void setUp() {
            testSubscriptionId = UUID.randomUUID();

            testSubscription = Subscription.builder()
                    .id(testSubscriptionId)
                    .allowedActiveSessions(3)
                    .isTemporary(false)
                    .recordStatus(RecordStatus.ACTIVE)
                    .build();

            testSubscriptionRequest = new SubscriptionRequest(3, false);
            testSubscriptionResponse = new SubscriptionResponse(testSubscriptionId, 3, false);
        }

        @Test
        @DisplayName("Happy Path: Create subscription with valid request")
        void testCreateSubscription_whenValidRequest_expectsSuccess() {
            // ARRANGE
            when(subscriptionMapper.toEntity(testSubscriptionRequest)).thenReturn(testSubscription);
            when(subscriptionRepository.save(testSubscription)).thenReturn(testSubscription);
            when(subscriptionMapper.toResponse(testSubscription)).thenReturn(testSubscriptionResponse);

            // ACT
            SubscriptionResponse result = subscriptionService.createSubscription(testSubscriptionRequest);

            // ASSERT
            assertNotNull(result);
            assertEquals(testSubscriptionResponse.id(), result.id());
            assertEquals(3, result.allowedActiveSessions());
            verify(subscriptionRepository).save(testSubscription);
            verify(subscriptionMapper).toEntity(testSubscriptionRequest);
        }

        @Test
        @DisplayName("Happy Path: Get subscription by ID returns valid subscription")
        void testGetSubscription_whenExists_expectsReturn() {
            // ARRANGE
            when(subscriptionRepository.findById(testSubscriptionId)).thenReturn(Optional.of(testSubscription));

            // ACT
            Subscription result = subscriptionService.getSubscription(testSubscriptionId);

            // ASSERT
            assertEquals(testSubscription, result);
            verify(subscriptionRepository).findById(testSubscriptionId);
        }

        @Test
        @DisplayName("Error Path: Get subscription when not found throws NotFoundException")
        void testGetSubscription_whenNotFound_expectsNotFoundException() {
            // ARRANGE
            when(subscriptionRepository.findById(testSubscriptionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () -> subscriptionService.getSubscription(testSubscriptionId));
            verify(subscriptionRepository).findById(testSubscriptionId);
        }

        @Test
        @DisplayName("Happy Path: Get subscription response by ID")
        void testGetSubscriptionResponse_whenExists_expectsResponse() {
            // ARRANGE
            when(subscriptionRepository.findById(testSubscriptionId)).thenReturn(Optional.of(testSubscription));
            when(subscriptionMapper.toResponse(testSubscription)).thenReturn(testSubscriptionResponse);

            // ACT
            SubscriptionResponse result = subscriptionService.getSubscriptionResponse(testSubscriptionId);

            // ASSERT
            assertNotNull(result);
            assertEquals(testSubscriptionResponse, result);
        }

        @Test
        @DisplayName("Happy Path: Delete subscription sets status to DELETED")
        void testDeleteSubscription_whenExists_expectsDeleted() {
            // ARRANGE
            when(subscriptionRepository.findById(testSubscriptionId)).thenReturn(Optional.of(testSubscription));
            when(subscriptionRepository.save(any(Subscription.class))).thenReturn(testSubscription);

            // ACT
            subscriptionService.deleteSubscription(testSubscriptionId);

            // ASSERT
            ArgumentCaptor<Subscription> subscriptionCaptor = ArgumentCaptor.forClass(Subscription.class);
            verify(subscriptionRepository).save(subscriptionCaptor.capture());
            assertEquals(RecordStatus.DELETED, subscriptionCaptor.getValue().getRecordStatus());
        }

        @Test
        @DisplayName("Happy Path: Update subscription with new allowed sessions")
        void testUpdateSubscription_whenSessionsChanged_expectsUpdate() {
            // ARRANGE
            SubscriptionRequest updateRequest = new SubscriptionRequest(5, false);
            when(subscriptionRepository.findById(testSubscriptionId)).thenReturn(Optional.of(testSubscription));
            when(subscriptionRepository.save(any(Subscription.class))).thenReturn(testSubscription);
            when(subscriptionMapper.toResponse(any(Subscription.class))).thenReturn(testSubscriptionResponse);

            // ACT
            subscriptionService.updateSubscription(testSubscriptionId, updateRequest);

            // ASSERT
            ArgumentCaptor<Subscription> subscriptionCaptor = ArgumentCaptor.forClass(Subscription.class);
            verify(subscriptionRepository).save(subscriptionCaptor.capture());
            assertEquals(5, subscriptionCaptor.getValue().getAllowedActiveSessions());
        }

        @Test
        @DisplayName("Boundary: Update subscription with same values does not change")
        void testUpdateSubscription_whenNoChanges_expectsNoUpdate() {
            // ARRANGE
            SubscriptionRequest sameRequest = new SubscriptionRequest(3, false);
            when(subscriptionRepository.findById(testSubscriptionId)).thenReturn(Optional.of(testSubscription));
            when(subscriptionRepository.save(any(Subscription.class))).thenReturn(testSubscription);
            when(subscriptionMapper.toResponse(any(Subscription.class))).thenReturn(testSubscriptionResponse);

            // ACT
            subscriptionService.updateSubscription(testSubscriptionId, sameRequest);

            // ASSERT
            verify(subscriptionRepository).save(testSubscription);
        }

        @Test
        @DisplayName("Happy Path: Get all subscriptions returns list")
        void testGetAllSubscriptions_whenExists_expectsList() {
            // ARRANGE
            java.util.List<Subscription> subscriptions = java.util.Arrays.asList(testSubscription);
            when(subscriptionRepository.findAll()).thenReturn(subscriptions);
            when(subscriptionMapper.toResponse(testSubscription)).thenReturn(testSubscriptionResponse);

            // ACT
            java.util.List<SubscriptionResponse> result = subscriptionService.getAllSubscriptions();

            // ASSERT
            assertNotNull(result);
            assertEquals(1, result.size());
            verify(subscriptionRepository).findAll();
        }
    }

    // ============================================================================
    // USER SERVICE TESTS (User Service)
    // ============================================================================

    @Nested
    @DisplayName("UserService Tests")
    class UserServiceTests {

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
        private RegistrationRequest testRegistrationRequest;
        private ChangePasswordRequest testChangePasswordRequest;

        @BeforeEach
        void setUp() {
            testUserId = UUID.randomUUID();

            testUser = User.builder()
                    .id(testUserId)
                    .username("testuser")
                    .email("test@example.com")
                    .passwordHash("hashedPassword123")
                    .isBanned(false)
                    .recordStatus(RecordStatus.ACTIVE)
                    .role(Role.USER)
                    .build();

            testRegistrationRequest = new RegistrationRequest(
                    "newuser",
                    "newuser@example.com",
                    "password123",
                    Role.USER
            );

            testChangePasswordRequest = new ChangePasswordRequest(
                    "oldPassword",
                    "newPassword123"
            );
        }

        @Test
        @DisplayName("Happy Path: Create user with valid registration request")
        void testCreateUser_whenValidRequest_expectsSuccess() {
            // ARRANGE
            when(userRepository.existsByEmailOrUsername(testRegistrationRequest.email(), testRegistrationRequest.username()))
                    .thenReturn(false);
            when(passwordEncoder.encode(testRegistrationRequest.password())).thenReturn("encodedPassword");
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.createUser(testRegistrationRequest);

            // ASSERT
            ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);
            verify(userRepository).save(userCaptor.capture());
            User savedUser = userCaptor.getValue();
            assertEquals(testRegistrationRequest.username(), savedUser.getUsername());
            assertEquals(testRegistrationRequest.email(), savedUser.getEmail());
            assertEquals("encodedPassword", savedUser.getPasswordHash());
            assertFalse(savedUser.getIsBanned());
            verify(kafkaUserProducer).sendUserCreatedMessage(any(CreateUserMessage.class));
        }

        @Test
        @DisplayName("Error Path: Create user with existing email throws IllegalArgumentException")
        void testCreateUser_whenEmailExists_expectsException() {
            // ARRANGE
            when(userRepository.existsByEmailOrUsername(testRegistrationRequest.email(), testRegistrationRequest.username()))
                    .thenReturn(true);

            // ACT & ASSERT
            assertThrows(IllegalArgumentException.class, () -> userService.createUser(testRegistrationRequest));
            verify(userRepository, never()).save(any(User.class));
            verify(kafkaUserProducer, never()).sendUserCreatedMessage(any(CreateUserMessage.class));
        }

        @Test
        @DisplayName("Error Path: Create user with existing username throws IllegalArgumentException")
        void testCreateUser_whenUsernameExists_expectsException() {
            // ARRANGE
            when(userRepository.existsByEmailOrUsername(testRegistrationRequest.email(), testRegistrationRequest.username()))
                    .thenReturn(true);

            // ACT & ASSERT
            assertThrows(IllegalArgumentException.class, () -> userService.createUser(testRegistrationRequest));
        }

        @Test
        @DisplayName("Happy Path: Get user by ID returns valid user")
        void testGetUserById_whenExists_expectsReturn() {
            // ARRANGE
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));

            // ACT
            User result = userService.getUserById(testUserId);

            // ASSERT
            assertEquals(testUser, result);
            verify(userRepository).findById(testUserId);
        }

        @Test
        @DisplayName("Error Path: Get user by ID when not found throws NotFoundException")
        void testGetUserById_whenNotFound_expectsNotFoundException() {
            // ARRANGE
            when(userRepository.findById(testUserId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () -> userService.getUserById(testUserId));
            verify(userRepository).findById(testUserId);
        }

        @Test
        @DisplayName("Happy Path: Get user response by ID")
        void testGetUserResponse_whenExists_expectsResponse() {
            // ARRANGE
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));

            // ACT
            UserResponse result = userService.getUserResponse(testUserId);

            // ASSERT
            assertNotNull(result);
            assertEquals(testUser.getUsername(), result.username());
            assertEquals(testUser.getEmail(), result.email());
        }

        @Test
        @DisplayName("Happy Path: Ban user toggles banned status from false to true")
        void testBanUser_whenNotBanned_expectsBanned() {
            // ARRANGE
            testUser.setIsBanned(false);
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.banUser(testUserId);

            // ASSERT
            ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);
            verify(userRepository).save(userCaptor.capture());
            assertTrue(userCaptor.getValue().getIsBanned());
        }

        @Test
        @DisplayName("Happy Path: Ban user toggles banned status from true to false (unban)")
        void testBanUser_whenBanned_expectsUnbanned() {
            // ARRANGE
            testUser.setIsBanned(true);
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.banUser(testUserId);

            // ASSERT
            ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);
            verify(userRepository).save(userCaptor.capture());
            assertFalse(userCaptor.getValue().getIsBanned());
        }

        @Test
        @DisplayName("Happy Path: Update user role successfully")
        void testUpdateUserRole_whenValidRole_expectsUpdate() {
            // ARRANGE
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.updateUserRole(Role.ADMIN, testUserId);

            // ASSERT
            ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);
            verify(userRepository).save(userCaptor.capture());
            assertEquals(Role.ADMIN, userCaptor.getValue().getRole());
        }

        @Test
        @DisplayName("Happy Path: Change password with correct old password")
        void testChangePassword_whenCorrectOldPassword_expectsSuccess() {
            // ARRANGE
            mockStatic(SecurityUtils.class);
            when(SecurityUtils.extractJwtUserPrincipals()).thenReturn(testUser);
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));
            when(passwordEncoder.matches(testChangePasswordRequest.oldPassword(), testUser.getPasswordHash()))
                    .thenReturn(true);
            when(passwordEncoder.encode(testChangePasswordRequest.newPassword())).thenReturn("newEncodedPassword");
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.changePassword(testChangePasswordRequest);

            // ASSERT
            ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);
            verify(userRepository).save(userCaptor.capture());
            assertEquals("newEncodedPassword", userCaptor.getValue().getPasswordHash());
        }

        @Test
        @DisplayName("Error Path: Change password with incorrect old password throws exception")
        void testChangePassword_whenIncorrectOldPassword_expectsException() {
            // ARRANGE
            mockStatic(SecurityUtils.class);
            when(SecurityUtils.extractJwtUserPrincipals()).thenReturn(testUser);
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));
            when(passwordEncoder.matches(testChangePasswordRequest.oldPassword(), testUser.getPasswordHash()))
                    .thenReturn(false);

            // ACT & ASSERT
            assertThrows(SecurityBadCredentialsException.class, () -> userService.changePassword(testChangePasswordRequest));
            verify(userRepository, never()).save(any(User.class));
        }

        @Test
        @DisplayName("Happy Path: Load user by username returns valid user")
        void testLoadUserByUsername_whenExists_expectsReturn() {
            // ARRANGE
            when(userRepository.findByEmailOrUsername(testUser.getUsername())).thenReturn(Optional.of(testUser));

            // ACT
            org.springframework.security.core.userdetails.UserDetails result = userService.loadUserByUsername(testUser.getUsername());

            // ASSERT
            assertNotNull(result);
            assertEquals(testUser.getUsername(), result.getUsername());
        }

        @Test
        @DisplayName("Error Path: Load user by username when not found throws UsernameNotFoundException")
        void testLoadUserByUsername_whenNotFound_expectsException() {
            // ARRANGE
            when(userRepository.findByEmailOrUsername("nonexistent")).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(org.springframework.security.core.userdetails.UsernameNotFoundException.class,
                    () -> userService.loadUserByUsername("nonexistent"));
        }

        @Test
        @DisplayName("Boundary: Create user with minimum valid password length")
        void testCreateUser_whenMinimumPasswordLength_expectsSuccess() {
            // ARRANGE
            RegistrationRequest minPasswordRequest = new RegistrationRequest(
                    "user",
                    "user@test.com",
                    "p",
                    Role.USER
            );
            when(userRepository.existsByEmailOrUsername(minPasswordRequest.email(), minPasswordRequest.username()))
                    .thenReturn(false);
            when(passwordEncoder.encode(minPasswordRequest.password())).thenReturn("encoded");
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.createUser(minPasswordRequest);

            // ASSERT
            verify(userRepository).save(any(User.class));
        }

        @Test
        @DisplayName("Boundary: Update user role multiple times")
        void testUpdateUserRole_whenMultipleTimes_expectsLastRoleSet() {
            // ARRANGE
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.updateUserRole(Role.MODERATOR, testUserId);
            userService.updateUserRole(Role.ADMIN, testUserId);

            // ASSERT
            ArgumentCaptor<User> userCaptor = ArgumentCaptor.forClass(User.class);
            verify(userRepository, times(2)).save(userCaptor.capture());
            assertEquals(Role.ADMIN, userCaptor.getValue().getRole());
        }
    }
}
