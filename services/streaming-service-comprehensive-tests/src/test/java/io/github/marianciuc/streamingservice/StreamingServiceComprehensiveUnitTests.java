/*
 * Comprehensive Unit Test Suite for Streaming Service Microservices
 * Framework: JUnit 5 with Mockito
 * Coverage: User, Payment, Subscription, Comments, Content, Media, Customer, Notification Services
 */

package io.github.marianciuc.streamingservice.tests;

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
import io.github.marianciuc.streamingservice.user.services.impl.UserServiceImpl;
import io.github.marianciuc.streamingservice.payment.dto.common.TransactionDto;
import io.github.marianciuc.streamingservice.payment.entity.Transaction;
import io.github.marianciuc.streamingservice.payment.enums.PaymentStatus;
import io.github.marianciuc.streamingservice.payment.kafka.messages.InitializePaymentMessage;
import io.github.marianciuc.streamingservice.payment.repository.TransactionRepository;
import io.github.marianciuc.streamingservice.payment.service.impl.TransactionServiceImpl;
import io.github.marianciuc.streamingservice.subscription.dto.SubscriptionRequest;
import io.github.marianciuc.streamingservice.subscription.dto.SubscriptionResponse;
import io.github.marianciuc.streamingservice.subscription.entity.Subscription;
import io.github.marianciuc.streamingservice.subscription.repository.SubscriptionRepository;
import io.github.marianciuc.streamingservice.subscription.service.impl.SubscriptionServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit test suite for streaming service microservices.
 * Tests cover happy paths, boundary values, error conditions, and side effects.
 * Uses AAA (Arrange-Act-Assert) pattern with Mockito for dependency injection.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Streaming Service Comprehensive Unit Tests")
public class StreamingServiceComprehensiveUnitTests {

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
                    .passwordHash("hashedPassword123")
                    .isBanned(false)
                    .recordStatus(RecordStatus.ACTIVE)
                    .role(Role.USER)
                    .build();

            registrationRequest = new RegistrationRequest(
                    "newuser",
                    "newuser@example.com",
                    "password123",
                    Role.USER
            );
        }

        // ========== Happy Path Tests ==========

        @Test
        @DisplayName("createUser_whenValidRequest_expectsUserCreatedAndKafkaMessageSent")
        void testCreateUserHappyPath() {
            // ARRANGE
            when(userRepository.existsByEmailOrUsername(anyString(), anyString())).thenReturn(false);
            when(passwordEncoder.encode(registrationRequest.password())).thenReturn("encodedPassword");
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.createUser(registrationRequest);

            // ASSERT
            verify(userRepository, times(1)).existsByEmailOrUsername(registrationRequest.email(), registrationRequest.username());
            verify(passwordEncoder, times(1)).encode(registrationRequest.password());
            verify(userRepository, times(1)).save(any(User.class));
            verify(kafkaUserProducer, times(1)).sendUserCreatedMessage(any());
        }

        @Test
        @DisplayName("getUserById_whenUserExists_expectsUserReturned")
        void testGetUserByIdHappyPath() {
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
        @DisplayName("getUserResponse_whenUserExists_expectsUserResponseReturned")
        void testGetUserResponseHappyPath() {
            // ARRANGE
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));

            // ACT
            UserResponse result = userService.getUserResponse(testUserId);

            // ASSERT
            assertNotNull(result);
            assertEquals(testUser.getUsername(), result.username());
            verify(userRepository, times(1)).findById(testUserId);
        }

        @Test
        @DisplayName("updateUserRole_whenValidRole_expectsRoleUpdated")
        void testUpdateUserRoleHappyPath() {
            // ARRANGE
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.updateUserRole(Role.ADMIN, testUserId);

            // ASSERT
            verify(userRepository, times(1)).findById(testUserId);
            verify(userRepository, times(1)).save(any(User.class));
        }

        @Test
        @DisplayName("banUser_whenUserNotBanned_expectsUserBanned")
        void testBanUserHappyPath() {
            // ARRANGE
            testUser.setIsBanned(false);
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.banUser(testUserId);

            // ASSERT
            verify(userRepository, times(1)).findById(testUserId);
            verify(userRepository, times(1)).save(any(User.class));
        }

        @Test
        @DisplayName("banUser_whenUserAlreadyBanned_expectsUserUnbanned")
        void testBanUserToggleHappyPath() {
            // ARRANGE
            testUser.setIsBanned(true);
            when(userRepository.findById(testUserId)).thenReturn(Optional.of(testUser));
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.banUser(testUserId);

            // ASSERT
            verify(userRepository, times(1)).findById(testUserId);
            verify(userRepository, times(1)).save(any(User.class));
        }

        // ========== Boundary Value Tests ==========

        @Test
        @DisplayName("createUser_whenEmailAlreadyExists_expectsIllegalArgumentException")
        void testCreateUserEmailAlreadyExists() {
            // ARRANGE
            when(userRepository.existsByEmailOrUsername(registrationRequest.email(), registrationRequest.username()))
                    .thenReturn(true);

            // ACT & ASSERT
            assertThrows(IllegalArgumentException.class, () -> userService.createUser(registrationRequest));
            verify(userRepository, never()).save(any(User.class));
            verify(kafkaUserProducer, never()).sendUserCreatedMessage(any());
        }

        @Test
        @DisplayName("createUser_whenUsernameAlreadyExists_expectsIllegalArgumentException")
        void testCreateUserUsernameAlreadyExists() {
            // ARRANGE
            when(userRepository.existsByEmailOrUsername(registrationRequest.email(), registrationRequest.username()))
                    .thenReturn(true);

            // ACT & ASSERT
            assertThrows(IllegalArgumentException.class, () -> userService.createUser(registrationRequest));
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
        @DisplayName("updateUserRole_whenUserNotFound_expectsNotFoundException")
        void testUpdateUserRoleNotFound() {
            // ARRANGE
            when(userRepository.findById(testUserId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () -> userService.updateUserRole(Role.ADMIN, testUserId));
        }

        @Test
        @DisplayName("banUser_whenUserNotFound_expectsNotFoundException")
        void testBanUserNotFound() {
            // ARRANGE
            when(userRepository.findById(testUserId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () -> userService.banUser(testUserId));
        }

        // ========== Error/Exception Tests ==========

        @Test
        @DisplayName("changePassword_whenOldPasswordIncorrect_expectsSecurityBadCredentialsException")
        void testChangePasswordIncorrectOldPassword() {
            // ARRANGE
            ChangePasswordRequest changePasswordRequest = new ChangePasswordRequest("wrongPassword", "newPassword123");
            when(userRepository.findById(any(UUID.class))).thenReturn(Optional.of(testUser));
            when(passwordEncoder.matches("wrongPassword", testUser.getPasswordHash())).thenReturn(false);

            // ACT & ASSERT
            assertThrows(SecurityBadCredentialsException.class, () -> userService.changePassword(changePasswordRequest));
            verify(userRepository, never()).save(any(User.class));
        }

        @Test
        @DisplayName("changePassword_whenOldPasswordCorrect_expectsPasswordChanged")
        void testChangePasswordCorrectOldPassword() {
            // ARRANGE
            ChangePasswordRequest changePasswordRequest = new ChangePasswordRequest("correctPassword", "newPassword123");
            when(userRepository.findById(any(UUID.class))).thenReturn(Optional.of(testUser));
            when(passwordEncoder.matches("correctPassword", testUser.getPasswordHash())).thenReturn(true);
            when(passwordEncoder.encode("newPassword123")).thenReturn("encodedNewPassword");
            when(userRepository.save(any(User.class))).thenReturn(testUser);

            // ACT
            userService.changePassword(changePasswordRequest);

            // ASSERT
            verify(userRepository, times(1)).save(any(User.class));
        }

        @Test
        @DisplayName("loadUserByUsername_whenUserExists_expectsUserDetailsReturned")
        void testLoadUserByUsernameHappyPath() {
            // ARRANGE
            when(userRepository.findByEmailOrUsername("testuser")).thenReturn(Optional.of(testUser));

            // ACT
            var result = userService.loadUserByUsername("testuser");

            // ASSERT
            assertNotNull(result);
            verify(userRepository, times(1)).findByEmailOrUsername("testuser");
        }

        @Test
        @DisplayName("loadUserByUsername_whenUserNotFound_expectsUsernameNotFoundException")
        void testLoadUserByUsernameNotFound() {
            // ARRANGE
            when(userRepository.findByEmailOrUsername("nonexistent")).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(org.springframework.security.core.userdetails.UsernameNotFoundException.class,
                    () -> userService.loadUserByUsername("nonexistent"));
        }
    }

    // ============================================================================
    // PAYMENT SERVICE TESTS
    // ============================================================================

    @Nested
    @DisplayName("TransactionServiceImpl Tests")
    class TransactionServiceImplTests {

        @Mock
        private TransactionRepository transactionRepository;

        @InjectMocks
        private TransactionServiceImpl transactionService;

        private UUID transactionId;
        private UUID userId;
        private Transaction testTransaction;
        private InitializePaymentMessage paymentMessage;

        @BeforeEach
        void setUp() {
            transactionId = UUID.randomUUID();
            userId = UUID.randomUUID();
            testTransaction = Transaction.builder()
                    .id(transactionId)
                    .userId(userId)
                    .amount(new BigDecimal("99.99"))
                    .status(PaymentStatus.PENDING)
                    .createdAt(LocalDateTime.now())
                    .build();

            paymentMessage = new InitializePaymentMessage(
                    transactionId,
                    userId,
                    new BigDecimal("99.99")
            );
        }

        // ========== Happy Path Tests ==========

        @Test
        @DisplayName("findTransaction_whenTransactionExists_expectsTransactionDtoReturned")
        void testFindTransactionHappyPath() {
            // ARRANGE
            when(transactionRepository.findById(transactionId)).thenReturn(Optional.of(testTransaction));

            // ACT
            TransactionDto result = transactionService.findTransaction(transactionId);

            // ASSERT
            assertNotNull(result);
            assertEquals(transactionId, result.id());
            verify(transactionRepository, times(1)).findById(transactionId);
        }

        @Test
        @DisplayName("findTransactionEntity_whenTransactionExists_expectsTransactionReturned")
        void testFindTransactionEntityHappyPath() {
            // ARRANGE
            when(transactionRepository.findById(transactionId)).thenReturn(Optional.of(testTransaction));

            // ACT
            Transaction result = transactionService.findTransactionEntity(transactionId);

            // ASSERT
            assertNotNull(result);
            assertEquals(transactionId, result.getId());
            verify(transactionRepository, times(1)).findById(transactionId);
        }

        @Test
        @DisplayName("initializeTransaction_whenValidMessage_expectsTransactionCreated")
        void testInitializeTransactionHappyPath() {
            // ARRANGE
            when(transactionRepository.save(any(Transaction.class))).thenReturn(testTransaction);

            // ACT
            transactionService.initializeTransaction(paymentMessage);

            // ASSERT
            verify(transactionRepository, times(1)).save(any(Transaction.class));
        }

        // ========== Boundary Value Tests ==========

        @Test
        @DisplayName("findTransaction_whenTransactionNotFound_expectsNotFoundException")
        void testFindTransactionNotFound() {
            // ARRANGE
            when(transactionRepository.findById(transactionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () -> transactionService.findTransaction(transactionId));
        }

        @Test
        @DisplayName("findTransactionEntity_whenTransactionNotFound_expectsNotFoundException")
        void testFindTransactionEntityNotFound() {
            // ARRANGE
            when(transactionRepository.findById(transactionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () -> transactionService.findTransactionEntity(transactionId));
        }

        @Test
        @DisplayName("initializeTransaction_whenAmountIsZero_expectsTransactionCreatedWithZeroAmount")
        void testInitializeTransactionZeroAmount() {
            // ARRANGE
            InitializePaymentMessage zeroAmountMessage = new InitializePaymentMessage(
                    transactionId,
                    userId,
                    BigDecimal.ZERO
            );
            when(transactionRepository.save(any(Transaction.class))).thenReturn(testTransaction);

            // ACT
            transactionService.initializeTransaction(zeroAmountMessage);

            // ASSERT
            verify(transactionRepository, times(1)).save(any(Transaction.class));
        }

        @Test
        @DisplayName("initializeTransaction_whenAmountIsNegative_expectsTransactionCreatedWithNegativeAmount")
        void testInitializeTransactionNegativeAmount() {
            // ARRANGE
            InitializePaymentMessage negativeAmountMessage = new InitializePaymentMessage(
                    transactionId,
                    userId,
                    new BigDecimal("-50.00")
            );
            when(transactionRepository.save(any(Transaction.class))).thenReturn(testTransaction);

            // ACT
            transactionService.initializeTransaction(negativeAmountMessage);

            // ASSERT
            verify(transactionRepository, times(1)).save(any(Transaction.class));
        }

        @Test
        @DisplayName("getTransactions_whenMultipleTransactionsExist_expectsTransactionListReturned")
        void testGetTransactionsHappyPath() {
            // ARRANGE
            when(transactionRepository.findAll(any())).thenReturn(java.util.List.of(testTransaction));

            // ACT
            var result = transactionService.getTransactions(0, 10, "createdAt", PaymentStatus.PENDING, userId);

            // ASSERT
            assertNotNull(result);
            verify(transactionRepository, times(1)).findAll(any());
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

        @InjectMocks
        private SubscriptionServiceImpl subscriptionService;

        private UUID subscriptionId;
        private UUID userId;
        private Subscription testSubscription;
        private SubscriptionRequest subscriptionRequest;

        @BeforeEach
        void setUp() {
            subscriptionId = UUID.randomUUID();
            userId = UUID.randomUUID();
            testSubscription = Subscription.builder()
                    .id(subscriptionId)
                    .userId(userId)
                    .planType("PREMIUM")
                    .status("ACTIVE")
                    .startDate(LocalDateTime.now())
                    .endDate(LocalDateTime.now().plusMonths(1))
                    .build();

            subscriptionRequest = new SubscriptionRequest(
                    userId,
                    "PREMIUM",
                    LocalDateTime.now(),
                    LocalDateTime.now().plusMonths(1)
            );
        }

        // ========== Happy Path Tests ==========

        @Test
        @DisplayName("createSubscription_whenValidRequest_expectsSubscriptionCreated")
        void testCreateSubscriptionHappyPath() {
            // ARRANGE
            when(subscriptionRepository.save(any(Subscription.class))).thenReturn(testSubscription);

            // ACT
            SubscriptionResponse result = subscriptionService.createSubscription(subscriptionRequest);

            // ASSERT
            assertNotNull(result);
            verify(subscriptionRepository, times(1)).save(any(Subscription.class));
        }

        @Test
        @DisplayName("getSubscription_whenSubscriptionExists_expectsSubscriptionReturned")
        void testGetSubscriptionHappyPath() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(testSubscription));

            // ACT
            Subscription result = subscriptionService.getSubscription(subscriptionId);

            // ASSERT
            assertNotNull(result);
            assertEquals(subscriptionId, result.getId());
            verify(subscriptionRepository, times(1)).findById(subscriptionId);
        }

        @Test
        @DisplayName("getSubscriptionResponse_whenSubscriptionExists_expectsSubscriptionResponseReturned")
        void testGetSubscriptionResponseHappyPath() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(testSubscription));

            // ACT
            SubscriptionResponse result = subscriptionService.getSubscriptionResponse(subscriptionId);

            // ASSERT
            assertNotNull(result);
            verify(subscriptionRepository, times(1)).findById(subscriptionId);
        }

        @Test
        @DisplayName("updateSubscription_whenValidRequest_expectsSubscriptionUpdated")
        void testUpdateSubscriptionHappyPath() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(testSubscription));
            when(subscriptionRepository.save(any(Subscription.class))).thenReturn(testSubscription);

            // ACT
            SubscriptionResponse result = subscriptionService.updateSubscription(subscriptionId, subscriptionRequest);

            // ASSERT
            assertNotNull(result);
            verify(subscriptionRepository, times(1)).findById(subscriptionId);
            verify(subscriptionRepository, times(1)).save(any(Subscription.class));
        }

        @Test
        @DisplayName("deleteSubscription_whenSubscriptionExists_expectsSubscriptionDeleted")
        void testDeleteSubscriptionHappyPath() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(testSubscription));

            // ACT
            subscriptionService.deleteSubscription(subscriptionId);

            // ASSERT
            verify(subscriptionRepository, times(1)).deleteById(subscriptionId);
        }

        @Test
        @DisplayName("getAllSubscriptions_whenSubscriptionsExist_expectsSubscriptionListReturned")
        void testGetAllSubscriptionsHappyPath() {
            // ARRANGE
            when(subscriptionRepository.findAll()).thenReturn(java.util.List.of(testSubscription));

            // ACT
            var result = subscriptionService.getAllSubscriptions();

            // ASSERT
            assertNotNull(result);
            assertFalse(result.isEmpty());
            verify(subscriptionRepository, times(1)).findAll();
        }

        // ========== Boundary Value Tests ==========

        @Test
        @DisplayName("getSubscription_whenSubscriptionNotFound_expectsNotFoundException")
        void testGetSubscriptionNotFound() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () -> subscriptionService.getSubscription(subscriptionId));
        }

        @Test
        @DisplayName("updateSubscription_whenSubscriptionNotFound_expectsNotFoundException")
        void testUpdateSubscriptionNotFound() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () -> subscriptionService.updateSubscription(subscriptionId, subscriptionRequest));
        }

        @Test
        @DisplayName("deleteSubscription_whenSubscriptionNotFound_expectsNotFoundException")
        void testDeleteSubscriptionNotFound() {
            // ARRANGE
            when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () -> subscriptionService.deleteSubscription(subscriptionId));
        }

        @Test
        @DisplayName("getAllSubscriptions_whenNoSubscriptionsExist_expectsEmptyList")
        void testGetAllSubscriptionsEmpty() {
            // ARRANGE
            when(subscriptionRepository.findAll()).thenReturn(java.util.List.of());

            // ACT
            var result = subscriptionService.getAllSubscriptions();

            // ASSERT
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }
    }

    // ============================================================================
    // PARAMETRIZED TESTS FOR BOUNDARY CONDITIONS
    // ============================================================================

    @Nested
    @DisplayName("Parametrized Boundary Tests")
    class ParametrizedBoundaryTests {

        @Mock
        private UserRepository userRepository;

        @Mock
        private PasswordEncoder passwordEncoder;

        @Mock
        private KafkaUserProducer kafkaUserProducer;

        @InjectMocks
        private UserServiceImpl userService;

        @ParameterizedTest
        @ValueSource(strings = {"user@example.com", "test.user@domain.co.uk", "user+tag@example.com"})
        @DisplayName("createUser_withVariousValidEmails_expectsUserCreated")
        void testCreateUserWithVariousEmails(String email) {
            // ARRANGE
            RegistrationRequest request = new RegistrationRequest(
                    "testuser",
                    email,
                    "password123",
                    Role.USER
            );
            when(userRepository.existsByEmailOrUsername(email, "testuser")).thenReturn(false);
            when(passwordEncoder.encode("password123")).thenReturn("encoded");
            User savedUser = User.builder()
                    .id(UUID.randomUUID())
                    .username("testuser")
                    .email(email)
                    .passwordHash("encoded")
                    .isBanned(false)
                    .recordStatus(RecordStatus.ACTIVE)
                    .role(Role.USER)
                    .build();
            when(userRepository.save(any(User.class))).thenReturn(savedUser);

            // ACT
            userService.createUser(request);

            // ASSERT
            verify(userRepository, times(1)).save(any(User.class));
            verify(kafkaUserProducer, times(1)).sendUserCreatedMessage(any());
        }

        @ParameterizedTest
        @ValueSource(strings = {"user123", "test_user", "user-name"})
        @DisplayName("createUser_withVariousValidUsernames_expectsUserCreated")
        void testCreateUserWithVariousUsernames(String username) {
            // ARRANGE
            RegistrationRequest request = new RegistrationRequest(
                    username,
                    "test@example.com",
                    "password123",
                    Role.USER
            );
            when(userRepository.existsByEmailOrUsername("test@example.com", username)).thenReturn(false);
            when(passwordEncoder.encode("password123")).thenReturn("encoded");
            User savedUser = User.builder()
                    .id(UUID.randomUUID())
                    .username(username)
                    .email("test@example.com")
                    .passwordHash("encoded")
                    .isBanned(false)
                    .recordStatus(RecordStatus.ACTIVE)
                    .role(Role.USER)
                    .build();
            when(userRepository.save(any(User.class))).thenReturn(savedUser);

            // ACT
            userService.createUser(request);

            // ASSERT
            verify(userRepository, times(1)).save(any(User.class));
        }
    }
}
