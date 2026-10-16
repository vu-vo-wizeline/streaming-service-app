package io.wizeline.streaming;

import io.wizeline.streaming.user.dto.UserRegistrationRequest;
import io.wizeline.streaming.user.dto.UserAuthRequest;
import io.wizeline.streaming.user.entity.User;
import io.wizeline.streaming.user.service.UserService;
import io.wizeline.streaming.payment.dto.PaymentRequest;
import io.wizeline.streaming.payment.dto.PaymentResponse;
import io.wizeline.streaming.payment.entity.Payment;
import io.wizeline.streaming.payment.service.PaymentService;
import io.wizeline.streaming.subscription.dto.SubscriptionRequest;
import io.wizeline.streaming.subscription.entity.Subscription;
import io.wizeline.streaming.subscription.service.SubscriptionService;
import io.wizeline.streaming.subscription.scheduler.SubscriptionRenewalScheduler;
import io.wizeline.streaming.order.dto.OrderRequest;
import io.wizeline.streaming.order.entity.Order;
import io.wizeline.streaming.order.service.OrderService;
import io.wizeline.streaming.common.event.KafkaEventPublisher;
import io.wizeline.streaming.common.exception.ValidationException;
import io.wizeline.streaming.common.exception.PaymentException;
import io.wizeline.streaming.common.exception.SubscriptionException;
import io.wizeline.streaming.common.exception.OrderException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit test suite for streaming service microservices.
 * Tests cover User Service, Payment Service, Subscription Service, and Order Service.
 * Uses JUnit 5 with Mockito for dependency injection and mocking.
 * Follows AAA (Arrange-Act-Assert) pattern for all test cases.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Streaming Service Core Tests")
public class StreamingServiceCoreTests {

    // ==================== USER SERVICE TESTS ====================

    @Mock
    private UserService.UserRepository userRepository;

    @Mock
    private PasswordEncoder passwordEncoder;

    @Mock
    private KafkaEventPublisher kafkaEventPublisher;

    @InjectMocks
    private UserService userService;

    // ==================== PAYMENT SERVICE TESTS ====================

    @Mock
    private PaymentService.PaymentRepository paymentRepository;

    @Mock
    private PaymentService.StripeClient stripeClient;

    @InjectMocks
    private PaymentService paymentService;

    // ==================== SUBSCRIPTION SERVICE TESTS ====================

    @Mock
    private SubscriptionService.SubscriptionRepository subscriptionRepository;

    @Mock
    private SubscriptionService.OrderServiceClient orderServiceClient;

    @InjectMocks
    private SubscriptionService subscriptionService;

    @InjectMocks
    private SubscriptionRenewalScheduler subscriptionRenewalScheduler;

    // ==================== ORDER SERVICE TESTS ====================

    @Mock
    private OrderService.OrderRepository orderRepository;

    @Mock
    private OrderService.SubscriptionServiceClient subscriptionServiceClient;

    @InjectMocks
    private OrderService orderService;

    @BeforeEach
    void setUp() {
        // Reset mocks before each test
        reset(userRepository, passwordEncoder, kafkaEventPublisher,
              paymentRepository, stripeClient,
              subscriptionRepository, orderServiceClient,
              orderRepository, subscriptionServiceClient);
    }

    // ==================== USER SERVICE TESTS ====================

    @Test
    @DisplayName("User Registration: Happy path - valid user data creates account and publishes event")
    void testUserRegistration_whenValidData_expectsSuccessfulCreation() {
        // ARRANGE
        UserRegistrationRequest request = new UserRegistrationRequest(
            "john.doe@example.com",
            "John Doe",
            "SecurePassword123!"
        );
        User expectedUser = new User(
            UUID.randomUUID(),
            "john.doe@example.com",
            "John Doe",
            "hashedPassword",
            LocalDateTime.now()
        );

        when(userRepository.existsByEmail(request.getEmail())).thenReturn(false);
        when(passwordEncoder.encode(request.getPassword())).thenReturn("hashedPassword");
        when(userRepository.save(any(User.class))).thenReturn(expectedUser);

        // ACT
        User result = userService.registerUser(request);

        // ASSERT
        assertNotNull(result);
        assertEquals(expectedUser.getId(), result.getId());
        assertEquals(expectedUser.getEmail(), result.getEmail());
        verify(userRepository).save(any(User.class));
        verify(kafkaEventPublisher).publishUserCreatedEvent(expectedUser);
    }

    @Test
    @DisplayName("User Registration: Boundary - duplicate email throws ValidationException")
    void testUserRegistration_whenDuplicateEmail_expectsValidationException() {
        // ARRANGE
        UserRegistrationRequest request = new UserRegistrationRequest(
            "existing@example.com",
            "John Doe",
            "SecurePassword123!"
        );
        when(userRepository.existsByEmail(request.getEmail())).thenReturn(true);

        // ACT & ASSERT
        assertThrows(ValidationException.class, () -> userService.registerUser(request));
        verify(userRepository, never()).save(any(User.class));
        verify(kafkaEventPublisher, never()).publishUserCreatedEvent(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   ", "invalid-email", "@example.com"})
    @DisplayName("User Registration: Boundary - invalid email formats throw ValidationException")
    void testUserRegistration_whenInvalidEmail_expectsValidationException(String invalidEmail) {
        // ARRANGE
        UserRegistrationRequest request = new UserRegistrationRequest(
            invalidEmail,
            "John Doe",
            "SecurePassword123!"
        );

        // ACT & ASSERT
        assertThrows(ValidationException.class, () -> userService.registerUser(request));
        verify(userRepository, never()).save(any(User.class));
    }

    @Test
    @DisplayName("User Authentication: Happy path - valid credentials return JWT token")
    void testUserAuthentication_whenValidCredentials_expectsJWTToken() {
        // ARRANGE
        UserAuthRequest authRequest = new UserAuthRequest(
            "john.doe@example.com",
            "SecurePassword123!"
        );
        User user = new User(
            UUID.randomUUID(),
            "john.doe@example.com",
            "John Doe",
            "hashedPassword",
            LocalDateTime.now()
        );

        when(userRepository.findByEmail(authRequest.getEmail())).thenReturn(Optional.of(user));
        when(passwordEncoder.matches(authRequest.getPassword(), user.getPasswordHash())).thenReturn(true);

        // ACT
        String token = userService.authenticate(authRequest);

        // ASSERT
        assertNotNull(token);
        assertFalse(token.isEmpty());
        verify(userRepository).findByEmail(authRequest.getEmail());
    }

    @Test
    @DisplayName("User Authentication: Error - non-existent user throws ValidationException")
    void testUserAuthentication_whenUserNotFound_expectsValidationException() {
        // ARRANGE
        UserAuthRequest authRequest = new UserAuthRequest(
            "nonexistent@example.com",
            "password"
        );
        when(userRepository.findByEmail(authRequest.getEmail())).thenReturn(Optional.empty());

        // ACT & ASSERT
        assertThrows(ValidationException.class, () -> userService.authenticate(authRequest));
    }

    @Test
    @DisplayName("User Authentication: Error - invalid password throws ValidationException")
    void testUserAuthentication_whenInvalidPassword_expectsValidationException() {
        // ARRANGE
        UserAuthRequest authRequest = new UserAuthRequest(
            "john.doe@example.com",
            "WrongPassword123!"
        );
        User user = new User(
            UUID.randomUUID(),
            "john.doe@example.com",
            "John Doe",
            "hashedPassword",
            LocalDateTime.now()
        );

        when(userRepository.findByEmail(authRequest.getEmail())).thenReturn(Optional.of(user));
        when(passwordEncoder.matches(authRequest.getPassword(), user.getPasswordHash())).thenReturn(false);

        // ACT & ASSERT
        assertThrows(ValidationException.class, () -> userService.authenticate(authRequest));
    }

    @Test
    @DisplayName("User Profile Retrieval: Happy path - returns user profile")
    void testGetUserProfile_whenUserExists_expectsUserProfile() {
        // ARRANGE
        UUID userId = UUID.randomUUID();
        User user = new User(
            userId,
            "john.doe@example.com",
            "John Doe",
            "hashedPassword",
            LocalDateTime.now()
        );
        when(userRepository.findById(userId)).thenReturn(Optional.of(user));

        // ACT
        User result = userService.getUserProfile(userId);

        // ASSERT
        assertNotNull(result);
        assertEquals(userId, result.getId());
        assertEquals("john.doe@example.com", result.getEmail());
    }

    @Test
    @DisplayName("User Profile Retrieval: Error - non-existent user throws ValidationException")
    void testGetUserProfile_whenUserNotFound_expectsValidationException() {
        // ARRANGE
        UUID userId = UUID.randomUUID();
        when(userRepository.findById(userId)).thenReturn(Optional.empty());

        // ACT & ASSERT
        assertThrows(ValidationException.class, () -> userService.getUserProfile(userId));
    }

    // ==================== PAYMENT SERVICE TESTS ====================

    @Test
    @DisplayName("Payment Processing: Happy path - valid payment creates transaction and publishes event")
    void testProcessPayment_whenValidRequest_expectsSuccessfulPayment() {
        // ARRANGE
        PaymentRequest paymentRequest = new PaymentRequest(
            UUID.randomUUID(),
            new BigDecimal("99.99"),
            "USD",
            "tok_visa"
        );
        Payment expectedPayment = new Payment(
            UUID.randomUUID(),
            paymentRequest.getUserId(),
            paymentRequest.getAmount(),
            "COMPLETED",
            "stripe_charge_id_123",
            LocalDateTime.now()
        );

        when(stripeClient.processPayment(paymentRequest)).thenReturn("stripe_charge_id_123");
        when(paymentRepository.save(any(Payment.class))).thenReturn(expectedPayment);

        // ACT
        Payment result = paymentService.processPayment(paymentRequest);

        // ASSERT
        assertNotNull(result);
        assertEquals("COMPLETED", result.getStatus());
        assertEquals("stripe_charge_id_123", result.getStripeChargeId());
        verify(stripeClient).processPayment(paymentRequest);
        verify(paymentRepository).save(any(Payment.class));
        verify(kafkaEventPublisher).publishPaymentCompletedEvent(result);
    }

    @Test
    @DisplayName("Payment Processing: Boundary - zero amount throws PaymentException")
    void testProcessPayment_whenZeroAmount_expectsPaymentException() {
        // ARRANGE
        PaymentRequest paymentRequest = new PaymentRequest(
            UUID.randomUUID(),
            BigDecimal.ZERO,
            "USD",
            "tok_visa"
        );

        // ACT & ASSERT
        assertThrows(PaymentException.class, () -> paymentService.processPayment(paymentRequest));
        verify(stripeClient, never()).processPayment(any());
    }

    @Test
    @DisplayName("Payment Processing: Boundary - negative amount throws PaymentException")
    void testProcessPayment_whenNegativeAmount_expectsPaymentException() {
        // ARRANGE
        PaymentRequest paymentRequest = new PaymentRequest(
            UUID.randomUUID(),
            new BigDecimal("-50.00"),
            "USD",
            "tok_visa"
        );

        // ACT & ASSERT
        assertThrows(PaymentException.class, () -> paymentService.processPayment(paymentRequest));
    }

    @Test
    @DisplayName("Payment Processing: Error - Stripe API failure throws PaymentException")
    void testProcessPayment_whenStripeFailure_expectsPaymentException() {
        // ARRANGE
        PaymentRequest paymentRequest = new PaymentRequest(
            UUID.randomUUID(),
            new BigDecimal("99.99"),
            "USD",
            "tok_chargeDeclined"
        );
        when(stripeClient.processPayment(paymentRequest))
            .thenThrow(new PaymentException("Card declined"));

        // ACT & ASSERT
        assertThrows(PaymentException.class, () -> paymentService.processPayment(paymentRequest));
        verify(paymentRepository, never()).save(any(Payment.class));
    }

    @ParameterizedTest
    @CsvSource({
        "99.99, USD, COMPLETED",
        "199.99, EUR, COMPLETED",
        "9.99, GBP, COMPLETED"
    })
    @DisplayName("Payment Processing: Parametrized - multiple currencies and amounts")
    void testProcessPayment_withMultipleCurrencies_expectsSuccess(String amount, String currency, String expectedStatus) {
        // ARRANGE
        PaymentRequest paymentRequest = new PaymentRequest(
            UUID.randomUUID(),
            new BigDecimal(amount),
            currency,
            "tok_visa"
        );
        Payment expectedPayment = new Payment(
            UUID.randomUUID(),
            paymentRequest.getUserId(),
            paymentRequest.getAmount(),
            expectedStatus,
            "stripe_charge_id_123",
            LocalDateTime.now()
        );

        when(stripeClient.processPayment(paymentRequest)).thenReturn("stripe_charge_id_123");
        when(paymentRepository.save(any(Payment.class))).thenReturn(expectedPayment);

        // ACT
        Payment result = paymentService.processPayment(paymentRequest);

        // ASSERT
        assertNotNull(result);
        assertEquals(expectedStatus, result.getStatus());
    }

    @Test
    @DisplayName("Payment Retrieval: Happy path - returns payment by ID")
    void testGetPayment_whenPaymentExists_expectsPaymentDetails() {
        // ARRANGE
        UUID paymentId = UUID.randomUUID();
        Payment payment = new Payment(
            paymentId,
            UUID.randomUUID(),
            new BigDecimal("99.99"),
            "COMPLETED",
            "stripe_charge_id_123",
            LocalDateTime.now()
        );
        when(paymentRepository.findById(paymentId)).thenReturn(Optional.of(payment));

        // ACT
        Payment result = paymentService.getPayment(paymentId);

        // ASSERT
        assertNotNull(result);
        assertEquals(paymentId, result.getId());
        assertEquals("COMPLETED", result.getStatus());
    }

    @Test
    @DisplayName("Payment Retrieval: Error - non-existent payment throws PaymentException")
    void testGetPayment_whenPaymentNotFound_expectsPaymentException() {
        // ARRANGE
        UUID paymentId = UUID.randomUUID();
        when(paymentRepository.findById(paymentId)).thenReturn(Optional.empty());

        // ACT & ASSERT
        assertThrows(PaymentException.class, () -> paymentService.getPayment(paymentId));
    }

    // ==================== SUBSCRIPTION SERVICE TESTS ====================

    @Test
    @DisplayName("Subscription Creation: Happy path - creates subscription after payment")
    void testCreateSubscription_whenValidRequest_expectsSubscriptionCreated() {
        // ARRANGE
        SubscriptionRequest request = new SubscriptionRequest(
            UUID.randomUUID(),
            "PREMIUM",
            LocalDate.now().plusMonths(1)
        );
        Subscription expectedSubscription = new Subscription(
            UUID.randomUUID(),
            request.getUserId(),
            request.getPlan(),
            "ACTIVE",
            LocalDateTime.now(),
            request.getRenewalDate().atStartOfDay(),
            LocalDateTime.now()
        );

        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(expectedSubscription);

        // ACT
        Subscription result = subscriptionService.createSubscription(request);

        // ASSERT
        assertNotNull(result);
        assertEquals("ACTIVE", result.getStatus());
        assertEquals(request.getPlan(), result.getPlan());
        verify(subscriptionRepository).save(any(Subscription.class));
        verify(kafkaEventPublisher).publishSubscriptionCreatedEvent(result);
    }

    @Test
    @DisplayName("Subscription Creation: Boundary - null user ID throws SubscriptionException")
    void testCreateSubscription_whenNullUserId_expectsSubscriptionException() {
        // ARRANGE
        SubscriptionRequest request = new SubscriptionRequest(
            null,
            "PREMIUM",
            LocalDate.now().plusMonths(1)
        );

        // ACT & ASSERT
        assertThrows(SubscriptionException.class, () -> subscriptionService.createSubscription(request));
    }

    @Test
    @DisplayName("Subscription Creation: Boundary - past renewal date throws SubscriptionException")
    void testCreateSubscription_whenPastRenewalDate_expectsSubscriptionException() {
        // ARRANGE
        SubscriptionRequest request = new SubscriptionRequest(
            UUID.randomUUID(),
            "PREMIUM",
            LocalDate.now().minusDays(1)
        );

        // ACT & ASSERT
        assertThrows(SubscriptionException.class, () -> subscriptionService.createSubscription(request));
    }

    @ParameterizedTest
    @ValueSource(strings = {"BASIC", "STANDARD", "PREMIUM", "ENTERPRISE"})
    @DisplayName("Subscription Creation: Parametrized - multiple subscription plans")
    void testCreateSubscription_withMultiplePlans_expectsSuccess(String plan) {
        // ARRANGE
        SubscriptionRequest request = new SubscriptionRequest(
            UUID.randomUUID(),
            plan,
            LocalDate.now().plusMonths(1)
        );
        Subscription expectedSubscription = new Subscription(
            UUID.randomUUID(),
            request.getUserId(),
            plan,
            "ACTIVE",
            LocalDateTime.now(),
            request.getRenewalDate().atStartOfDay(),
            LocalDateTime.now()
        );

        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(expectedSubscription);

        // ACT
        Subscription result = subscriptionService.createSubscription(request);

        // ASSERT
        assertNotNull(result);
        assertEquals(plan, result.getPlan());
    }

    @Test
    @DisplayName("Subscription Renewal: Happy path - scheduler renews active subscriptions")
    void testSubscriptionRenewal_whenSchedulerRuns_expectsActiveSubscriptionsRenewed() {
        // ARRANGE
        UUID subscriptionId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        Subscription subscription = new Subscription(
            subscriptionId,
            userId,
            "PREMIUM",
            "ACTIVE",
            LocalDateTime.now().minusMonths(1),
            LocalDateTime.now(),
            LocalDateTime.now().minusMonths(1)
        );

        when(subscriptionRepository.findActiveSubscriptionsForRenewal()).thenReturn(java.util.List.of(subscription));
        when(paymentService.processRenewalPayment(userId)).thenReturn(true);
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(subscription);

        // ACT
        subscriptionRenewalScheduler.renewSubscriptions();

        // ASSERT
        verify(subscriptionRepository).findActiveSubscriptionsForRenewal();
        verify(paymentService).processRenewalPayment(userId);
        verify(kafkaEventPublisher).publishSubscriptionRenewedEvent(any());
    }

    @Test
    @DisplayName("Subscription Cancellation: Happy path - cancels subscription and publishes event")
    void testCancelSubscription_whenValidRequest_expectsSubscriptionCancelled() {
        // ARRANGE
        UUID subscriptionId = UUID.randomUUID();
        Subscription subscription = new Subscription(
            subscriptionId,
            UUID.randomUUID(),
            "PREMIUM",
            "ACTIVE",
            LocalDateTime.now().minusMonths(1),
            LocalDateTime.now().plusMonths(1),
            LocalDateTime.now().minusMonths(1)
        );

        when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(subscription));
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(subscription);

        // ACT
        subscriptionService.cancelSubscription(subscriptionId);

        // ASSERT
        verify(subscriptionRepository).findById(subscriptionId);
        verify(subscriptionRepository).save(any(Subscription.class));
        verify(kafkaEventPublisher).publishSubscriptionCancelledEvent(any());
    }

    @Test
    @DisplayName("Subscription Cancellation: Error - non-existent subscription throws SubscriptionException")
    void testCancelSubscription_whenSubscriptionNotFound_expectsSubscriptionException() {
        // ARRANGE
        UUID subscriptionId = UUID.randomUUID();
        when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.empty());

        // ACT & ASSERT
        assertThrows(SubscriptionException.class, () -> subscriptionService.cancelSubscription(subscriptionId));
    }

    @Test
    @DisplayName("Subscription Retrieval: Happy path - returns subscription details")
    void testGetSubscription_whenSubscriptionExists_expectsSubscriptionDetails() {
        // ARRANGE
        UUID subscriptionId = UUID.randomUUID();
        Subscription subscription = new Subscription(
            subscriptionId,
            UUID.randomUUID(),
            "PREMIUM",
            "ACTIVE",
            LocalDateTime.now().minusMonths(1),
            LocalDateTime.now().plusMonths(1),
            LocalDateTime.now().minusMonths(1)
        );
        when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(subscription));

        // ACT
        Subscription result = subscriptionService.getSubscription(subscriptionId);

        // ASSERT
        assertNotNull(result);
        assertEquals(subscriptionId, result.getId());
        assertEquals("ACTIVE", result.getStatus());
    }

    // ==================== ORDER SERVICE TESTS ====================

    @Test
    @DisplayName("Order Creation: Happy path - creates order with valid subscription")
    void testCreateOrder_whenValidSubscription_expectsOrderCreated() {
        // ARRANGE
        OrderRequest orderRequest = new OrderRequest(
            UUID.randomUUID(),
            UUID.randomUUID(),
            new BigDecimal("99.99")
        );
        Order expectedOrder = new Order(
            UUID.randomUUID(),
            orderRequest.getUserId(),
            orderRequest.getSubscriptionId(),
            orderRequest.getAmount(),
            "PENDING",
            LocalDateTime.now()
        );

        when(subscriptionServiceClient.validateSubscription(orderRequest.getSubscriptionId())).thenReturn(true);
        when(orderRepository.save(any(Order.class))).thenReturn(expectedOrder);

        // ACT
        Order result = orderService.createOrder(orderRequest);

        // ASSERT
        assertNotNull(result);
        assertEquals("PENDING", result.getStatus());
        assertEquals(orderRequest.getAmount(), result.getAmount());
        verify(subscriptionServiceClient).validateSubscription(orderRequest.getSubscriptionId());
        verify(orderRepository).save(any(Order.class));
        verify(kafkaEventPublisher).publishOrderCreatedEvent(result);
    }

    @Test
    @DisplayName("Order Creation: Error - invalid subscription throws OrderException")
    void testCreateOrder_whenInvalidSubscription_expectsOrderException() {
        // ARRANGE
        OrderRequest orderRequest = new OrderRequest(
            UUID.randomUUID(),
            UUID.randomUUID(),
            new BigDecimal("99.99")
        );

        when(subscriptionServiceClient.validateSubscription(orderRequest.getSubscriptionId())).thenReturn(false);

        // ACT & ASSERT
        assertThrows(OrderException.class, () -> orderService.createOrder(orderRequest));
        verify(orderRepository, never()).save(any(Order.class));
    }

    @Test
    @DisplayName("Order Creation: Boundary - zero amount throws OrderException")
    void testCreateOrder_whenZeroAmount_expectsOrderException() {
        // ARRANGE
        OrderRequest orderRequest = new OrderRequest(
            UUID.randomUUID(),
            UUID.randomUUID(),
            BigDecimal.ZERO
        );

        // ACT & ASSERT
        assertThrows(OrderException.class, () -> orderService.createOrder(orderRequest));
    }

    @Test
    @DisplayName("Order Completion: Happy path - completes order and publishes event")
    void testCompleteOrder_whenValidOrder_expectsOrderCompleted() {
        // ARRANGE
        UUID orderId = UUID.randomUUID();
        Order order = new Order(
            orderId,
            UUID.randomUUID(),
            UUID.randomUUID(),
            new BigDecimal("99.99"),
            "PENDING",
            LocalDateTime.now()
        );

        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));
        when(orderRepository.save(any(Order.class))).thenReturn(order);

        // ACT
        orderService.completeOrder(orderId);

        // ASSERT
        verify(orderRepository).findById(orderId);
        verify(orderRepository).save(any(Order.class));
        verify(kafkaEventPublisher).publishOrderCompletedEvent(any());
    }

    @Test
    @DisplayName("Order Completion: Error - non-existent order throws OrderException")
    void testCompleteOrder_whenOrderNotFound_expectsOrderException() {
        // ARRANGE
        UUID orderId = UUID.randomUUID();
        when(orderRepository.findById(orderId)).thenReturn(Optional.empty());

        // ACT & ASSERT
        assertThrows(OrderException.class, () -> orderService.completeOrder(orderId));
    }

    @Test
    @DisplayName("Order Retrieval: Happy path - returns order details")
    void testGetOrder_whenOrderExists_expectsOrderDetails() {
        // ARRANGE
        UUID orderId = UUID.randomUUID();
        Order order = new Order(
            orderId,
            UUID.randomUUID(),
            UUID.randomUUID(),
            new BigDecimal("99.99"),
            "COMPLETED",
            LocalDateTime.now()
        );
        when(orderRepository.findById(orderId)).thenReturn(Optional.of(order));

        // ACT
        Order result = orderService.getOrder(orderId);

        // ASSERT
        assertNotNull(result);
        assertEquals(orderId, result.getId());
        assertEquals("COMPLETED", result.getStatus());
    }

    @Test
    @DisplayName("Order Retrieval: Error - non-existent order throws OrderException")
    void testGetOrder_whenOrderNotFound_expectsOrderException() {
        // ARRANGE
        UUID orderId = UUID.randomUUID();
        when(orderRepository.findById(orderId)).thenReturn(Optional.empty());

        // ACT & ASSERT
        assertThrows(OrderException.class, () -> orderService.getOrder(orderId));
    }

    // ==================== INTEGRATION SCENARIO TESTS ====================

    @Test
    @DisplayName("End-to-End: User Registration to Subscription Purchase Flow")
    void testEndToEnd_userRegistrationToSubscriptionPurchase() {
        // ARRANGE - User Registration
        UserRegistrationRequest userRequest = new UserRegistrationRequest(
            "newuser@example.com",
            "New User",
            "SecurePassword123!"
        );
        User newUser = new User(
            UUID.randomUUID(),
            userRequest.getEmail(),
            userRequest.getName(),
            "hashedPassword",
            LocalDateTime.now()
        );

        when(userRepository.existsByEmail(userRequest.getEmail())).thenReturn(false);
        when(passwordEncoder.encode(userRequest.getPassword())).thenReturn("hashedPassword");
        when(userRepository.save(any(User.class))).thenReturn(newUser);

        // ACT - Register User
        User registeredUser = userService.registerUser(userRequest);

        // ASSERT - User Created
        assertNotNull(registeredUser);
        verify(kafkaEventPublisher).publishUserCreatedEvent(registeredUser);

        // ARRANGE - Payment Processing
        PaymentRequest paymentRequest = new PaymentRequest(
            registeredUser.getId(),
            new BigDecimal("99.99"),
            "USD",
            "tok_visa"
        );
        Payment payment = new Payment(
            UUID.randomUUID(),
            registeredUser.getId(),
            paymentRequest.getAmount(),
            "COMPLETED",
            "stripe_charge_id_123",
            LocalDateTime.now()
        );

        when(stripeClient.processPayment(paymentRequest)).thenReturn("stripe_charge_id_123");
        when(paymentRepository.save(any(Payment.class))).thenReturn(payment);

        // ACT - Process Payment
        Payment processedPayment = paymentService.processPayment(paymentRequest);

        // ASSERT - Payment Completed
        assertNotNull(processedPayment);
        assertEquals("COMPLETED", processedPayment.getStatus());
        verify(kafkaEventPublisher).publishPaymentCompletedEvent(processedPayment);

        // ARRANGE - Subscription Creation
        SubscriptionRequest subscriptionRequest = new SubscriptionRequest(
            registeredUser.getId(),
            "PREMIUM",
            LocalDate.now().plusMonths(1)
        );
        Subscription subscription = new Subscription(
            UUID.randomUUID(),
            registeredUser.getId(),
            subscriptionRequest.getPlan(),
            "ACTIVE",
            LocalDateTime.now(),
            subscriptionRequest.getRenewalDate().atStartOfDay(),
            LocalDateTime.now()
        );

        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(subscription);

        // ACT - Create Subscription
        Subscription createdSubscription = subscriptionService.createSubscription(subscriptionRequest);

        // ASSERT - Subscription Created
        assertNotNull(createdSubscription);
        assertEquals("ACTIVE", createdSubscription.getStatus());
        verify(kafkaEventPublisher).publishSubscriptionCreatedEvent(createdSubscription);
    }

    @Test
    @DisplayName("Concurrency: Multiple simultaneous payment processing requests")
    void testConcurrency_multiplePaymentRequests() throws InterruptedException {
        // ARRANGE
        int threadCount = 5;
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(threadCount);

        // ACT & ASSERT
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    PaymentRequest paymentRequest = new PaymentRequest(
                        UUID.randomUUID(),
                        new BigDecimal("99.99"),
                        "USD",
                        "tok_visa"
                    );
                    Payment payment = new Payment(
                        UUID.randomUUID(),
                        paymentRequest.getUserId(),
                        paymentRequest.getAmount(),
                        "COMPLETED",
                        "stripe_charge_id_" + System.nanoTime(),
                        LocalDateTime.now()
                    );

                    when(stripeClient.processPayment(paymentRequest)).thenReturn(payment.getStripeChargeId());
                    when(paymentRepository.save(any(Payment.class))).thenReturn(payment);

                    Payment result = paymentService.processPayment(paymentRequest);
                    assertNotNull(result);
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();
        verify(paymentRepository, atLeast(threadCount)).save(any(Payment.class));
    }
}
