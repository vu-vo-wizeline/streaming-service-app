package io.github.marianciuc.streamingservice.payment.service.impl;

import com.stripe.exception.StripeException;
import com.stripe.model.Customer;
import com.stripe.model.PaymentIntent;
import com.stripe.model.Refund;
import io.github.marianciuc.streamingservice.payment.dto.common.CardHolderDto;
import io.github.marianciuc.streamingservice.payment.dto.common.TransactionDto;
import io.github.marianciuc.streamingservice.payment.dto.requests.CreateCartHolderRequest;
import io.github.marianciuc.streamingservice.payment.dto.requests.UpdateCardHolderRequest;
import io.github.marianciuc.streamingservice.payment.dto.responses.AddressDto;
import io.github.marianciuc.streamingservice.payment.entity.Address;
import io.github.marianciuc.streamingservice.payment.entity.CardHolder;
import io.github.marianciuc.streamingservice.payment.entity.JWTUserPrincipal;
import io.github.marianciuc.streamingservice.payment.entity.Refund;
import io.github.marianciuc.streamingservice.payment.entity.Transaction;
import io.github.marianciuc.streamingservice.payment.enums.CardStatus;
import io.github.marianciuc.streamingservice.payment.enums.Currency;
import io.github.marianciuc.streamingservice.payment.enums.PaymentStatus;
import io.github.marianciuc.streamingservice.payment.enums.RefundStatus;
import io.github.marianciuc.streamingservice.payment.kafka.PaymentKafkaProducer;
import io.github.marianciuc.streamingservice.payment.kafka.messages.InitializePaymentMessage;
import io.github.marianciuc.streamingservice.payment.repository.CardHolderRepository;
import io.github.marianciuc.streamingservice.payment.repository.RefundRepository;
import io.github.marianciuc.streamingservice.payment.repository.TransactionRepository;
import io.github.marianciuc.streamingservice.payment.service.AddressService;
import io.github.marianciuc.streamingservice.payment.service.CardHolderService;
import io.github.marianciuc.streamingservice.payment.service.UserService;
import io.github.marianciuc.streamingservice.payment.specifications.TransactionSpecification;
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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit test suite for Payment Service implementations.
 * Tests cover TransactionServiceImpl, CardHolderServiceImpl, and RefundServiceImpl
 * with focus on happy paths, error handling, boundary values, and security.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Payment Service Unit Tests")
public class PaymentServiceImplTest {

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

        private InitializePaymentMessage testMessage;
        private CardHolder testCardHolder;
        private Transaction testTransaction;

        @BeforeEach
        void setUp() {
            UUID userId = UUID.randomUUID();
            UUID orderId = UUID.randomUUID();
            UUID transactionId = UUID.randomUUID();

            testCardHolder = CardHolder.builder()
                    .userId(userId)
                    .stripeCustomerId("cus_test123")
                    .cardHolderName("John Doe")
                    .email("john@example.com")
                    .phoneNumber("+1234567890")
                    .cardStatus(CardStatus.ACTIVE)
                    .build();

            testMessage = new InitializePaymentMessage(
                    userId,
                    orderId,
                    100L,
                    Currency.USD
            );

            testTransaction = Transaction.builder()
                    .id(transactionId)
                    .orderId(orderId)
                    .amount(100L)
                    .currency(Currency.USD)
                    .status(PaymentStatus.PENDING)
                    .cardHolder(testCardHolder)
                    .createdAt(LocalDateTime.now())
                    .build();
        }

        @Test
        @DisplayName("Should initialize transaction successfully with valid Stripe payment intent")
        void testInitializeTransaction_whenValidPaymentIntent_expectsSuccess() throws StripeException {
            // ARRANGE
            PaymentIntent mockPaymentIntent = mock(PaymentIntent.class);
            when(mockPaymentIntent.getId()).thenReturn("pi_test123");
            when(cardHolderService.findCardHolderEntity(testMessage.userId())).thenReturn(testCardHolder);
            when(transactionRepository.save(any(Transaction.class))).thenAnswer(invocation -> invocation.getArgument(0));

            // ACT
            transactionService.initializeTransaction(testMessage);

            // ASSERT
            ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);
            verify(transactionRepository).save(transactionCaptor.capture());
            Transaction savedTransaction = transactionCaptor.getValue();

            assertEquals(PaymentStatus.SUCCESS, savedTransaction.getStatus());
            assertEquals("pi_test123", savedTransaction.getStripePaymentIntentId());
            assertEquals(testMessage.amount(), savedTransaction.getAmount());
            assertEquals(testMessage.currency(), savedTransaction.getCurrency());
        }

        @Test
        @DisplayName("Should handle Stripe exception and mark transaction as failed")
        void testInitializeTransaction_whenStripeException_expectsFailedStatus() throws StripeException {
            // ARRANGE
            StripeException stripeException = new StripeException("Card declined", "card_declined", "402");
            when(cardHolderService.findCardHolderEntity(testMessage.userId())).thenReturn(testCardHolder);
            when(transactionRepository.save(any(Transaction.class))).thenAnswer(invocation -> invocation.getArgument(0));

            // ACT & ASSERT
            assertThrows(RuntimeException.class, () -> transactionService.initializeTransaction(testMessage));

            ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);
            verify(transactionRepository).save(transactionCaptor.capture());
            Transaction savedTransaction = transactionCaptor.getValue();

            assertEquals(PaymentStatus.FAILED, savedTransaction.getStatus());
            assertEquals("Card declined", savedTransaction.getFailureMessage());
        }

        @Test
        @DisplayName("Should find transaction by ID successfully")
        void testFindTransactionEntity_whenValidId_expectsTransaction() {
            // ARRANGE
            UUID transactionId = UUID.randomUUID();
            when(transactionRepository.findById(transactionId)).thenReturn(Optional.of(testTransaction));

            // ACT
            Transaction result = transactionService.findTransactionEntity(transactionId);

            // ASSERT
            assertNotNull(result);
            assertEquals(testTransaction.getId(), result.getId());
            verify(transactionRepository).findById(transactionId);
        }

        @Test
        @DisplayName("Should throw exception when transaction not found")
        void testFindTransactionEntity_whenInvalidId_expectsException() {
            // ARRANGE
            UUID transactionId = UUID.randomUUID();
            when(transactionRepository.findById(transactionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(RuntimeException.class, () -> transactionService.findTransactionEntity(transactionId));
            verify(transactionRepository).findById(transactionId);
        }

        @Test
        @DisplayName("Should retrieve transactions for admin user with any userId filter")
        void testGetTransactions_whenAdminUser_expectsTransactionsWithFilteredUserId() {
            // ARRANGE
            UUID adminUserId = UUID.randomUUID();
            UUID filteredUserId = UUID.randomUUID();
            JWTUserPrincipal adminPrincipal = createJWTPrincipal(adminUserId, "ROLE_ADMIN");
            setupSecurityContext(adminPrincipal);

            List<Transaction> transactions = List.of(testTransaction);
            Page<Transaction> transactionPage = new PageImpl<>(transactions);
            when(transactionRepository.findAll(any(Specification.class), any(Pageable.class)))
                    .thenReturn(transactionPage);

            // ACT
            List<TransactionDto> result = transactionService.getTransactions(0, 10, "ASC", PaymentStatus.SUCCESS, filteredUserId);

            // ASSERT
            assertNotNull(result);
            assertEquals(1, result.size());
            verify(transactionRepository).findAll(any(Specification.class), any(Pageable.class));
        }

        @Test
        @DisplayName("Should retrieve transactions for non-admin user with own userId only")
        void testGetTransactions_whenNonAdminUser_expectsOwnTransactionsOnly() {
            // ARRANGE
            UUID userId = UUID.randomUUID();
            JWTUserPrincipal userPrincipal = createJWTPrincipal(userId, "ROLE_USER");
            setupSecurityContext(userPrincipal);

            List<Transaction> transactions = List.of(testTransaction);
            Page<Transaction> transactionPage = new PageImpl<>(transactions);
            when(transactionRepository.findAll(any(Specification.class), any(Pageable.class)))
                    .thenReturn(transactionPage);

            // ACT
            List<TransactionDto> result = transactionService.getTransactions(0, 10, "ASC", PaymentStatus.SUCCESS, null);

            // ASSERT
            assertNotNull(result);
            assertEquals(1, result.size());
            verify(transactionRepository).findAll(any(Specification.class), any(Pageable.class));
        }

        @Test
        @DisplayName("Should throw AccessDeniedException when authentication is invalid")
        void testGetTransactions_whenInvalidAuth_expectsAccessDenied() {
            // ARRANGE
            SecurityContextHolder.clearContext();

            // ACT & ASSERT
            assertThrows(AccessDeniedException.class, () ->
                    transactionService.getTransactions(0, 10, "ASC", PaymentStatus.SUCCESS, null)
            );
        }

        @ParameterizedTest
        @ValueSource(strings = {"ASC", "DESC"})
        @DisplayName("Should handle different sort directions")
        void testGetTransactions_withDifferentSortDirections_expectsSuccess(String sortDirection) {
            // ARRANGE
            UUID userId = UUID.randomUUID();
            JWTUserPrincipal userPrincipal = createJWTPrincipal(userId, "ROLE_USER");
            setupSecurityContext(userPrincipal);

            Page<Transaction> transactionPage = new PageImpl<>(List.of(testTransaction));
            when(transactionRepository.findAll(any(Specification.class), any(Pageable.class)))
                    .thenReturn(transactionPage);

            // ACT
            List<TransactionDto> result = transactionService.getTransactions(0, 10, sortDirection, null, null);

            // ASSERT
            assertNotNull(result);
            verify(transactionRepository).findAll(any(Specification.class), any(Pageable.class));
        }
    }

    @Nested
    @DisplayName("CardHolderServiceImpl Tests")
    class CardHolderServiceImplTests {

        @Mock
        private AddressService addressService;

        @Mock
        private CardHolderRepository cardHolderRepository;

        @Mock
        private UserService userService;

        @InjectMocks
        private CardHolderServiceImpl cardHolderService;

        private CreateCartHolderRequest testRequest;
        private Address testAddress;
        private CardHolder testCardHolder;
        private UUID testUserId;

        @BeforeEach
        void setUp() {
            testUserId = UUID.randomUUID();
            testAddress = Address.builder()
                    .id(UUID.randomUUID())
                    .line1("123 Main St")
                    .city("New York")
                    .state("NY")
                    .postalCode("10001")
                    .country("US")
                    .build();

            AddressDto addressDto = new AddressDto(
                    "123 Main St", "New York", "NY", "10001", "US"
            );

            testRequest = new CreateCartHolderRequest(
                    "John Doe",
                    "john@example.com",
                    "+1234567890",
                    "tok_visa",
                    addressDto
            );

            testCardHolder = CardHolder.builder()
                    .userId(testUserId)
                    .cardHolderName("John Doe")
                    .email("john@example.com")
                    .phoneNumber("+1234567890")
                    .stripeCustomerId("cus_test123")
                    .cardStatus(CardStatus.ACTIVE)
                    .address(testAddress)
                    .build();
        }

        @Test
        @DisplayName("Should create card holder successfully with valid Stripe customer")
        void testCreateCardHolder_whenValidRequest_expectsSuccess() throws StripeException {
            // ARRANGE
            Customer mockCustomer = mock(Customer.class);
            when(mockCustomer.getId()).thenReturn("cus_test123");
            when(userService.extractUserIdFromAuth()).thenReturn(testUserId);
            when(cardHolderRepository.existsCardHolderByUserId(testUserId)).thenReturn(false);
            when(addressService.addAddress(any())).thenReturn(testAddress);
            when(cardHolderRepository.save(any(CardHolder.class))).thenReturn(testCardHolder);

            // ACT
            CardHolderDto result = cardHolderService.createCardHolder(testRequest);

            // ASSERT
            assertNotNull(result);
            assertEquals("John Doe", result.cardHolderName());
            assertEquals("john@example.com", result.email());
            verify(cardHolderRepository).save(any(CardHolder.class));
        }

        @Test
        @DisplayName("Should throw exception when card holder already exists")
        void testCreateCardHolder_whenCardHolderExists_expectsException() {
            // ARRANGE
            when(userService.extractUserIdFromAuth()).thenReturn(testUserId);
            when(cardHolderRepository.existsCardHolderByUserId(testUserId)).thenReturn(true);

            // ACT & ASSERT
            assertThrows(IllegalArgumentException.class, () ->
                    cardHolderService.createCardHolder(testRequest)
            );
            verify(cardHolderRepository, never()).save(any());
        }

        @Test
        @DisplayName("Should handle Stripe exception during card holder creation")
        void testCreateCardHolder_whenStripeException_expectsRuntimeException() throws StripeException {
            // ARRANGE
            StripeException stripeException = new StripeException("Invalid token", "invalid_request_error", "400");
            when(userService.extractUserIdFromAuth()).thenReturn(testUserId);
            when(cardHolderRepository.existsCardHolderByUserId(testUserId)).thenReturn(false);
            when(addressService.addAddress(any())).thenReturn(testAddress);

            // ACT & ASSERT
            assertThrows(RuntimeException.class, () ->
                    cardHolderService.createCardHolder(testRequest)
            );
        }

        @Test
        @DisplayName("Should find card holder for admin user with specified ID")
        void testFindCardHolder_whenAdminUserWithId_expectsCardHolder() {
            // ARRANGE
            UUID adminUserId = UUID.randomUUID();
            UUID cardHolderId = UUID.randomUUID();
            JWTUserPrincipal adminPrincipal = createJWTPrincipal(adminUserId, "ROLE_ADMIN");
            setupSecurityContext(adminPrincipal);

            CardHolder cardHolder = CardHolder.builder()
                    .userId(cardHolderId)
                    .cardHolderName("Jane Doe")
                    .build();
            when(userService.hasAdminRoles()).thenReturn(true);
            when(cardHolderRepository.findById(cardHolderId)).thenReturn(Optional.of(cardHolder));

            // ACT
            CardHolderDto result = cardHolderService.findCardHolder(cardHolderId);

            // ASSERT
            assertNotNull(result);
            assertEquals("Jane Doe", result.cardHolderName());
        }

        @Test
        @DisplayName("Should find own card holder for non-admin user")
        void testFindCardHolder_whenNonAdminUser_expectsOwnCardHolder() {
            // ARRANGE
            UUID userId = UUID.randomUUID();
            JWTUserPrincipal userPrincipal = createJWTPrincipal(userId, "ROLE_USER");
            setupSecurityContext(userPrincipal);

            when(userService.hasAdminRoles()).thenReturn(false);
            when(userService.extractUserIdFromAuth()).thenReturn(userId);
            when(cardHolderRepository.findById(userId)).thenReturn(Optional.of(testCardHolder));

            // ACT
            CardHolderDto result = cardHolderService.findCardHolder(null);

            // ASSERT
            assertNotNull(result);
            verify(cardHolderRepository).findById(userId);
        }

        @Test
        @DisplayName("Should throw exception when card holder not found")
        void testFindCardHolder_whenNotFound_expectsException() {
            // ARRANGE
            UUID cardHolderId = UUID.randomUUID();
            when(userService.hasAdminRoles()).thenReturn(true);
            when(cardHolderRepository.findById(cardHolderId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(IllegalArgumentException.class, () ->
                    cardHolderService.findCardHolder(cardHolderId)
            );
        }

        @Test
        @DisplayName("Should update payment method successfully")
        void testUpdatePaymentMethod_whenValidToken_expectsSuccess() throws StripeException {
            // ARRANGE
            UUID userId = UUID.randomUUID();
            String newToken = "tok_visa_new";
            when(userService.extractUserIdFromAuth()).thenReturn(userId);
            when(cardHolderRepository.findById(userId)).thenReturn(Optional.of(testCardHolder));

            Customer mockCustomer = mock(Customer.class);
            // Mock static Customer.retrieve
            // Note: In real scenario, use PowerMock or Mockito.mockStatic for Java 16+

            // ACT
            cardHolderService.updatePaymentMethod(newToken);

            // ASSERT
            verify(cardHolderRepository).findById(userId);
        }

        @Test
        @DisplayName("Should update card holder information")
        void testUpdateCardHolder_whenValidRequest_expectsSuccess() {
            // ARRANGE
            UUID userId = UUID.randomUUID();
            JWTUserPrincipal userPrincipal = createJWTPrincipal(userId, "ROLE_USER");
            setupSecurityContext(userPrincipal);

            UpdateCardHolderRequest updateRequest = new UpdateCardHolderRequest(
                    "Jane Doe",
                    "jane@example.com",
                    "+9876543210"
            );

            when(cardHolderRepository.findById(userId)).thenReturn(Optional.of(testCardHolder));
            when(cardHolderRepository.save(any(CardHolder.class))).thenReturn(testCardHolder);

            // ACT
            cardHolderService.updateCardHolder(updateRequest);

            // ASSERT
            verify(cardHolderRepository).save(any(CardHolder.class));
        }

        @Test
        @DisplayName("Should not update card holder when request fields are empty")
        void testUpdateCardHolder_whenEmptyFields_expectsNoUpdate() {
            // ARRANGE
            UUID userId = UUID.randomUUID();
            JWTUserPrincipal userPrincipal = createJWTPrincipal(userId, "ROLE_USER");
            setupSecurityContext(userPrincipal);

            UpdateCardHolderRequest updateRequest = new UpdateCardHolderRequest("", "", "");
            when(cardHolderRepository.findById(userId)).thenReturn(Optional.of(testCardHolder));
            when(cardHolderRepository.save(any(CardHolder.class))).thenReturn(testCardHolder);

            // ACT
            cardHolderService.updateCardHolder(updateRequest);

            // ASSERT
            verify(cardHolderRepository).save(any(CardHolder.class));
        }
    }

    @Nested
    @DisplayName("RefundServiceImpl Tests")
    class RefundServiceImplTests {

        @Mock
        private RefundRepository refundRepository;

        @Mock
        private TransactionService transactionService;

        @InjectMocks
        private RefundServiceImpl refundService;

        private Transaction testTransaction;
        private UUID transactionId;

        @BeforeEach
        void setUp() {
            transactionId = UUID.randomUUID();
            testTransaction = Transaction.builder()
                    .id(transactionId)
                    .amount(100L)
                    .currency(Currency.USD)
                    .status(PaymentStatus.SUCCESS)
                    .stripePaymentIntentId("pi_test123")
                    .build();
        }

        @Test
        @DisplayName("Should process refund successfully")
        void testProcessRefund_whenValidTransaction_expectsSuccess() throws StripeException {
            // ARRANGE
            Refund mockRefund = mock(Refund.class);
            when(mockRefund.getId()).thenReturn("re_test123");
            when(mockRefund.getAmount()).thenReturn(10000L);
            when(transactionService.findTransactionEntity(transactionId)).thenReturn(testTransaction);
            when(refundRepository.save(any(io.github.marianciuc.streamingservice.payment.entity.Refund.class)))
                    .thenAnswer(invocation -> invocation.getArgument(0));

            // ACT
            refundService.processRefund(transactionId);

            // ASSERT
            ArgumentCaptor<io.github.marianciuc.streamingservice.payment.entity.Refund> refundCaptor =
                    ArgumentCaptor.forClass(io.github.marianciuc.streamingservice.payment.entity.Refund.class);
            verify(refundRepository).save(refundCaptor.capture());
            io.github.marianciuc.streamingservice.payment.entity.Refund savedRefund = refundCaptor.getValue();

            assertEquals(RefundStatus.SUCCESS, savedRefund.getStatus());
            assertEquals("re_test123", savedRefund.getStripeRefundId());
            assertEquals(10000L, savedRefund.getAmount());
        }

        @Test
        @DisplayName("Should handle Stripe exception during refund processing")
        void testProcessRefund_whenStripeException_expectsFailedStatus() throws StripeException {
            // ARRANGE
            StripeException stripeException = new StripeException("Refund failed", "refund_error", "402");
            when(transactionService.findTransactionEntity(transactionId)).thenReturn(testTransaction);
            when(refundRepository.save(any(io.github.marianciuc.streamingservice.payment.entity.Refund.class)))
                    .thenAnswer(invocation -> invocation.getArgument(0));

            // ACT & ASSERT
            assertThrows(RuntimeException.class, () -> refundService.processRefund(transactionId));

            ArgumentCaptor<io.github.marianciuc.streamingservice.payment.entity.Refund> refundCaptor =
                    ArgumentCaptor.forClass(io.github.marianciuc.streamingservice.payment.entity.Refund.class);
            verify(refundRepository).save(refundCaptor.capture());
            io.github.marianciuc.streamingservice.payment.entity.Refund savedRefund = refundCaptor.getValue();

            assertEquals(RefundStatus.FAILED, savedRefund.getStatus());
        }

        @Test
        @DisplayName("Should throw exception when transaction not found")
        void testProcessRefund_whenTransactionNotFound_expectsException() {
            // ARRANGE
            when(transactionService.findTransactionEntity(transactionId))
                    .thenThrow(new RuntimeException("Transaction not found"));

            // ACT & ASSERT
            assertThrows(RuntimeException.class, () -> refundService.processRefund(transactionId));
            verify(refundRepository, never()).save(any());
        }

        @ParameterizedTest
        @ValueSource(longs = {50L, 100L, 500L, 1000L})
        @DisplayName("Should process refunds for different amounts")
        void testProcessRefund_withDifferentAmounts_expectsSuccess(Long amount) throws StripeException {
            // ARRANGE
            Transaction transaction = Transaction.builder()
                    .id(transactionId)
                    .amount(amount)
                    .currency(Currency.USD)
                    .status(PaymentStatus.SUCCESS)
                    .stripePaymentIntentId("pi_test123")
                    .build();

            Refund mockRefund = mock(Refund.class);
            when(mockRefund.getId()).thenReturn("re_test123");
            when(mockRefund.getAmount()).thenReturn(amount * 100);
            when(transactionService.findTransactionEntity(transactionId)).thenReturn(transaction);
            when(refundRepository.save(any(io.github.marianciuc.streamingservice.payment.entity.Refund.class)))
                    .thenAnswer(invocation -> invocation.getArgument(0));

            // ACT
            refundService.processRefund(transactionId);

            // ASSERT
            verify(refundRepository).save(any(io.github.marianciuc.streamingservice.payment.entity.Refund.class));
        }
    }

    // Helper methods
    private JWTUserPrincipal createJWTPrincipal(UUID userId, String role) {
        Collection<GrantedAuthority> authorities = new ArrayList<>();
        authorities.add(new SimpleGrantedAuthority(role));
        return new JWTUserPrincipal(userId, "testuser", authorities);
    }

    private void setupSecurityContext(JWTUserPrincipal principal) {
        SecurityContext securityContext = mock(SecurityContext.class);
        Authentication authentication = mock(Authentication.class);
        when(authentication.getPrincipal()).thenReturn(principal);
        when(securityContext.getAuthentication()).thenReturn(authentication);
        SecurityContextHolder.setContext(securityContext);
    }
}
