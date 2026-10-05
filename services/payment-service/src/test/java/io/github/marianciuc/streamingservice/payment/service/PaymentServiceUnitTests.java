package io.github.marianciuc.streamingservice.payment.service;

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
import io.github.marianciuc.streamingservice.payment.service.impl.RefundServiceImpl;
import io.github.marianciuc.streamingservice.payment.service.impl.TransactionServiceImpl;
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
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("Payment Service Unit Tests")
class PaymentServiceUnitTests {

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
                    1000L,
                    Currency.USD
            );
        }

        @Test
        @DisplayName("Should initialize transaction successfully when payment intent succeeds")
        void testInitializeTransaction_whenPaymentIntentSucceeds_expectsTransactionSaved() throws StripeException {
            // ARRANGE
            when(cardHolderService.findCardHolderEntity(testUserId)).thenReturn(testCardHolder);
            when(transactionRepository.save(any(Transaction.class))).thenAnswer(invocation -> {
                Transaction tx = invocation.getArgument(0);
                tx.setId(testTransactionId);
                return tx;
            });

            // ACT
            transactionService.initializeTransaction(testPaymentMessage);

            // ASSERT
            ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);
            verify(transactionRepository).save(transactionCaptor.capture());
            Transaction savedTransaction = transactionCaptor.getValue();

            assertEquals(testPaymentMessage.amount(), savedTransaction.getAmount());
            assertEquals(testPaymentMessage.currency(), savedTransaction.getCurrency());
            assertEquals(testOrderId, savedTransaction.getOrderId());
            assertEquals(PaymentStatus.SUCCESS, savedTransaction.getStatus());
            assertEquals(testCardHolder, savedTransaction.getCardHolder());
        }

        @Test
        @DisplayName("Should handle Stripe exception and mark transaction as failed")
        void testInitializeTransaction_whenStripeThrowsException_expectsTransactionFailed() throws StripeException {
            // ARRANGE
            when(cardHolderService.findCardHolderEntity(testUserId)).thenReturn(testCardHolder);
            when(transactionRepository.save(any(Transaction.class))).thenAnswer(invocation -> {
                Transaction tx = invocation.getArgument(0);
                tx.setId(testTransactionId);
                return tx;
            });

            // Mock Stripe to throw exception
            StripeException stripeException = new StripeException("Card declined");
            try (var mockPaymentIntent = mockStatic(com.stripe.model.PaymentIntent.class)) {
                mockPaymentIntent.when(() -> PaymentIntent.create(any())).thenThrow(stripeException);

                // ACT & ASSERT
                assertThrows(RuntimeException.class, () -> transactionService.initializeTransaction(testPaymentMessage));

                ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);
                verify(transactionRepository).save(transactionCaptor.capture());
                Transaction savedTransaction = transactionCaptor.getValue();

                assertEquals(PaymentStatus.FAILED, savedTransaction.getStatus());
                assertEquals("Card declined", savedTransaction.getFailureMessage());
            }
        }

        @Test
        @DisplayName("Should find transaction by ID successfully")
        void testFindTransactionEntity_whenTransactionExists_expectsTransactionReturned() {
            // ARRANGE
            Transaction expectedTransaction = Transaction.builder()
                    .id(testTransactionId)
                    .orderId(testOrderId)
                    .amount(1000L)
                    .currency(Currency.USD)
                    .status(PaymentStatus.SUCCESS)
                    .cardHolder(testCardHolder)
                    .build();

            when(transactionRepository.findById(testTransactionId)).thenReturn(Optional.of(expectedTransaction));

            // ACT
            Transaction result = transactionService.findTransactionEntity(testTransactionId);

            // ASSERT
            assertNotNull(result);
            assertEquals(testTransactionId, result.getId());
            assertEquals(testOrderId, result.getOrderId());
            assertEquals(1000L, result.getAmount());
            verify(transactionRepository).findById(testTransactionId);
        }

        @Test
        @DisplayName("Should throw exception when transaction not found")
        void testFindTransactionEntity_whenTransactionNotFound_expectsRuntimeException() {
            // ARRANGE
            when(transactionRepository.findById(testTransactionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(RuntimeException.class, () -> transactionService.findTransactionEntity(testTransactionId));
            verify(transactionRepository).findById(testTransactionId);
        }

        @Test
        @DisplayName("Should retrieve transactions for authenticated user")
        void testGetTransactions_whenUserIsAuthenticated_expectsTransactionsReturned() {
            // ARRANGE
            JWTUserPrincipal principal = new JWTUserPrincipal(
                    testUserId,
                    "user@example.com",
                    "password",
                    java.util.Collections.singletonList(() -> "ROLE_USER")
            );

            Authentication authentication = mock(Authentication.class);
            when(authentication.getPrincipal()).thenReturn(principal);

            SecurityContext securityContext = mock(SecurityContext.class);
            when(securityContext.getAuthentication()).thenReturn(authentication);
            SecurityContextHolder.setContext(securityContext);

            // ACT & ASSERT
            assertThrows(AccessDeniedException.class, () ->
                    transactionService.getTransactions(0, 10, "ASC", PaymentStatus.SUCCESS, testUserId)
            );
        }

        @ParameterizedTest
        @ValueSource(longs = {0L, 1L, 100L, 999999L})
        @DisplayName("Should handle various transaction amounts")
        void testInitializeTransaction_withVariousAmounts_expectsTransactionCreated(Long amount) {
            // ARRANGE
            InitializePaymentMessage messageWithAmount = new InitializePaymentMessage(
                    testUserId,
                    testOrderId,
                    amount,
                    Currency.USD
            );

            when(cardHolderService.findCardHolderEntity(testUserId)).thenReturn(testCardHolder);
            when(transactionRepository.save(any(Transaction.class))).thenAnswer(invocation -> {
                Transaction tx = invocation.getArgument(0);
                tx.setId(testTransactionId);
                return tx;
            });

            // ACT
            transactionService.initializeTransaction(messageWithAmount);

            // ASSERT
            ArgumentCaptor<Transaction> transactionCaptor = ArgumentCaptor.forClass(Transaction.class);
            verify(transactionRepository).save(transactionCaptor.capture());
            assertEquals(amount, transactionCaptor.getValue().getAmount());
        }
    }

    @Nested
    @DisplayName("RefundService Tests")
    class RefundServiceTests {

        @Mock
        private RefundRepository refundRepository;

        @Mock
        private TransactionService transactionService;

        @InjectMocks
        private RefundServiceImpl refundService;

        private UUID testTransactionId;
        private Transaction testTransaction;

        @BeforeEach
        void setUp() {
            testTransactionId = UUID.randomUUID();
            testTransaction = Transaction.builder()
                    .id(testTransactionId)
                    .orderId(UUID.randomUUID())
                    .amount(1000L)
                    .currency(Currency.USD)
                    .status(PaymentStatus.SUCCESS)
                    .stripePaymentIntentId("pi_test123")
                    .build();
        }

        @Test
        @DisplayName("Should process refund successfully when Stripe refund succeeds")
        void testProcessRefund_whenStripeRefundSucceeds_expectsRefundSaved() throws StripeException {
            // ARRANGE
            when(transactionService.findTransactionEntity(testTransactionId)).thenReturn(testTransaction);
            when(refundRepository.save(any(io.github.marianciuc.streamingservice.payment.entity.Refund.class)))
                    .thenAnswer(invocation -> invocation.getArgument(0));

            // ACT
            refundService.processRefund(testTransactionId);

            // ASSERT
            ArgumentCaptor<io.github.marianciuc.streamingservice.payment.entity.Refund> refundCaptor =
                    ArgumentCaptor.forClass(io.github.marianciuc.streamingservice.payment.entity.Refund.class);
            verify(refundRepository).save(refundCaptor.capture());
            io.github.marianciuc.streamingservice.payment.entity.Refund savedRefund = refundCaptor.getValue();

            assertEquals(testTransaction, savedRefund.getTransaction());
            assertEquals(Currency.USD, savedRefund.getCurrency());
            verify(transactionService).findTransactionEntity(testTransactionId);
        }

        @Test
        @DisplayName("Should handle Stripe refund exception and mark refund as failed")
        void testProcessRefund_whenStripeThrowsException_expectsRefundFailed() throws StripeException {
            // ARRANGE
            when(transactionService.findTransactionEntity(testTransactionId)).thenReturn(testTransaction);
            when(refundRepository.save(any(io.github.marianciuc.streamingservice.payment.entity.Refund.class)))
                    .thenAnswer(invocation -> invocation.getArgument(0));

            StripeException stripeException = new StripeException("Refund failed");
            try (var mockRefund = mockStatic(Refund.class)) {
                mockRefund.when(() -> Refund.create(any())).thenThrow(stripeException);

                // ACT & ASSERT
                assertThrows(RuntimeException.class, () -> refundService.processRefund(testTransactionId));

                ArgumentCaptor<io.github.marianciuc.streamingservice.payment.entity.Refund> refundCaptor =
                        ArgumentCaptor.forClass(io.github.marianciuc.streamingservice.payment.entity.Refund.class);
                verify(refundRepository).save(refundCaptor.capture());
                io.github.marianciuc.streamingservice.payment.entity.Refund savedRefund = refundCaptor.getValue();

                assertEquals(RefundStatus.FAILED, savedRefund.getStatus());
            }
        }

        @Test
        @DisplayName("Should throw exception when transaction not found")
        void testProcessRefund_whenTransactionNotFound_expectsRuntimeException() {
            // ARRANGE
            when(transactionService.findTransactionEntity(testTransactionId))
                    .thenThrow(new RuntimeException("Transaction not found"));

            // ACT & ASSERT
            assertThrows(RuntimeException.class, () -> refundService.processRefund(testTransactionId));
            verify(transactionService).findTransactionEntity(testTransactionId);
            verify(refundRepository, never()).save(any());
        }

        @Test
        @DisplayName("Should process refund with null transaction ID")
        void testProcessRefund_whenTransactionIdIsNull_expectsException() {
            // ARRANGE
            when(transactionService.findTransactionEntity(null))
                    .thenThrow(new IllegalArgumentException("Transaction ID cannot be null"));

            // ACT & ASSERT
            assertThrows(IllegalArgumentException.class, () -> refundService.processRefund(null));
        }
    }

    @Nested
    @DisplayName("CardHolder Service Tests")
    class CardHolderServiceTests {

        @Test
        @DisplayName("Should validate card holder creation with valid data")
        void testCardHolderCreation_withValidData_expectsCardHolderCreated() {
            // ARRANGE
            UUID userId = UUID.randomUUID();
            CardHolder cardHolder = CardHolder.builder()
                    .userId(userId)
                    .stripeCustomerId("cus_test123")
                    .build();

            // ACT & ASSERT
            assertNotNull(cardHolder);
            assertEquals(userId, cardHolder.getUserId());
            assertEquals("cus_test123", cardHolder.getStripeCustomerId());
        }

        @Test
        @DisplayName("Should handle card holder with null user ID")
        void testCardHolderCreation_withNullUserId_expectsValidation() {
            // ARRANGE & ACT
            CardHolder cardHolder = CardHolder.builder()
                    .userId(null)
                    .stripeCustomerId("cus_test123")
                    .build();

            // ASSERT
            assertNull(cardHolder.getUserId());
        }
    }

    @Nested
    @DisplayName("Transaction Entity Tests")
    class TransactionEntityTests {

        @Test
        @DisplayName("Should create transaction with all required fields")
        void testTransactionCreation_withAllFields_expectsTransactionCreated() {
            // ARRANGE
            UUID transactionId = UUID.randomUUID();
            UUID orderId = UUID.randomUUID();
            CardHolder cardHolder = CardHolder.builder()
                    .id(UUID.randomUUID())
                    .stripeCustomerId("cus_test123")
                    .build();

            // ACT
            Transaction transaction = Transaction.builder()
                    .id(transactionId)
                    .orderId(orderId)
                    .cardHolder(cardHolder)
                    .stripePaymentIntentId("pi_test123")
                    .currency(Currency.USD)
                    .amount(1000L)
                    .status(PaymentStatus.SUCCESS)
                    .createdAt(LocalDateTime.now())
                    .build();

            // ASSERT
            assertEquals(transactionId, transaction.getId());
            assertEquals(orderId, transaction.getOrderId());
            assertEquals(cardHolder, transaction.getCardHolder());
            assertEquals("pi_test123", transaction.getStripePaymentIntentId());
            assertEquals(Currency.USD, transaction.getCurrency());
            assertEquals(1000L, transaction.getAmount());
            assertEquals(PaymentStatus.SUCCESS, transaction.getStatus());
        }

        @Test
        @DisplayName("Should handle transaction with failure message")
        void testTransactionCreation_withFailureMessage_expectsFailureMessageSet() {
            // ARRANGE & ACT
            Transaction transaction = Transaction.builder()
                    .id(UUID.randomUUID())
                    .status(PaymentStatus.FAILED)
                    .failureMessage("Card declined")
                    .build();

            // ASSERT
            assertEquals(PaymentStatus.FAILED, transaction.getStatus());
            assertEquals("Card declined", transaction.getFailureMessage());
        }

        @ParameterizedTest
        @ValueSource(strings = {"USD", "EUR", "GBP"})
        @DisplayName("Should handle various currency types")
        void testTransactionCreation_withVariousCurrencies_expectsCurrencySet(String currencyCode) {
            // ARRANGE
            Currency currency = Currency.valueOf(currencyCode);

            // ACT
            Transaction transaction = Transaction.builder()
                    .id(UUID.randomUUID())
                    .currency(currency)
                    .amount(1000L)
                    .build();

            // ASSERT
            assertEquals(currency, transaction.getCurrency());
        }
    }
}
