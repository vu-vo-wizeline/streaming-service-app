package com.streaming.payment.service;

import com.streaming.payment.dto.PaymentRequest;
import com.streaming.payment.dto.PaymentResponse;
import com.streaming.payment.entity.Payment;
import com.streaming.payment.entity.PaymentStatus;
import com.streaming.payment.exception.PaymentProcessingException;
import com.streaming.payment.exception.InvalidPaymentException;
import com.streaming.payment.repository.PaymentRepository;
import com.stripe.exception.StripeException;
import com.stripe.model.Charge;
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

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("PaymentService Unit Tests")
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private StripePaymentGateway stripePaymentGateway;

    @Mock
    private PaymentEventPublisher paymentEventPublisher;

    @InjectMocks
    private PaymentService paymentService;

    private PaymentRequest validPaymentRequest;
    private Payment testPayment;

    @BeforeEach
    void setUp() {
        validPaymentRequest = PaymentRequest.builder()
            .userId("user-123")
            .amount(new BigDecimal("99.99"))
            .currency("USD")
            .stripeTokenId("tok_visa")
            .subscriptionId("sub-456")
            .build();

        testPayment = Payment.builder()
            .id("payment-789")
            .userId("user-123")
            .amount(new BigDecimal("99.99"))
            .currency("USD")
            .status(PaymentStatus.PENDING)
            .stripeChargeId("ch_1234567890")
            .createdAt(LocalDateTime.now())
            .build();
    }

    @Test
    @DisplayName("processPayment_whenValidRequest_expectsSuccessfulCharge")
    void testProcessPaymentSuccess() throws StripeException {
        // ARRANGE
        Charge mockCharge = mock(Charge.class);
        when(mockCharge.getId()).thenReturn("ch_1234567890");
        when(mockCharge.getStatus()).thenReturn("succeeded");
        when(stripePaymentGateway.charge(any())).thenReturn(mockCharge);
        when(paymentRepository.save(any(Payment.class))).thenReturn(testPayment);

        // ACT
        PaymentResponse response = paymentService.processPayment(validPaymentRequest);

        // ASSERT
        assertNotNull(response);
        assertEquals("ch_1234567890", response.getStripeChargeId());
        assertEquals(PaymentStatus.COMPLETED, response.getStatus());
        verify(stripePaymentGateway, times(1)).charge(any());
        verify(paymentRepository, times(1)).save(any(Payment.class));
        verify(paymentEventPublisher, times(1)).publishPaymentSucceeded(any());
    }

    @Test
    @DisplayName("processPayment_whenStripeDeclines_expectsFailedStatus")
    void testProcessPaymentStripeDecline() throws StripeException {
        // ARRANGE
        when(stripePaymentGateway.charge(any()))
            .thenThrow(new StripeException("Your card was declined"));
        when(paymentRepository.save(any(Payment.class))).thenReturn(
            testPayment.toBuilder().status(PaymentStatus.FAILED).build()
        );

        // ACT & ASSERT
        assertThrows(PaymentProcessingException.class, () -> {
            paymentService.processPayment(validPaymentRequest);
        });
        verify(paymentEventPublisher, times(1)).publishPaymentFailed(any());
    }

    @Test
    @DisplayName("processPayment_whenNullAmount_expectsInvalidPaymentException")
    void testProcessPaymentNullAmount() {
        // ARRANGE
        PaymentRequest invalidRequest = validPaymentRequest.toBuilder()
            .amount(null)
            .build();

        // ACT & ASSERT
        assertThrows(InvalidPaymentException.class, () -> {
            paymentService.processPayment(invalidRequest);
        });
        verify(stripePaymentGateway, never()).charge(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"0.00", "-10.00", "-0.01"})
    @DisplayName("processPayment_whenInvalidAmount_expectsInvalidPaymentException")
    void testProcessPaymentInvalidAmounts(String amount) {
        // ARRANGE
        PaymentRequest invalidRequest = validPaymentRequest.toBuilder()
            .amount(new BigDecimal(amount))
            .build();

        // ACT & ASSERT
        assertThrows(InvalidPaymentException.class, () -> {
            paymentService.processPayment(invalidRequest);
        });
    }

    @Test
    @DisplayName("processPayment_whenNullUserId_expectsInvalidPaymentException")
    void testProcessPaymentNullUserId() {
        // ARRANGE
        PaymentRequest invalidRequest = validPaymentRequest.toBuilder()
            .userId(null)
            .build();

        // ACT & ASSERT
        assertThrows(InvalidPaymentException.class, () -> {
            paymentService.processPayment(invalidRequest);
        });
    }

    @Test
    @DisplayName("processPayment_whenEmptyUserId_expectsInvalidPaymentException")
    void testProcessPaymentEmptyUserId() {
        // ARRANGE
        PaymentRequest invalidRequest = validPaymentRequest.toBuilder()
            .userId("")
            .build();

        // ACT & ASSERT
        assertThrows(InvalidPaymentException.class, () -> {
            paymentService.processPayment(invalidRequest);
        });
    }

    @Test
    @DisplayName("processPayment_whenNullToken_expectsInvalidPaymentException")
    void testProcessPaymentNullToken() {
        // ARRANGE
        PaymentRequest invalidRequest = validPaymentRequest.toBuilder()
            .stripeTokenId(null)
            .build();

        // ACT & ASSERT
        assertThrows(InvalidPaymentException.class, () -> {
            paymentService.processPayment(invalidRequest);
        });
    }

    @Test
    @DisplayName("refundPayment_whenValidPaymentId_expectsRefundProcessed")
    void testRefundPaymentSuccess() throws StripeException {
        // ARRANGE
        when(paymentRepository.findById("payment-789")).thenReturn(Optional.of(testPayment));
        when(stripePaymentGateway.refund("ch_1234567890")).thenReturn(true);
        when(paymentRepository.save(any(Payment.class))).thenReturn(
            testPayment.toBuilder().status(PaymentStatus.REFUNDED).build()
        );

        // ACT
        PaymentResponse response = paymentService.refundPayment("payment-789");

        // ASSERT
        assertNotNull(response);
        assertEquals(PaymentStatus.REFUNDED, response.getStatus());
        verify(stripePaymentGateway, times(1)).refund("ch_1234567890");
        verify(paymentEventPublisher, times(1)).publishPaymentRefunded(any());
    }

    @Test
    @DisplayName("refundPayment_whenPaymentNotFound_expectsPaymentNotFoundException")
    void testRefundPaymentNotFound() {
        // ARRANGE
        when(paymentRepository.findById("nonexistent")).thenReturn(Optional.empty());

        // ACT & ASSERT
        assertThrows(PaymentNotFoundException.class, () -> {
            paymentService.refundPayment("nonexistent");
        });
        verify(stripePaymentGateway, never()).refund(any());
    }

    @Test
    @DisplayName("refundPayment_whenAlreadyRefunded_expectsInvalidRefundException")
    void testRefundPaymentAlreadyRefunded() {
        // ARRANGE
        Payment refundedPayment = testPayment.toBuilder()
            .status(PaymentStatus.REFUNDED)
            .build();
        when(paymentRepository.findById("payment-789")).thenReturn(Optional.of(refundedPayment));

        // ACT & ASSERT
        assertThrows(InvalidRefundException.class, () -> {
            paymentService.refundPayment("payment-789");
        });
        verify(stripePaymentGateway, never()).refund(any());
    }

    @Test
    @DisplayName("getPaymentStatus_whenValidPaymentId_expectsCurrentStatus")
    void testGetPaymentStatus() {
        // ARRANGE
        when(paymentRepository.findById("payment-789")).thenReturn(Optional.of(testPayment));

        // ACT
        PaymentStatus status = paymentService.getPaymentStatus("payment-789");

        // ASSERT
        assertEquals(PaymentStatus.PENDING, status);
        verify(paymentRepository, times(1)).findById("payment-789");
    }

    @Test
    @DisplayName("getPaymentStatus_whenPaymentNotFound_expectsPaymentNotFoundException")
    void testGetPaymentStatusNotFound() {
        // ARRANGE
        when(paymentRepository.findById("nonexistent")).thenReturn(Optional.empty());

        // ACT & ASSERT
        assertThrows(PaymentNotFoundException.class, () -> {
            paymentService.getPaymentStatus("nonexistent");
        });
    }

    @Test
    @DisplayName("processPayment_whenStripeNetworkError_expectsPaymentProcessingException")
    void testProcessPaymentNetworkError() throws StripeException {
        // ARRANGE
        when(stripePaymentGateway.charge(any()))
            .thenThrow(new StripeException("Network error"));

        // ACT & ASSERT
        assertThrows(PaymentProcessingException.class, () -> {
            paymentService.processPayment(validPaymentRequest);
        });
        verify(paymentEventPublisher, times(1)).publishPaymentFailed(any());
    }

    @Test
    @DisplayName("processPayment_whenRepositorySaveFails_expectsPaymentProcessingException")
    void testProcessPaymentRepositoryFailure() throws StripeException {
        // ARRANGE
        Charge mockCharge = mock(Charge.class);
        when(mockCharge.getId()).thenReturn("ch_1234567890");
        when(mockCharge.getStatus()).thenReturn("succeeded");
        when(stripePaymentGateway.charge(any())).thenReturn(mockCharge);
        when(paymentRepository.save(any(Payment.class)))
            .thenThrow(new RuntimeException("Database error"));

        // ACT & ASSERT
        assertThrows(PaymentProcessingException.class, () -> {
            paymentService.processPayment(validPaymentRequest);
        });
    }

    @ParameterizedTest
    @CsvSource({
        "99.99, USD, COMPLETED",
        "199.99, EUR, COMPLETED",
        "9.99, GBP, COMPLETED"
    })
    @DisplayName("processPayment_withMultipleCurrencies_expectsSuccess")
    void testProcessPaymentMultipleCurrencies(String amount, String currency, String expectedStatus) throws StripeException {
        // ARRANGE
        PaymentRequest request = validPaymentRequest.toBuilder()
            .amount(new BigDecimal(amount))
            .currency(currency)
            .build();
        Charge mockCharge = mock(Charge.class);
        when(mockCharge.getId()).thenReturn("ch_1234567890");
        when(mockCharge.getStatus()).thenReturn("succeeded");
        when(stripePaymentGateway.charge(any())).thenReturn(mockCharge);
        when(paymentRepository.save(any(Payment.class))).thenReturn(testPayment);

        // ACT
        PaymentResponse response = paymentService.processPayment(request);

        // ASSERT
        assertNotNull(response);
        assertEquals(PaymentStatus.COMPLETED, response.getStatus());
    }

    @Test
    @DisplayName("processPayment_capturesPaymentDetails_expectsCorrectDataPersisted")
    void testProcessPaymentCapturesDetails() throws StripeException {
        // ARRANGE
        Charge mockCharge = mock(Charge.class);
        when(mockCharge.getId()).thenReturn("ch_1234567890");
        when(mockCharge.getStatus()).thenReturn("succeeded");
        when(stripePaymentGateway.charge(any())).thenReturn(mockCharge);
        when(paymentRepository.save(any(Payment.class))).thenReturn(testPayment);

        ArgumentCaptor<Payment> paymentCaptor = ArgumentCaptor.forClass(Payment.class);

        // ACT
        paymentService.processPayment(validPaymentRequest);

        // ASSERT
        verify(paymentRepository).save(paymentCaptor.capture());
        Payment capturedPayment = paymentCaptor.getValue();
        assertEquals("user-123", capturedPayment.getUserId());
        assertEquals(new BigDecimal("99.99"), capturedPayment.getAmount());
        assertEquals("USD", capturedPayment.getCurrency());
        assertEquals("ch_1234567890", capturedPayment.getStripeChargeId());
    }
}
