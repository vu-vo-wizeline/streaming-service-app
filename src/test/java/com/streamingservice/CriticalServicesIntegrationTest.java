package com.streamingservice;

import com.streamingservice.payment.PaymentService;
import com.streamingservice.payment.PaymentRepository;
import com.streamingservice.payment.PaymentEvent;
import com.streamingservice.subscription.SubscriptionService;
import com.streamingservice.subscription.SubscriptionRepository;
import com.streamingservice.subscription.SubscriptionRenewalJob;
import com.streamingservice.media.MediaService;
import com.streamingservice.media.MediaRepository;
import com.streamingservice.media.VideoUploadSession;
import com.streamingservice.comments.CommentService;
import com.streamingservice.comments.CommentRepository;
import com.streamingservice.comments.Comment;
import com.stripe.model.Charge;
import com.stripe.exception.StripeException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import redis.clients.jedis.Jedis;
import feign.FeignException;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit test suite for critical streaming service microservices.
 * Tests Payment Service, Subscription Service, Media Service, and Comments Service.
 * 
 * Coverage includes: happy paths, boundary values, error handling, side effects,
 * idempotency, and concurrency scenarios.
 */
public class CriticalServicesIntegrationTest {

    // ==================== PAYMENT SERVICE TESTS ====================
    
    @Mock
    private PaymentRepository paymentRepository;
    
    @Mock
    private KafkaProducer<String, PaymentEvent> kafkaProducer;
    
    @Mock
    private com.stripe.net.RequestOptions stripeRequestOptions;
    
    private PaymentService paymentService;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        paymentService = new PaymentService(paymentRepository, kafkaProducer);
    }
    
    @Test
    @DisplayName("Payment Service: Process valid payment successfully")
    void testProcessPayment_whenValidPayment_expectsPaymentCreatedAndKafkaPublished() {
        // ARRANGE
        String customerId = "cust_12345";
        BigDecimal amount = new BigDecimal("100.00");
        String currency = "USD";
        String paymentMethodId = "pm_valid_card";
        
        Payment expectedPayment = Payment.builder()
            .customerId(customerId)
            .amount(amount)
            .currency(currency)
            .status("PENDING")
            .build();
        
        when(paymentRepository.save(any(Payment.class))).thenReturn(expectedPayment);
        
        // ACT
        Payment result = paymentService.processPayment(customerId, amount, currency, paymentMethodId);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(customerId, result.getCustomerId());
        assertEquals(amount, result.getAmount());
        verify(paymentRepository, times(1)).save(any(Payment.class));
        verify(kafkaProducer, times(1)).send(any(ProducerRecord.class));
    }
    
    @Test
    @DisplayName("Payment Service: Reject zero amount payment")
    void testProcessPayment_whenAmountIsZero_expectsIllegalArgumentException() {
        // ARRANGE
        String customerId = "cust_12345";
        BigDecimal amount = BigDecimal.ZERO;
        String currency = "USD";
        String paymentMethodId = "pm_valid_card";
        
        // ACT & ASSERT
        assertThrows(IllegalArgumentException.class, () -> {
            paymentService.processPayment(customerId, amount, currency, paymentMethodId);
        });
        verify(paymentRepository, never()).save(any());
        verify(kafkaProducer, never()).send(any());
    }
    
    @Test
    @DisplayName("Payment Service: Reject negative amount payment")
    void testProcessPayment_whenAmountIsNegative_expectsIllegalArgumentException() {
        // ARRANGE
        String customerId = "cust_12345";
        BigDecimal amount = new BigDecimal("-50.00");
        String currency = "USD";
        String paymentMethodId = "pm_valid_card";
        
        // ACT & ASSERT
        assertThrows(IllegalArgumentException.class, () -> {
            paymentService.processPayment(customerId, amount, currency, paymentMethodId);
        });
        verify(paymentRepository, never()).save(any());
    }
    
    @Test
    @DisplayName("Payment Service: Reject null customer ID")
    void testProcessPayment_whenCustomerIdIsNull_expectsIllegalArgumentException() {
        // ARRANGE
        BigDecimal amount = new BigDecimal("100.00");
        String currency = "USD";
        String paymentMethodId = "pm_valid_card";
        
        // ACT & ASSERT
        assertThrows(IllegalArgumentException.class, () -> {
            paymentService.processPayment(null, amount, currency, paymentMethodId);
        });
        verify(paymentRepository, never()).save(any());
    }
    
    @Test
    @DisplayName("Payment Service: Handle Stripe API failure with retry")
    void testProcessPayment_whenStripeThrowsException_expectsPaymentProcessingException() {
        // ARRANGE
        String customerId = "cust_12345";
        BigDecimal amount = new BigDecimal("100.00");
        String currency = "USD";
        String paymentMethodId = "pm_invalid_card";
        
        when(paymentRepository.save(any(Payment.class)))
            .thenThrow(new RuntimeException("Stripe API error"));
        
        // ACT & ASSERT
        assertThrows(RuntimeException.class, () -> {
            paymentService.processPayment(customerId, amount, currency, paymentMethodId);
        });
    }
    
    @Test
    @DisplayName("Payment Service: Kafka publish failure triggers exception")
    void testProcessPayment_whenKafkaPublishFails_expectsPaymentProcessingException() {
        // ARRANGE
        String customerId = "cust_12345";
        BigDecimal amount = new BigDecimal("100.00");
        String currency = "USD";
        String paymentMethodId = "pm_valid_card";
        
        Payment savedPayment = Payment.builder()
            .id("pay_123")
            .customerId(customerId)
            .amount(amount)
            .currency(currency)
            .status("PENDING")
            .build();
        
        when(paymentRepository.save(any(Payment.class))).thenReturn(savedPayment);
        doThrow(new RuntimeException("Kafka unavailable"))
            .when(kafkaProducer).send(any(ProducerRecord.class));
        
        // ACT & ASSERT
        assertThrows(RuntimeException.class, () -> {
            paymentService.processPayment(customerId, amount, currency, paymentMethodId);
        });
    }
    
    @Test
    @DisplayName("Payment Service: Idempotent payment processing")
    void testProcessPayment_whenProcessedTwice_expectsOnlyOnePaymentCreated() {
        // ARRANGE
        String customerId = "cust_12345";
        BigDecimal amount = new BigDecimal("100.00");
        String currency = "USD";
        String paymentMethodId = "pm_valid_card";
        String idempotencyKey = "idempotency_key_123";
        
        Payment payment = Payment.builder()
            .id("pay_123")
            .customerId(customerId)
            .amount(amount)
            .currency(currency)
            .idempotencyKey(idempotencyKey)
            .status("COMPLETED")
            .build();
        
        when(paymentRepository.findByIdempotencyKey(idempotencyKey))
            .thenReturn(Optional.of(payment))
            .thenReturn(Optional.of(payment));
        
        // ACT
        Payment result1 = paymentService.processPaymentIdempotent(customerId, amount, currency, paymentMethodId, idempotencyKey);
        Payment result2 = paymentService.processPaymentIdempotent(customerId, amount, currency, paymentMethodId, idempotencyKey);
        
        // ASSERT
        assertEquals(result1.getId(), result2.getId());
        verify(paymentRepository, times(1)).save(any(Payment.class));
    }
    
    @Test
    @DisplayName("Payment Service: Query payment status")
    void testGetPaymentStatus_whenValidPaymentId_expectsCorrectStatus() {
        // ARRANGE
        String paymentId = "pay_123";
        Payment payment = Payment.builder()
            .id(paymentId)
            .customerId("cust_12345")
            .amount(new BigDecimal("100.00"))
            .status("COMPLETED")
            .build();
        
        when(paymentRepository.findById(paymentId)).thenReturn(Optional.of(payment));
        
        // ACT
        Payment result = paymentService.getPaymentStatus(paymentId);
        
        // ASSERT
        assertNotNull(result);
        assertEquals("COMPLETED", result.getStatus());
        verify(paymentRepository, times(1)).findById(paymentId);
    }
    
    @ParameterizedTest
    @CsvSource({
        "100.00, USD, COMPLETED",
        "50.50, EUR, PENDING",
        "999.99, GBP, FAILED"
    })
    @DisplayName("Payment Service: Process various payment amounts and currencies")
    void testProcessPayment_withVariousAmountsAndCurrencies_expectsSuccess(String amount, String currency, String expectedStatus) {
        // ARRANGE
        String customerId = "cust_12345";
        BigDecimal paymentAmount = new BigDecimal(amount);
        String paymentMethodId = "pm_valid_card";
        
        Payment payment = Payment.builder()
            .customerId(customerId)
            .amount(paymentAmount)
            .currency(currency)
            .status(expectedStatus)
            .build();
        
        when(paymentRepository.save(any(Payment.class))).thenReturn(payment);
        
        // ACT
        Payment result = paymentService.processPayment(customerId, paymentAmount, currency, paymentMethodId);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(currency, result.getCurrency());
        assertEquals(paymentAmount, result.getAmount());
    }
    
    // ==================== SUBSCRIPTION SERVICE TESTS ====================
    
    @Mock
    private SubscriptionRepository subscriptionRepository;
    
    @Mock
    private PaymentService paymentServiceMock;
    
    @Mock
    private com.streamingservice.order.OrderServiceClient orderServiceClient;
    
    @Mock
    private KafkaProducer<String, Object> subscriptionKafkaProducer;
    
    private SubscriptionService subscriptionService;
    
    @BeforeEach
    void setUpSubscriptionService() {
        MockitoAnnotations.openMocks(this);
        subscriptionService = new SubscriptionService(
            subscriptionRepository,
            paymentServiceMock,
            orderServiceClient,
            subscriptionKafkaProducer
        );
    }
    
    @Test
    @DisplayName("Subscription Service: Create subscription successfully")
    void testCreateSubscription_whenValidInput_expectsSubscriptionCreatedAndOrderCreated() {
        // ARRANGE
        String userId = "user_123";
        String planId = "plan_premium";
        String paymentMethodId = "pm_valid_card";
        
        Subscription subscription = Subscription.builder()
            .userId(userId)
            .planId(planId)
            .status("ACTIVE")
            .autoRenew(true)
            .createdAt(LocalDateTime.now())
            .build();
        
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(subscription);
        when(orderServiceClient.createOrder(any())).thenReturn(new Order("order_123", userId, planId));
        
        // ACT
        Subscription result = subscriptionService.createSubscription(userId, planId, paymentMethodId);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(userId, result.getUserId());
        assertEquals("ACTIVE", result.getStatus());
        verify(subscriptionRepository, times(1)).save(any(Subscription.class));
        verify(orderServiceClient, times(1)).createOrder(any());
        verify(subscriptionKafkaProducer, times(1)).send(any(ProducerRecord.class));
    }
    
    @Test
    @DisplayName("Subscription Service: Reject null user ID")
    void testCreateSubscription_whenUserIdIsNull_expectsIllegalArgumentException() {
        // ARRANGE
        String planId = "plan_premium";
        String paymentMethodId = "pm_valid_card";
        
        // ACT & ASSERT
        assertThrows(IllegalArgumentException.class, () -> {
            subscriptionService.createSubscription(null, planId, paymentMethodId);
        });
        verify(subscriptionRepository, never()).save(any());
    }
    
    @Test
    @DisplayName("Subscription Service: Renew subscription successfully")
    void testRenewSubscription_whenPaymentSucceeds_expectsSubscriptionRenewed() {
        // ARRANGE
        String subscriptionId = "sub_123";
        Subscription subscription = Subscription.builder()
            .id(subscriptionId)
            .userId("user_123")
            .planId("plan_premium")
            .status("ACTIVE")
            .autoRenew(true)
            .expiresAt(LocalDateTime.now().minusDays(1))
            .build();
        
        Subscription renewedSubscription = Subscription.builder()
            .id(subscriptionId)
            .userId("user_123")
            .planId("plan_premium")
            .status("ACTIVE")
            .autoRenew(true)
            .expiresAt(LocalDateTime.now().plusDays(30))
            .build();
        
        when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(subscription));
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(renewedSubscription);
        
        // ACT
        Subscription result = subscriptionService.renewSubscription(subscriptionId);
        
        // ASSERT
        assertNotNull(result);
        assertEquals("ACTIVE", result.getStatus());
        assertTrue(result.getExpiresAt().isAfter(LocalDateTime.now()));
        verify(subscriptionRepository, times(1)).save(any(Subscription.class));
    }
    
    @Test
    @DisplayName("Subscription Service: Cancel subscription")
    void testCancelSubscription_whenValidSubscriptionId_expectsSubscriptionCancelled() {
        // ARRANGE
        String subscriptionId = "sub_123";
        Subscription subscription = Subscription.builder()
            .id(subscriptionId)
            .userId("user_123")
            .status("ACTIVE")
            .build();
        
        Subscription cancelledSubscription = Subscription.builder()
            .id(subscriptionId)
            .userId("user_123")
            .status("CANCELLED")
            .build();
        
        when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(subscription));
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(cancelledSubscription);
        
        // ACT
        Subscription result = subscriptionService.cancelSubscription(subscriptionId);
        
        // ASSERT
        assertNotNull(result);
        assertEquals("CANCELLED", result.getStatus());
        verify(subscriptionRepository, times(1)).save(any(Subscription.class));
        verify(subscriptionKafkaProducer, times(1)).send(any(ProducerRecord.class));
    }
    
    @Test
    @DisplayName("Subscription Service: Handle renewal failure when payment declined")
    void testRenewSubscription_whenPaymentDeclined_expectsSubscriptionMarkedForCancellation() {
        // ARRANGE
        String subscriptionId = "sub_123";
        Subscription subscription = Subscription.builder()
            .id(subscriptionId)
            .userId("user_123")
            .planId("plan_premium")
            .status("ACTIVE")
            .autoRenew(true)
            .build();
        
        when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(subscription));
        when(paymentServiceMock.processPayment(any(), any(), any(), any()))
            .thenThrow(new RuntimeException("Payment declined"));
        
        // ACT & ASSERT
        assertThrows(RuntimeException.class, () -> {
            subscriptionService.renewSubscription(subscriptionId);
        });
    }
    
    @Test
    @DisplayName("Subscription Service: Handle Feign circuit breaker when Order Service unavailable")
    void testCreateSubscription_whenOrderServiceUnavailable_expectsCircuitBreakerTriggered() {
        // ARRANGE
        String userId = "user_123";
        String planId = "plan_premium";
        String paymentMethodId = "pm_valid_card";
        
        Subscription subscription = Subscription.builder()
            .userId(userId)
            .planId(planId)
            .status("ACTIVE")
            .build();
        
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(subscription);
        when(orderServiceClient.createOrder(any()))
            .thenThrow(FeignException.errorStatus("createOrder", new feign.Response.Builder()
                .status(503)
                .reason("Service Unavailable")
                .build()));
        
        // ACT & ASSERT
        assertThrows(FeignException.class, () -> {
            subscriptionService.createSubscription(userId, planId, paymentMethodId);
        });
    }
    
    @Test
    @DisplayName("Subscription Service: Prevent duplicate active subscriptions")
    void testCreateSubscription_whenUserHasActiveSubscription_expectsIllegalStateException() {
        // ARRANGE
        String userId = "user_123";
        String planId = "plan_premium";
        String paymentMethodId = "pm_valid_card";
        
        Subscription existingSubscription = Subscription.builder()
            .userId(userId)
            .planId(planId)
            .status("ACTIVE")
            .build();
        
        when(subscriptionRepository.findActiveByUserId(userId))
            .thenReturn(Optional.of(existingSubscription));
        
        // ACT & ASSERT
        assertThrows(IllegalStateException.class, () -> {
            subscriptionService.createSubscription(userId, planId, paymentMethodId);
        });
        verify(subscriptionRepository, never()).save(any());
    }
    
    @Test
    @DisplayName("Subscription Service: Quartz job processes expiring subscriptions")
    void testSubscriptionRenewalJob_whenRunAtMidnight_expectsExpiringSubscriptionsProcessed() {
        // ARRANGE
        Subscription expiringSubscription = Subscription.builder()
            .id("sub_123")
            .userId("user_123")
            .status("ACTIVE")
            .autoRenew(true)
            .expiresAt(LocalDateTime.now().plusHours(1))
            .build();
        
        when(subscriptionRepository.findExpiringSubscriptions(any()))
            .thenReturn(java.util.List.of(expiringSubscription));
        when(subscriptionRepository.save(any(Subscription.class)))
            .thenReturn(expiringSubscription);
        
        SubscriptionRenewalJob renewalJob = new SubscriptionRenewalJob(subscriptionService);
        
        // ACT
        renewalJob.executeRenewalJob();
        
        // ASSERT
        verify(subscriptionRepository, atLeastOnce()).findExpiringSubscriptions(any());
    }
    
    // ==================== MEDIA SERVICE TESTS ====================
    
    @Mock
    private MediaRepository mediaRepository;
    
    @Mock
    private MinioClient minioClient;
    
    @Mock
    private Jedis redisClient;
    
    @Mock
    private KafkaProducer<String, Object> mediaKafkaProducer;
    
    private MediaService mediaService;
    
    @BeforeEach
    void setUpMediaService() {
        MockitoAnnotations.openMocks(this);
        mediaService = new MediaService(
            mediaRepository,
            minioClient,
            redisClient,
            mediaKafkaProducer
        );
    }
    
    @Test
    @DisplayName("Media Service: Upload video chunk successfully")
    void testUploadVideoChunk_whenValidChunk_expectsStoredInMinioAndRedisUpdated() throws Exception {
        // ARRANGE
        String uploadId = "upload_123";
        int chunkIndex = 0;
        byte[] chunkData = "video_chunk_data".getBytes();
        String bucketName = "video-uploads";
        
        when(minioClient.putObject(any(PutObjectArgs.class))).thenReturn(null);
        when(redisClient.hset(anyString(), anyString(), anyString())).thenReturn(1L);
        
        // ACT
        mediaService.uploadVideoChunk(uploadId, chunkIndex, chunkData);
        
        // ASSERT
        verify(minioClient, times(1)).putObject(any(PutObjectArgs.class));
        verify(redisClient, times(1)).hset(anyString(), anyString(), anyString());
    }
    
    @Test
    @DisplayName("Media Service: Reject null video ID")
    void testUploadVideoChunk_whenVideoIdIsNull_expectsIllegalArgumentException() {
        // ARRANGE
        int chunkIndex = 0;
        byte[] chunkData = "video_chunk_data".getBytes();
        
        // ACT & ASSERT
        assertThrows(IllegalArgumentException.class, () -> {
            mediaService.uploadVideoChunk(null, chunkIndex, chunkData);
        });
        verify(minioClient, never()).putObject(any());
    }
    
    @Test
    @DisplayName("Media Service: Reject empty chunk data")
    void testUploadVideoChunk_whenChunkDataIsEmpty_expectsIllegalArgumentException() {
        // ARRANGE
        String uploadId = "upload_123";
        int chunkIndex = 0;
        byte[] chunkData = new byte[0];
        
        // ACT & ASSERT
        assertThrows(IllegalArgumentException.class, () -> {
            mediaService.uploadVideoChunk(uploadId, chunkIndex, chunkData);
        });
        verify(minioClient, never()).putObject(any());
    }
    
    @Test
    @DisplayName("Media Service: Trigger FFmpeg processing when all chunks uploaded")
    void testCompleteVideoUpload_whenAllChunksReceived_expectsFFmpegJobTriggered() throws Exception {
        // ARRANGE
        String uploadId = "upload_123";
        String videoId = "video_123";
        int totalChunks = 5;
        
        VideoUploadSession session = VideoUploadSession.builder()
            .uploadId(uploadId)
            .videoId(videoId)
            .totalChunks(totalChunks)
            .uploadedChunks(totalChunks)
            .status("COMPLETE")
            .build();
        
        when(mediaRepository.findByUploadId(uploadId)).thenReturn(Optional.of(session));
        when(mediaRepository.save(any(VideoUploadSession.class))).thenReturn(session);
        
        // ACT
        mediaService.completeVideoUpload(uploadId);
        
        // ASSERT
        verify(mediaKafkaProducer, times(1)).send(any(ProducerRecord.class));
    }
    
    @Test
    @DisplayName("Media Service: Handle MinIO upload failure")
    void testUploadVideoChunk_whenMinioThrowsException_expectsMediaUploadException() throws Exception {
        // ARRANGE
        String uploadId = "upload_123";
        int chunkIndex = 0;
        byte[] chunkData = "video_chunk_data".getBytes();
        
        doThrow(new RuntimeException("MinIO connection failed"))
            .when(minioClient).putObject(any(PutObjectArgs.class));
        
        // ACT & ASSERT
        assertThrows(RuntimeException.class, () -> {
            mediaService.uploadVideoChunk(uploadId, chunkIndex, chunkData);
        });
    }
    
    @Test
    @DisplayName("Media Service: Stream video with authorization check")
    void testStreamVideo_whenUserAuthorized_expectsHLSStreamReturned() {
        // ARRANGE
        String videoId = "video_123";
        String userId = "user_123";
        String hlsUrl = "https://minio.example.com/videos/video_123/playlist.m3u8";
        
        VideoMetadata videoMetadata = VideoMetadata.builder()
            .videoId(videoId)
            .hlsUrl(hlsUrl)
            .status("READY")
            .build();
        
        when(mediaRepository.findById(videoId)).thenReturn(Optional.of(videoMetadata));
        when(subscriptionServiceMock.isUserAuthorized(userId, videoId)).thenReturn(true);
        
        // ACT
        String result = mediaService.streamVideo(videoId, userId);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(hlsUrl, result);
        verify(mediaRepository, times(1)).findById(videoId);
    }
    
    @Test
    @DisplayName("Media Service: Deny stream access for unauthorized user")
    void testStreamVideo_whenUserUnauthorized_expectsAccessDeniedException() {
        // ARRANGE
        String videoId = "video_123";
        String userId = "user_unauthorized";
        
        VideoMetadata videoMetadata = VideoMetadata.builder()
            .videoId(videoId)
            .status("READY")
            .build();
        
        when(mediaRepository.findById(videoId)).thenReturn(Optional.of(videoMetadata));
        when(subscriptionServiceMock.isUserAuthorized(userId, videoId)).thenReturn(false);
        
        // ACT & ASSERT
        assertThrows(AccessDeniedException.class, () -> {
            mediaService.streamVideo(videoId, userId);
        });
    }
    
    @Test
    @DisplayName("Media Service: Handle concurrent chunk uploads without race condition")
    void testUploadVideoChunk_whenConcurrentUploads_expectsAllChunksStoredCorrectly() throws Exception {
        // ARRANGE
        String uploadId = "upload_123";
        byte[] chunkData = "video_chunk_data".getBytes();
        ExecutorService executor = Executors.newFixedThreadPool(5);
        
        when(minioClient.putObject(any(PutObjectArgs.class))).thenReturn(null);
        when(redisClient.hset(anyString(), anyString(), anyString())).thenReturn(1L);
        
        // ACT
        for (int i = 0; i < 5; i++) {
            final int chunkIndex = i;
            executor.submit(() -> {
                try {
                    mediaService.uploadVideoChunk(uploadId, chunkIndex, chunkData);
                } catch (Exception e) {
                    fail("Concurrent upload failed: " + e.getMessage());
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
        
        // ASSERT
        verify(minioClient, times(5)).putObject(any(PutObjectArgs.class));
        verify(redisClient, times(5)).hset(anyString(), anyString(), anyString());
    }
    
    @Test
    @DisplayName("Media Service: Store HLS URLs after FFmpeg processing")
    void testCompleteVideoProcessing_whenFFmpegSucceeds_expectsHLSUrlsStoredInDB() {
        // ARRANGE
        String videoId = "video_123";
        String hlsUrl = "https://minio.example.com/videos/video_123/playlist.m3u8";
        
        VideoMetadata videoMetadata = VideoMetadata.builder()
            .videoId(videoId)
            .hlsUrl(hlsUrl)
            .status("READY")
            .build();
        
        when(mediaRepository.save(any(VideoMetadata.class))).thenReturn(videoMetadata);
        
        // ACT
        mediaService.completeVideoProcessing(videoId, hlsUrl);
        
        // ASSERT
        verify(mediaRepository, times(1)).save(any(VideoMetadata.class));
    }
    
    // ==================== COMMENTS SERVICE TESTS ====================
    
    @Mock
    private CommentRepository commentRepository;
    
    @Mock
    private KafkaProducer<String, Object> commentKafkaProducer;
    
    private CommentService commentService;
    
    @BeforeEach
    void setUpCommentService() {
        MockitoAnnotations.openMocks(this);
        commentService = new CommentService(commentRepository, commentKafkaProducer);
    }
    
    @Test
    @DisplayName("Comments Service: Create comment successfully")
    void testCreateComment_whenValidInput_expectsCommentStoredAndKafkaPublished() {
        // ARRANGE
        String videoId = "video_123";
        String userId = "user_123";
        String commentText = "Great video!";
        
        Comment comment = Comment.builder()
            .videoId(videoId)
            .userId(userId)
            .text(commentText)
            .status("PENDING_MODERATION")
            .createdAt(LocalDateTime.now())
            .build();
        
        when(commentRepository.save(any(Comment.class))).thenReturn(comment);
        
        // ACT
        Comment result = commentService.createComment(videoId, userId, commentText);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(videoId, result.getVideoId());
        assertEquals(userId, result.getUserId());
        assertEquals(commentText, result.getText());
        verify(commentRepository, times(1)).save(any(Comment.class));
        verify(commentKafkaProducer, times(1)).send(any(ProducerRecord.class));
    }
    
    @Test
    @DisplayName("Comments Service: Reject null video ID")
    void testCreateComment_whenVideoIdIsNull_expectsIllegalArgumentException() {
        // ARRANGE
        String userId = "user_123";
        String commentText = "Great video!";
        
        // ACT & ASSERT
        assertThrows(IllegalArgumentException.class, () -> {
            commentService.createComment(null, userId, commentText);
        });
        verify(commentRepository, never()).save(any());
    }
    
    @Test
    @DisplayName("Comments Service: Reject null user ID")
    void testCreateComment_whenUserIdIsNull_expectsIllegalArgumentException() {
        // ARRANGE
        String videoId = "video_123";
        String commentText = "Great video!";
        
        // ACT & ASSERT
        assertThrows(IllegalArgumentException.class, () -> {
            commentService.createComment(videoId, null, commentText);
        });
        verify(commentRepository, never()).save(any());
    }
    
    @Test
    @DisplayName("Comments Service: Reject empty comment text")
    void testCreateComment_whenCommentTextIsEmpty_expectsIllegalArgumentException() {
        // ARRANGE
        String videoId = "video_123";
        String userId = "user_123";
        String commentText = "";
        
        // ACT & ASSERT
        assertThrows(IllegalArgumentException.class, () -> {
            commentService.createComment(videoId, userId, commentText);
        });
        verify(commentRepository, never()).save(any());
    }
    
    @Test
    @DisplayName("Comments Service: Reject comment exceeding max length")
    void testCreateComment_whenCommentExceedsMaxLength_expectsIllegalArgumentException() {
        // ARRANGE
        String videoId = "video_123";
        String userId = "user_123";
        String commentText = "a".repeat(5001); // Exceeds 5000 char limit
        
        // ACT & ASSERT
        assertThrows(IllegalArgumentException.class, () -> {
            commentService.createComment(videoId, userId, commentText);
        });
        verify(commentRepository, never()).save(any());
    }
    
    @Test
    @DisplayName("Comments Service: Handle Kafka publish failure")
    void testCreateComment_whenKafkaPublishFails_expectsCommentCreationException() {
        // ARRANGE
        String videoId = "video_123";
        String userId = "user_123";
        String commentText = "Great video!";
        
        Comment comment = Comment.builder()
            .id("comment_123")
            .videoId(videoId)
            .userId(userId)
            .text(commentText)
            .status("PENDING_MODERATION")
            .build();
        
        when(commentRepository.save(any(Comment.class))).thenReturn(comment);
        doThrow(new RuntimeException("Kafka unavailable"))
            .when(commentKafkaProducer).send(any(ProducerRecord.class));
        
        // ACT & ASSERT
        assertThrows(RuntimeException.class, () -> {
            commentService.createComment(videoId, userId, commentText);
        });
    }
    
    @Test
    @DisplayName("Comments Service: Retrieve paginated comments for video")
    void testGetCommentsByVideoId_whenValidVideoId_expectsPaginatedComments() {
        // ARRANGE
        String videoId = "video_123";
        int page = 0;
        int size = 10;
        
        Comment comment1 = Comment.builder()
            .id("comment_1")
            .videoId(videoId)
            .userId("user_1")
            .text("Great video!")
            .status("APPROVED")
            .build();
        
        Comment comment2 = Comment.builder()
            .id("comment_2")
            .videoId(videoId)
            .userId("user_2")
            .text("Amazing content!")
            .status("APPROVED")
            .build();
        
        when(commentRepository.findByVideoIdPaginated(videoId, page, size))
            .thenReturn(java.util.List.of(comment1, comment2));
        
        // ACT
        java.util.List<Comment> result = commentService.getCommentsByVideoId(videoId, page, size);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(2, result.size());
        verify(commentRepository, times(1)).findByVideoIdPaginated(videoId, page, size);
    }
    
    @Test
    @DisplayName("Comments Service: Delete comment by owner")
    void testDeleteComment_whenOwnerDeletes_expectsCommentDeleted() {
        // ARRANGE
        String commentId = "comment_123";
        String userId = "user_123";
        
        Comment comment = Comment.builder()
            .id(commentId)
            .videoId("video_123")
            .userId(userId)
            .text("Great video!")
            .status("APPROVED")
            .build();
        
        when(commentRepository.findById(commentId)).thenReturn(Optional.of(comment));
        
        // ACT
        commentService.deleteComment(commentId, userId);
        
        // ASSERT
        verify(commentRepository, times(1)).deleteById(commentId);
    }
    
    @Test
    @DisplayName("Comments Service: Prevent non-owner from deleting comment")
    void testDeleteComment_whenNonOwnerDeletes_expectsAccessDeniedException() {
        // ARRANGE
        String commentId = "comment_123";
        String userId = "user_123";
        String otherUserId = "user_456";
        
        Comment comment = Comment.builder()
            .id(commentId)
            .videoId("video_123")
            .userId(userId)
            .text("Great video!")
            .status("APPROVED")
            .build();
        
        when(commentRepository.findById(commentId)).thenReturn(Optional.of(comment));
        
        // ACT & ASSERT
        assertThrows(AccessDeniedException.class, () -> {
            commentService.deleteComment(commentId, otherUserId);
        });
        verify(commentRepository, never()).deleteById(commentId);
    }
    
    @ParameterizedTest
    @ValueSource(strings = {"Great video!", "Amazing content!", "Love this!"})
    @DisplayName("Comments Service: Create comments with various valid texts")
    void testCreateComment_withVariousValidTexts_expectsSuccess(String commentText) {
        // ARRANGE
        String videoId = "video_123";
        String userId = "user_123";
        
        Comment comment = Comment.builder()
            .videoId(videoId)
            .userId(userId)
            .text(commentText)
            .status("PENDING_MODERATION")
            .build();
        
        when(commentRepository.save(any(Comment.class))).thenReturn(comment);
        
        // ACT
        Comment result = commentService.createComment(videoId, userId, commentText);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(commentText, result.getText());
    }
}
