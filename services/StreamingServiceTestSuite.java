package io.github.streaming.service.test;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.LocalDateTime;
import java.util.*;

/**
 * Comprehensive Unit Test Suite for Streaming Service Microservices
 * 
 * This test suite covers:
 * - User Service: Authentication, JWT token management, user registration
 * - Payment Service: Payment processing, Stripe integration, transaction management
 * - Subscription Service: Subscription lifecycle, renewal jobs, status tracking
 * - Content Service: Content metadata, catalog management
 * - Media Service: Video processing, transcoding, streaming access control
 * - Comments Service: Comment creation, retrieval, storage
 * - Moderation Service: Content moderation, flagging
 * - Customer Service: Customer profiles, account management
 * 
 * Framework: JUnit 5 with Mockito
 * Pattern: Arrange-Act-Assert (AAA)
 */

// ============================================================================
// USER SERVICE TESTS
// ============================================================================

@ExtendWith(MockitoExtension.class)
@DisplayName("User Service Tests")
class UserServiceTest {
    
    private UserService userService;
    
    @Mock
    private UserRepository userRepository;
    
    @Mock
    private JwtTokenProvider jwtTokenProvider;
    
    @Mock
    private PasswordEncoder passwordEncoder;
    
    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @BeforeEach
    void setUp() {
        userService = new UserService(userRepository, jwtTokenProvider, passwordEncoder, kafkaTemplate);
    }
    
    @Test
    @DisplayName("User registration with valid credentials should create user and publish event")
    void testRegisterUser_whenValidCredentials_expectsUserCreatedAndEventPublished() {
        // ARRANGE
        String email = "user@example.com";
        String password = "SecurePassword123!";
        String hashedPassword = "$2a$10$hashedPassword";
        User newUser = new User(email, hashedPassword);
        
        when(userRepository.existsByEmail(email)).thenReturn(false);
        when(passwordEncoder.encode(password)).thenReturn(hashedPassword);
        when(userRepository.save(any(User.class))).thenReturn(newUser);
        
        // ACT
        User result = userService.registerUser(email, password);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(email, result.getEmail());
        verify(userRepository, times(1)).save(any(User.class));
        verify(kafkaTemplate, times(1)).send(eq("user-events"), any());
    }
    
    @Test
    @DisplayName("User registration with existing email should throw exception")
    void testRegisterUser_whenEmailExists_expectsUserAlreadyExistsException() {
        // ARRANGE
        String email = "existing@example.com";
        when(userRepository.existsByEmail(email)).thenReturn(true);
        
        // ACT & ASSERT
        assertThrows(UserAlreadyExistsException.class, () -> {
            userService.registerUser(email, "password");
        });
        verify(userRepository, never()).save(any());
    }
    
    @ParameterizedTest
    @ValueSource(strings = {"invalid", "no@domain", "", "   "})
    @DisplayName("User registration with invalid email formats should throw exception")
    void testRegisterUser_whenInvalidEmail_expectsValidationException(String invalidEmail) {
        // ACT & ASSERT
        assertThrows(InvalidEmailException.class, () -> {
            userService.registerUser(invalidEmail, "password");
        });
    }
    
    @ParameterizedTest
    @ValueSource(strings = {"short", "123", ""})
    @DisplayName("User registration with weak password should throw exception")
    void testRegisterUser_whenWeakPassword_expectsPasswordValidationException(String weakPassword) {
        // ACT & ASSERT
        assertThrows(WeakPasswordException.class, () -> {
            userService.registerUser("user@example.com", weakPassword);
        });
    }
    
    @Test
    @DisplayName("JWT token generation should return valid token")
    void testGenerateJwtToken_whenValidUser_expectsValidToken() {
        // ARRANGE
        User user = new User("user@example.com", "hashedPassword");
        user.setId(1L);
        String expectedToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...";
        
        when(jwtTokenProvider.generateToken(user)).thenReturn(expectedToken);
        
        // ACT
        String token = userService.generateJwtToken(user);
        
        // ASSERT
        assertNotNull(token);
        assertEquals(expectedToken, token);
        verify(jwtTokenProvider, times(1)).generateToken(user);
    }
    
    @Test
    @DisplayName("JWT token validation should return true for valid token")
    void testValidateJwtToken_whenValidToken_expectsTrue() {
        // ARRANGE
        String validToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...";
        when(jwtTokenProvider.validateToken(validToken)).thenReturn(true);
        
        // ACT
        boolean isValid = userService.validateJwtToken(validToken);
        
        // ASSERT
        assertTrue(isValid);
        verify(jwtTokenProvider, times(1)).validateToken(validToken);
    }
    
    @Test
    @DisplayName("JWT token validation should return false for invalid token")
    void testValidateJwtToken_whenInvalidToken_expectsFalse() {
        // ARRANGE
        String invalidToken = "invalid.token.here";
        when(jwtTokenProvider.validateToken(invalidToken)).thenReturn(false);
        
        // ACT
        boolean isValid = userService.validateJwtToken(invalidToken);
        
        // ASSERT
        assertFalse(isValid);
    }
    
    @Test
    @DisplayName("User profile retrieval should return user details")
    void testGetUserProfile_whenUserExists_expectsUserProfile() {
        // ARRANGE
        Long userId = 1L;
        User user = new User("user@example.com", "hashedPassword");
        user.setId(userId);
        
        when(userRepository.findById(userId)).thenReturn(Optional.of(user));
        
        // ACT
        User result = userService.getUserProfile(userId);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(userId, result.getId());
        assertEquals("user@example.com", result.getEmail());
    }
    
    @Test
    @DisplayName("User profile retrieval for non-existent user should throw exception")
    void testGetUserProfile_whenUserNotFound_expectsUserNotFoundException() {
        // ARRANGE
        Long userId = 999L;
        when(userRepository.findById(userId)).thenReturn(Optional.empty());
        
        // ACT & ASSERT
        assertThrows(UserNotFoundException.class, () -> {
            userService.getUserProfile(userId);
        });
    }
    
    @Test
    @DisplayName("Password update should hash new password and save")
    void testUpdatePassword_whenValidPassword_expectsPasswordUpdated() {
        // ARRANGE
        Long userId = 1L;
        String newPassword = "NewSecurePassword123!";
        String hashedPassword = "$2a$10$newHashedPassword";
        User user = new User("user@example.com", "oldHash");
        user.setId(userId);
        
        when(userRepository.findById(userId)).thenReturn(Optional.of(user));
        when(passwordEncoder.encode(newPassword)).thenReturn(hashedPassword);
        when(userRepository.save(any(User.class))).thenReturn(user);
        
        // ACT
        userService.updatePassword(userId, newPassword);
        
        // ASSERT
        verify(passwordEncoder, times(1)).encode(newPassword);
        verify(userRepository, times(1)).save(any(User.class));
    }
}

// ============================================================================
// PAYMENT SERVICE TESTS
// ============================================================================

@ExtendWith(MockitoExtension.class)
@DisplayName("Payment Service Tests")
class PaymentServiceTest {
    
    private PaymentService paymentService;
    
    @Mock
    private StripeClient stripeClient;
    
    @Mock
    private PaymentRepository paymentRepository;
    
    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @BeforeEach
    void setUp() {
        paymentService = new PaymentService(stripeClient, paymentRepository, kafkaTemplate);
    }
    
    @Test
    @DisplayName("Payment processing with valid card should succeed")
    void testProcessPayment_whenValidCard_expectsPaymentSuccess() {
        // ARRANGE
        Long userId = 1L;
        BigDecimal amount = new BigDecimal("99.99");
        String cardToken = "tok_visa";
        String chargeId = "ch_1234567890";
        
        StripeChargeResponse chargeResponse = new StripeChargeResponse(chargeId, true, "succeeded");
        when(stripeClient.charge(amount, cardToken)).thenReturn(chargeResponse);
        when(paymentRepository.save(any(Payment.class))).thenReturn(new Payment());
        
        // ACT
        PaymentResult result = paymentService.processPayment(userId, amount, cardToken);
        
        // ASSERT
        assertTrue(result.isSuccess());
        assertEquals(chargeId, result.getChargeId());
        verify(stripeClient, times(1)).charge(amount, cardToken);
        verify(paymentRepository, times(1)).save(any(Payment.class));
        verify(kafkaTemplate, times(1)).send(eq("payment-events"), any());
    }
    
    @Test
    @DisplayName("Payment processing with invalid card should fail")
    void testProcessPayment_whenInvalidCard_expectsPaymentFailure() {
        // ARRANGE
        Long userId = 1L;
        BigDecimal amount = new BigDecimal("99.99");
        String invalidCard = "tok_chargeDeclined";
        
        when(stripeClient.charge(amount, invalidCard))
            .thenThrow(new StripeException("Your card was declined"));
        
        // ACT & ASSERT
        assertThrows(PaymentProcessingException.class, () -> {
            paymentService.processPayment(userId, amount, invalidCard);
        });
    }
    
    @ParameterizedTest
    @CsvSource({
        "0.00, Invalid amount",
        "-10.00, Negative amount",
        "999999.99, Exceeds limit"
    })
    @DisplayName("Payment processing with invalid amounts should throw exception")
    void testProcessPayment_whenInvalidAmount_expectsValidationException(String amount, String reason) {
        // ACT & ASSERT
        assertThrows(InvalidPaymentAmountException.class, () -> {
            paymentService.processPayment(1L, new BigDecimal(amount), "tok_visa");
        });
    }
    
    @Test
    @DisplayName("Payment refund should process successfully")
    void testRefundPayment_whenValidChargeId_expectsRefundSuccess() {
        // ARRANGE
        String chargeId = "ch_1234567890";
        Payment payment = new Payment(chargeId, new BigDecimal("99.99"), "succeeded");
        
        when(paymentRepository.findByChargeId(chargeId)).thenReturn(Optional.of(payment));
        when(stripeClient.refund(chargeId)).thenReturn(true);
        when(paymentRepository.save(any(Payment.class))).thenReturn(payment);
        
        // ACT
        boolean result = paymentService.refundPayment(chargeId);
        
        // ASSERT
        assertTrue(result);
        verify(stripeClient, times(1)).refund(chargeId);
        verify(paymentRepository, times(1)).save(any(Payment.class));
    }
    
    @Test
    @DisplayName("Payment refund for non-existent charge should throw exception")
    void testRefundPayment_whenChargeNotFound_expectsPaymentNotFoundException() {
        // ARRANGE
        String chargeId = "ch_nonexistent";
        when(paymentRepository.findByChargeId(chargeId)).thenReturn(Optional.empty());
        
        // ACT & ASSERT
        assertThrows(PaymentNotFoundException.class, () -> {
            paymentService.refundPayment(chargeId);
        });
    }
    
    @Test
    @DisplayName("Payment status retrieval should return current status")
    void testGetPaymentStatus_whenPaymentExists_expectsStatus() {
        // ARRANGE
        String chargeId = "ch_1234567890";
        Payment payment = new Payment(chargeId, new BigDecimal("99.99"), "succeeded");
        
        when(paymentRepository.findByChargeId(chargeId)).thenReturn(Optional.of(payment));
        
        // ACT
        String status = paymentService.getPaymentStatus(chargeId);
        
        // ASSERT
        assertEquals("succeeded", status);
    }
    
    @Test
    @DisplayName("Partial payment processing should succeed")
    void testProcessPartialPayment_whenValidAmount_expectsSuccess() {
        // ARRANGE
        Long userId = 1L;
        BigDecimal fullAmount = new BigDecimal("100.00");
        BigDecimal partialAmount = new BigDecimal("50.00");
        String cardToken = "tok_visa";
        
        StripeChargeResponse chargeResponse = new StripeChargeResponse("ch_partial", true, "succeeded");
        when(stripeClient.charge(partialAmount, cardToken)).thenReturn(chargeResponse);
        when(paymentRepository.save(any(Payment.class))).thenReturn(new Payment());
        
        // ACT
        PaymentResult result = paymentService.processPartialPayment(userId, fullAmount, partialAmount, cardToken);
        
        // ASSERT
        assertTrue(result.isSuccess());
        verify(stripeClient, times(1)).charge(partialAmount, cardToken);
    }
}

// ============================================================================
// SUBSCRIPTION SERVICE TESTS
// ============================================================================

@ExtendWith(MockitoExtension.class)
@DisplayName("Subscription Service Tests")
class SubscriptionServiceTest {
    
    private SubscriptionService subscriptionService;
    
    @Mock
    private SubscriptionRepository subscriptionRepository;
    
    @Mock
    private PaymentServiceClient paymentServiceClient;
    
    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @Mock
    private QuartzScheduler quartzScheduler;
    
    @BeforeEach
    void setUp() {
        subscriptionService = new SubscriptionService(
            subscriptionRepository, 
            paymentServiceClient, 
            kafkaTemplate,
            quartzScheduler
        );
    }
    
    @Test
    @DisplayName("Subscription creation should save and publish event")
    void testCreateSubscription_whenValidData_expectsSubscriptionCreated() {
        // ARRANGE
        Long userId = 1L;
        String planId = "plan_premium";
        LocalDateTime now = LocalDateTime.now();
        Subscription subscription = new Subscription(userId, planId, now, now.plusMonths(1));
        
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(subscription);
        
        // ACT
        Subscription result = subscriptionService.createSubscription(userId, planId);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(userId, result.getUserId());
        assertEquals(planId, result.getPlanId());
        assertEquals("active", result.getStatus());
        verify(subscriptionRepository, times(1)).save(any(Subscription.class));
        verify(kafkaTemplate, times(1)).send(eq("subscription-events"), any());
    }
    
    @Test
    @DisplayName("Subscription cancellation should update status")
    void testCancelSubscription_whenValidSubscription_expectsCancelled() {
        // ARRANGE
        Long subscriptionId = 1L;
        Subscription subscription = new Subscription(1L, "plan_premium", LocalDateTime.now(), LocalDateTime.now().plusMonths(1));
        subscription.setId(subscriptionId);
        
        when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(subscription));
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(subscription);
        
        // ACT
        subscriptionService.cancelSubscription(subscriptionId);
        
        // ASSERT
        verify(subscriptionRepository, times(1)).save(any(Subscription.class));
        verify(kafkaTemplate, times(1)).send(eq("subscription-events"), any());
    }
    
    @Test
    @DisplayName("Subscription cancellation for non-existent subscription should throw exception")
    void testCancelSubscription_whenNotFound_expectsSubscriptionNotFoundException() {
        // ARRANGE
        Long subscriptionId = 999L;
        when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.empty());
        
        // ACT & ASSERT
        assertThrows(SubscriptionNotFoundException.class, () -> {
            subscriptionService.cancelSubscription(subscriptionId);
        });
    }
    
    @Test
    @DisplayName("Subscription renewal job should process expiring subscriptions")
    void testRenewSubscriptions_whenJobRuns_expectsExpiredSubscriptionsRenewed() {
        // ARRANGE
        LocalDateTime now = LocalDateTime.now();
        Subscription expiredSub = new Subscription(1L, "plan_premium", now.minusMonths(1), now);
        expiredSub.setId(1L);
        expiredSub.setStatus("active");
        
        List<Subscription> expiringSubscriptions = Arrays.asList(expiredSub);
        
        when(subscriptionRepository.findExpiringSubscriptions(any())).thenReturn(expiringSubscriptions);
        when(paymentServiceClient.processRenewal(1L, "plan_premium")).thenReturn(true);
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(expiredSub);
        
        // ACT
        subscriptionService.renewSubscriptions();
        
        // ASSERT
        verify(subscriptionRepository, times(1)).findExpiringSubscriptions(any());
        verify(paymentServiceClient, times(1)).processRenewal(1L, "plan_premium");
        verify(subscriptionRepository, times(1)).save(any(Subscription.class));
    }
    
    @Test
    @DisplayName("Subscription renewal failure should mark subscription as failed")
    void testRenewSubscriptions_whenPaymentFails_expectsSubscriptionMarkedFailed() {
        // ARRANGE
        LocalDateTime now = LocalDateTime.now();
        Subscription expiredSub = new Subscription(1L, "plan_premium", now.minusMonths(1), now);
        expiredSub.setId(1L);
        
        List<Subscription> expiringSubscriptions = Arrays.asList(expiredSub);
        
        when(subscriptionRepository.findExpiringSubscriptions(any())).thenReturn(expiringSubscriptions);
        when(paymentServiceClient.processRenewal(1L, "plan_premium")).thenReturn(false);
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(expiredSub);
        
        // ACT
        subscriptionService.renewSubscriptions();
        
        // ASSERT
        verify(subscriptionRepository, times(1)).save(any(Subscription.class));
        verify(kafkaTemplate, times(1)).send(eq("subscription-events"), any());
    }
    
    @Test
    @DisplayName("Get subscription status should return current status")
    void testGetSubscriptionStatus_whenSubscriptionExists_expectsStatus() {
        // ARRANGE
        Long subscriptionId = 1L;
        Subscription subscription = new Subscription(1L, "plan_premium", LocalDateTime.now(), LocalDateTime.now().plusMonths(1));
        subscription.setId(subscriptionId);
        subscription.setStatus("active");
        
        when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(subscription));
        
        // ACT
        String status = subscriptionService.getSubscriptionStatus(subscriptionId);
        
        // ASSERT
        assertEquals("active", status);
    }
    
    @Test
    @DisplayName("Subscription upgrade should update plan and process payment")
    void testUpgradeSubscription_whenValidPlan_expectsUpgraded() {
        // ARRANGE
        Long subscriptionId = 1L;
        String newPlan = "plan_premium_plus";
        Subscription subscription = new Subscription(1L, "plan_premium", LocalDateTime.now(), LocalDateTime.now().plusMonths(1));
        subscription.setId(subscriptionId);
        
        when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(subscription));
        when(paymentServiceClient.processUpgrade(1L, "plan_premium", newPlan)).thenReturn(true);
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(subscription);
        
        // ACT
        subscriptionService.upgradeSubscription(subscriptionId, newPlan);
        
        // ASSERT
        verify(paymentServiceClient, times(1)).processUpgrade(1L, "plan_premium", newPlan);
        verify(subscriptionRepository, times(1)).save(any(Subscription.class));
    }
}

// ============================================================================
// CONTENT SERVICE TESTS
// ============================================================================

@ExtendWith(MockitoExtension.class)
@DisplayName("Content Service Tests")
class ContentServiceTest {
    
    private ContentService contentService;
    
    @Mock
    private ContentRepository contentRepository;
    
    @Mock
    private ContentMetadataRepository metadataRepository;
    
    @BeforeEach
    void setUp() {
        contentService = new ContentService(contentRepository, metadataRepository);
    }
    
    @Test
    @DisplayName("Content creation should save metadata")
    void testCreateContent_whenValidData_expectsContentCreated() {
        // ARRANGE
        String title = "Test Video";
        String description = "A test video content";
        Long ownerId = 1L;
        Content content = new Content(title, description, ownerId);
        
        when(contentRepository.save(any(Content.class))).thenReturn(content);
        
        // ACT
        Content result = contentService.createContent(title, description, ownerId);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(title, result.getTitle());
        assertEquals(description, result.getDescription());
        assertEquals(ownerId, result.getOwnerId());
        verify(contentRepository, times(1)).save(any(Content.class));
    }
    
    @Test
    @DisplayName("Content retrieval should return content details")
    void testGetContent_whenContentExists_expectsContent() {
        // ARRANGE
        Long contentId = 1L;
        Content content = new Content("Test Video", "Description", 1L);
        content.setId(contentId);
        
        when(contentRepository.findById(contentId)).thenReturn(Optional.of(content));
        
        // ACT
        Content result = contentService.getContent(contentId);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(contentId, result.getId());
    }
    
    @Test
    @DisplayName("Content retrieval for non-existent content should throw exception")
    void testGetContent_whenNotFound_expectsContentNotFoundException() {
        // ARRANGE
        Long contentId = 999L;
        when(contentRepository.findById(contentId)).thenReturn(Optional.empty());
        
        // ACT & ASSERT
        assertThrows(ContentNotFoundException.class, () -> {
            contentService.getContent(contentId);
        });
    }
    
    @Test
    @DisplayName("Content deletion should remove content")
    void testDeleteContent_whenContentExists_expectsDeleted() {
        // ARRANGE
        Long contentId = 1L;
        Content content = new Content("Test Video", "Description", 1L);
        content.setId(contentId);
        
        when(contentRepository.findById(contentId)).thenReturn(Optional.of(content));
        
        // ACT
        contentService.deleteContent(contentId);
        
        // ASSERT
        verify(contentRepository, times(1)).deleteById(contentId);
    }
    
    @Test
    @DisplayName("Content search should return matching results")
    void testSearchContent_whenQueryProvided_expectsResults() {
        // ARRANGE
        String query = "action";
        List<Content> results = Arrays.asList(
            new Content("Action Movie", "Description", 1L),
            new Content("Action Series", "Description", 1L)
        );
        
        when(contentRepository.searchByTitle(query)).thenReturn(results);
        
        // ACT
        List<Content> searchResults = contentService.searchContent(query);
        
        // ASSERT
        assertEquals(2, searchResults.size());
        verify(contentRepository, times(1)).searchByTitle(query);
    }
}

// ============================================================================
// MEDIA SERVICE TESTS
// ============================================================================

@ExtendWith(MockitoExtension.class)
@DisplayName("Media Service Tests")
class MediaServiceTest {
    
    private MediaService mediaService;
    
    @Mock
    private MinioClient minioClient;
    
    @Mock
    private MediaRepository mediaRepository;
    
    @Mock
    private RedisTemplate<String, Object> redisTemplate;
    
    @Mock
    private FFmpegProcessor ffmpegProcessor;
    
    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @Mock
    private SubscriptionServiceClient subscriptionServiceClient;
    
    @BeforeEach
    void setUp() {
        mediaService = new MediaService(
            minioClient, 
            mediaRepository, 
            redisTemplate, 
            ffmpegProcessor,
            kafkaTemplate,
            subscriptionServiceClient
        );
    }
    
    @Test
    @DisplayName("Video upload should store in MinIO and create media record")
    void testUploadVideo_whenValidFile_expectsVideoStored() {
        // ARRANGE
        Long contentId = 1L;
        String fileName = "video.mp4";
        byte[] fileContent = "video content".getBytes();
        String minioPath = "videos/" + contentId + "/" + fileName;
        
        when(minioClient.uploadFile(minioPath, fileContent)).thenReturn(true);
        when(mediaRepository.save(any(Media.class))).thenReturn(new Media());
        
        // ACT
        mediaService.uploadVideo(contentId, fileName, fileContent);
        
        // ASSERT
        verify(minioClient, times(1)).uploadFile(minioPath, fileContent);
        verify(mediaRepository, times(1)).save(any(Media.class));
        verify(kafkaTemplate, times(1)).send(eq("media-events"), any());
    }
    
    @Test
    @DisplayName("Video transcoding should process multiple resolutions")
    void testTranscodeVideo_whenVideoUploaded_expectsMultipleResolutions() {
        // ARRANGE
        Long mediaId = 1L;
        String sourceFile = "videos/1/video.mp4";
        List<String> resolutions = Arrays.asList("720p", "1080p", "480p");
        
        when(ffmpegProcessor.transcode(sourceFile, "720p")).thenReturn("videos/1/720p.mp4");
        when(ffmpegProcessor.transcode(sourceFile, "1080p")).thenReturn("videos/1/1080p.mp4");
        when(ffmpegProcessor.transcode(sourceFile, "480p")).thenReturn("videos/1/480p.mp4");
        when(mediaRepository.save(any(Media.class))).thenReturn(new Media());
        
        // ACT
        mediaService.transcodeVideo(mediaId, sourceFile, resolutions);
        
        // ASSERT
        verify(ffmpegProcessor, times(3)).transcode(anyString(), anyString());
        verify(mediaRepository, times(1)).save(any(Media.class));
    }
    
    @Test
    @DisplayName("Video streaming with valid subscription should succeed")
    void testStreamVideo_whenSubscriptionActive_expectsStreamingAllowed() {
        // ARRANGE
        Long contentId = 1L;
        Long userId = 1L;
        String resolution = "720p";
        String videoPath = "videos/1/720p.mp4";
        
        when(subscriptionServiceClient.isSubscriptionActive(userId)).thenReturn(true);
        when(mediaRepository.findByContentIdAndResolution(contentId, resolution))
            .thenReturn(Optional.of(new Media(contentId, resolution, videoPath)));
        when(minioClient.getFileUrl(videoPath)).thenReturn("https://minio.example.com/" + videoPath);
        
        // ACT
        String streamUrl = mediaService.getStreamUrl(contentId, userId, resolution);
        
        // ASSERT
        assertNotNull(streamUrl);
        assertTrue(streamUrl.contains("minio.example.com"));
        verify(subscriptionServiceClient, times(1)).isSubscriptionActive(userId);
    }
    
    @Test
    @DisplayName("Video streaming without subscription should throw exception")
    void testStreamVideo_whenNoSubscription_expectsAccessDenied() {
        // ARRANGE
        Long contentId = 1L;
        Long userId = 1L;
        
        when(subscriptionServiceClient.isSubscriptionActive(userId)).thenReturn(false);
        
        // ACT & ASSERT
        assertThrows(SubscriptionRequiredException.class, () -> {
            mediaService.getStreamUrl(contentId, userId, "720p");
        });
    }
    
    @Test
    @DisplayName("Media metadata caching should improve performance")
    void testGetMediaMetadata_whenCached_expectsFromCache() {
        // ARRANGE
        Long mediaId = 1L;
        String cacheKey = "media:" + mediaId;
        Media cachedMedia = new Media(1L, "720p", "path/to/video.mp4");
        
        when(redisTemplate.opsForValue().get(cacheKey)).thenReturn(cachedMedia);
        
        // ACT
        Media result = mediaService.getMediaMetadata(mediaId);
        
        // ASSERT
        assertNotNull(result);
        assertEquals("720p", result.getResolution());
        verify(redisTemplate, times(1)).opsForValue().get(cacheKey);
    }
    
    @Test
    @DisplayName("Video deletion should remove from MinIO and database")
    void testDeleteVideo_whenVideoExists_expectsDeleted() {
        // ARRANGE
        Long mediaId = 1L;
        Media media = new Media(1L, "720p", "videos/1/720p.mp4");
        media.setId(mediaId);
        
        when(mediaRepository.findById(mediaId)).thenReturn(Optional.of(media));
        when(minioClient.deleteFile("videos/1/720p.mp4")).thenReturn(true);
        
        // ACT
        mediaService.deleteVideo(mediaId);
        
        // ASSERT
        verify(minioClient, times(1)).deleteFile("videos/1/720p.mp4");
        verify(mediaRepository, times(1)).deleteById(mediaId);
    }
}

// ============================================================================
// COMMENTS SERVICE TESTS
// ============================================================================

@ExtendWith(MockitoExtension.class)
@DisplayName("Comments Service Tests")
class CommentsServiceTest {
    
    private CommentsService commentsService;
    
    @Mock
    private CommentRepository commentRepository;
    
    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @BeforeEach
    void setUp() {
        commentsService = new CommentsService(commentRepository, kafkaTemplate);
    }
    
    @Test
    @DisplayName("Comment creation should save and publish event")
    void testCreateComment_whenValidData_expectsCommentCreated() {
        // ARRANGE
        Long contentId = 1L;
        Long userId = 1L;
        String text = "Great video!";
        Comment comment = new Comment(contentId, userId, text);
        
        when(commentRepository.save(any(Comment.class))).thenReturn(comment);
        
        // ACT
        Comment result = commentsService.createComment(contentId, userId, text);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(text, result.getText());
        assertEquals(userId, result.getUserId());
        verify(commentRepository, times(1)).save(any(Comment.class));
        verify(kafkaTemplate, times(1)).send(eq("comment-events"), any());
    }
    
    @Test
    @DisplayName("Comment creation with empty text should throw exception")
    void testCreateComment_whenEmptyText_expectsValidationException() {
        // ACT & ASSERT
        assertThrows(InvalidCommentException.class, () -> {
            commentsService.createComment(1L, 1L, "");
        });
    }
    
    @Test
    @DisplayName("Comment retrieval should return comment details")
    void testGetComment_whenCommentExists_expectsComment() {
        // ARRANGE
        Long commentId = 1L;
        Comment comment = new Comment(1L, 1L, "Great video!");
        comment.setId(commentId);
        
        when(commentRepository.findById(commentId)).thenReturn(Optional.of(comment));
        
        // ACT
        Comment result = commentsService.getComment(commentId);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(commentId, result.getId());
    }
    
    @Test
    @DisplayName("Get comments for content should return all comments")
    void testGetContentComments_whenCommentsExist_expectsCommentsList() {
        // ARRANGE
        Long contentId = 1L;
        List<Comment> comments = Arrays.asList(
            new Comment(contentId, 1L, "Great!"),
            new Comment(contentId, 2L, "Awesome!")
        );
        
        when(commentRepository.findByContentId(contentId)).thenReturn(comments);
        
        // ACT
        List<Comment> result = commentsService.getContentComments(contentId);
        
        // ASSERT
        assertEquals(2, result.size());
        verify(commentRepository, times(1)).findByContentId(contentId);
    }
    
    @Test
    @DisplayName("Comment deletion should remove comment")
    void testDeleteComment_whenCommentExists_expectsDeleted() {
        // ARRANGE
        Long commentId = 1L;
        Comment comment = new Comment(1L, 1L, "Great video!");
        comment.setId(commentId);
        
        when(commentRepository.findById(commentId)).thenReturn(Optional.of(comment));
        
        // ACT
        commentsService.deleteComment(commentId);
        
        // ASSERT
        verify(commentRepository, times(1)).deleteById(commentId);
    }
    
    @Test
    @DisplayName("Comment update should modify text")
    void testUpdateComment_whenValidText_expectsUpdated() {
        // ARRANGE
        Long commentId = 1L;
        String newText = "Updated comment";
        Comment comment = new Comment(1L, 1L, "Original text");
        comment.setId(commentId);
        
        when(commentRepository.findById(commentId)).thenReturn(Optional.of(comment));
        when(commentRepository.save(any(Comment.class))).thenReturn(comment);
        
        // ACT
        commentsService.updateComment(commentId, newText);
        
        // ASSERT
        verify(commentRepository, times(1)).save(any(Comment.class));
    }
}

// ============================================================================
// MODERATION SERVICE TESTS
// ============================================================================

@ExtendWith(MockitoExtension.class)
@DisplayName("Moderation Service Tests")
class ModerationServiceTest {
    
    private ModerationService moderationService;
    
    @Mock
    private ModerationRepository moderationRepository;
    
    @Mock
    private MongoTemplate mongoTemplate;
    
    @Mock
    private ContentAnalyzer contentAnalyzer;
    
    @BeforeEach
    void setUp() {
        moderationService = new ModerationService(
            moderationRepository, 
            mongoTemplate, 
            contentAnalyzer
        );
    }
    
    @Test
    @DisplayName("Content moderation should flag inappropriate content")
    void testModerateContent_whenInappropriate_expectsFlagged() {
        // ARRANGE
        Long commentId = 1L;
        String content = "Inappropriate content here";
        ModerationResult analysisResult = new ModerationResult(true, 0.95, "spam");
        
        when(contentAnalyzer.analyze(content)).thenReturn(analysisResult);
        when(moderationRepository.save(any(ModerationRecord.class))).thenReturn(new ModerationRecord());
        
        // ACT
        ModerationRecord result = moderationService.moderateContent(commentId, content);
        
        // ASSERT
        assertNotNull(result);
        assertTrue(result.isFlagged());
        verify(contentAnalyzer, times(1)).analyze(content);
        verify(moderationRepository, times(1)).save(any(ModerationRecord.class));
    }
    
    @Test
    @DisplayName("Content moderation should pass appropriate content")
    void testModerateContent_whenAppropriate_expectsNotFlagged() {
        // ARRANGE
        Long commentId = 1L;
        String content = "This is a great video!";
        ModerationResult analysisResult = new ModerationResult(false, 0.05, "clean");
        
        when(contentAnalyzer.analyze(content)).thenReturn(analysisResult);
        when(moderationRepository.save(any(ModerationRecord.class))).thenReturn(new ModerationRecord());
        
        // ACT
        ModerationRecord result = moderationService.moderateContent(commentId, content);
        
        // ASSERT
        assertNotNull(result);
        assertFalse(result.isFlagged());
    }
    
    @Test
    @DisplayName("Get flagged content should return all flagged items")
    void testGetFlaggedContent_whenFlaggedItemsExist_expectsList() {
        // ARRANGE
        List<ModerationRecord> flaggedItems = Arrays.asList(
            new ModerationRecord(1L, "spam", true),
            new ModerationRecord(2L, "hate_speech", true)
        );
        
        when(moderationRepository.findByFlaggedTrue()).thenReturn(flaggedItems);
        
        // ACT
        List<ModerationRecord> result = moderationService.getFlaggedContent();
        
        // ASSERT
        assertEquals(2, result.size());
        verify(moderationRepository, times(1)).findByFlaggedTrue();
    }
    
    @Test
    @DisplayName("Moderation review should update status")
    void testReviewModeration_whenApproved_expectsStatusUpdated() {
        // ARRANGE
        Long recordId = 1L;
        ModerationRecord record = new ModerationRecord(1L, "spam", true);
        record.setId(recordId);
        
        when(moderationRepository.findById(recordId)).thenReturn(Optional.of(record));
        when(moderationRepository.save(any(ModerationRecord.class))).thenReturn(record);
        
        // ACT
        moderationService.reviewModeration(recordId, "approved");
        
        // ASSERT
        verify(moderationRepository, times(1)).save(any(ModerationRecord.class));
    }
}

// ============================================================================
// CUSTOMER SERVICE TESTS
// ============================================================================

@ExtendWith(MockitoExtension.class)
@DisplayName("Customer Service Tests")
class CustomerServiceTest {
    
    private CustomerService customerService;
    
    @Mock
    private CustomerRepository customerRepository;
    
    @Mock
    private RedisTemplate<String, Object> redisTemplate;
    
    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;
    
    @BeforeEach
    void setUp() {
        customerService = new CustomerService(customerRepository, redisTemplate, kafkaTemplate);
    }
    
    @Test
    @DisplayName("Customer profile creation should save and cache")
    void testCreateCustomerProfile_whenValidData_expectsProfileCreated() {
        // ARRANGE
        Long userId = 1L;
        String email = "customer@example.com";
        String name = "John Doe";
        Customer customer = new Customer(userId, email, name);
        
        when(customerRepository.save(any(Customer.class))).thenReturn(customer);
        
        // ACT
        Customer result = customerService.createCustomerProfile(userId, email, name);
        
        // ASSERT
        assertNotNull(result);
        assertEquals(email, result.getEmail());
        assertEquals(name, result.getName());
        verify(customerRepository, times(1)).save(any(Customer.class));
        verify(redisTemplate, times(1)).opsForValue().set(anyString(), any());
    }
    
    @Test
    @DisplayName("Customer profile retrieval should return from cache if available")
    void testGetCustomerProfile_whenCached_expectsFromCache() {
        // ARRANGE
        Long userId = 1L;
        String cacheKey = "customer:" + userId;
        Customer cachedCustomer = new Customer(userId, "customer@example.com", "John Doe");
        
        when(redisTemplate.opsForValue().get(cacheKey)).thenReturn(cachedCustomer);
        
        // ACT
        Customer result = customerService.getCustomerProfile(userId);
        
        // ASSERT
        assertNotNull(result);
        assertEquals("John Doe", result.getName());
        verify(redisTemplate, times(1)).opsForValue().get(cacheKey);
    }
    
    @Test
    @DisplayName("Customer profile update should invalidate cache")
    void testUpdateCustomerProfile_whenUpdated_expectsCacheInvalidated() {
        // ARRANGE
        Long userId = 1L;
        String cacheKey = "customer:" + userId;
        Customer customer = new Customer(userId, "customer@example.com", "John Doe");
        customer.setId(userId);
        
        when(customerRepository.findById(userId)).thenReturn(Optional.of(customer));
        when(customerRepository.save(any(Customer.class))).thenReturn(customer);
        
        // ACT
        customerService.updateCustomerProfile(userId, "Jane Doe", "jane@example.com");
        
        // ASSERT
        verify(customerRepository, times(1)).save(any(Customer.class));
        verify(redisTemplate, times(1)).delete(cacheKey);
    }
    
    @Test
    @DisplayName("Customer preferences update should save preferences")
    void testUpdatePreferences_whenValidPreferences_expectsSaved() {
        // ARRANGE
        Long userId = 1L;
        Map<String, Object> preferences = new HashMap<>();
        preferences.put("language", "en");
        preferences.put("notifications", true);
        
        Customer customer = new Customer(userId, "customer@example.com", "John Doe");
        customer.setId(userId);
        
        when(customerRepository.findById(userId)).thenReturn(Optional.of(customer));
        when(customerRepository.save(any(Customer.class))).thenReturn(customer);
        
        // ACT
        customerService.updatePreferences(userId, preferences);
        
        // ASSERT
        verify(customerRepository, times(1)).save(any(Customer.class));
    }
    
    @Test
    @DisplayName("Customer deletion should remove profile and cache")
    void testDeleteCustomer_whenCustomerExists_expectsDeleted() {
        // ARRANGE
        Long userId = 1L;
        String cacheKey = "customer:" + userId;
        Customer customer = new Customer(userId, "customer@example.com", "John Doe");
        customer.setId(userId);
        
        when(customerRepository.findById(userId)).thenReturn(Optional.of(customer));
        
        // ACT
        customerService.deleteCustomer(userId);
        
        // ASSERT
        verify(customerRepository, times(1)).deleteById(userId);
        verify(redisTemplate, times(1)).delete(cacheKey);
    }
}
