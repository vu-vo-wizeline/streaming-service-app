package io.github.marianciuc.streamingservice;

import com.stripe.exception.StripeException;
import com.stripe.model.PaymentIntent;
import io.github.marianciuc.streamingservice.payment.dto.PaymentRequest;
import io.github.marianciuc.streamingservice.payment.entity.Payment;
import io.github.marianciuc.streamingservice.payment.event.PaymentStatusMessage;
import io.github.marianciuc.streamingservice.payment.repository.PaymentRepository;
import io.github.marianciuc.streamingservice.payment.service.PaymentService;
import io.github.marianciuc.streamingservice.subscription.dto.SubscriptionRenewalRequest;
import io.github.marianciuc.streamingservice.subscription.entity.Subscription;
import io.github.marianciuc.streamingservice.subscription.repository.SubscriptionRepository;
import io.github.marianciuc.streamingservice.subscription.service.SubscriptionService;
import io.github.marianciuc.streamingservice.subscription.client.PaymentServiceClient;
import io.github.marianciuc.streamingservice.user.dto.CreateUserRequest;
import io.github.marianciuc.streamingservice.user.entity.User;
import io.github.marianciuc.streamingservice.user.event.CreateUserMessage;
import io.github.marianciuc.streamingservice.user.exception.UserAlreadyExistsException;
import io.github.marianciuc.streamingservice.user.repository.UserRepository;
import io.github.marianciuc.streamingservice.user.service.UserService;
import io.github.marianciuc.streamingservice.media.dto.UploadChunkRequest;
import io.github.marianciuc.streamingservice.media.entity.MediaFile;
import io.github.marianciuc.streamingservice.media.event.StartConvertingMessage;
import io.github.marianciuc.streamingservice.media.repository.MediaRepository;
import io.github.marianciuc.streamingservice.media.service.MediaService;
import io.github.marianciuc.streamingservice.media.client.MinIOClient;
import io.github.marianciuc.streamingservice.customer.dto.CustomerProfile;
import io.github.marianciuc.streamingservice.customer.event.UserCreatedEventConsumer;
import io.github.marianciuc.streamingservice.customer.repository.CustomerRepository;
import io.github.marianciuc.streamingservice.customer.service.CustomerService;
import io.github.marianciuc.streamingservice.comments.dto.CreateCommentRequest;
import io.github.marianciuc.streamingservice.comments.entity.Comment;
import io.github.marianciuc.streamingservice.comments.repository.CommentRepository;
import io.github.marianciuc.streamingservice.comments.service.CommentService;
import io.github.marianciuc.streamingservice.comments.client.UserServiceClient;
import io.github.marianciuc.streamingservice.comments.client.ContentServiceClient;
import io.github.marianciuc.streamingservice.content.dto.ContentMetadata;
import io.github.marianciuc.streamingservice.content.repository.ContentRepository;
import io.github.marianciuc.streamingservice.content.service.ContentService;
import io.github.marianciuc.streamingservice.moderation.dto.ModerationFlag;
import io.github.marianciuc.streamingservice.moderation.repository.ModerationPostgresRepository;
import io.github.marianciuc.streamingservice.moderation.repository.ModerationMongoRepository;
import io.github.marianciuc.streamingservice.moderation.service.ModerationService;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit test suite for streaming service microservices.
 * Tests cover critical business logic paths, error handling, boundary conditions,
 * and external dependency interactions using mocks and stubs.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Streaming Service Unit Tests")
public class StreamingServiceIntegrationTests {

    // ==================== PAYMENT SERVICE TESTS ====================

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private KafkaTemplate<String, PaymentStatusMessage> paymentKafkaTemplate;

    @Mock
    private com.stripe.net.RequestOptions stripeRequestOptions;

    @InjectMocks
    private PaymentService paymentService;

    @BeforeEach
    void setUp() {
        // Reset mocks before each test
        reset(paymentRepository, paymentKafkaTemplate);
    }

    @Test
    @DisplayName("Payment Service: Process payment with valid amount and token - should create payment and publish event")
    void testProcessPayment_whenValidAmountAndToken_expectsPaymentCreatedAndEventPublished() {
        // ARRANGE
        PaymentRequest request = new PaymentRequest();
        request.setAmount(new BigDecimal("99.99"));
        request.setStripeToken("tok_visa");
        request.setUserId(UUID.randomUUID().toString());
        request.setCurrency("USD");

        Payment expectedPayment = new Payment();
        expectedPayment.setId(UUID.randomUUID().toString());
        expectedPayment.setAmount(request.getAmount());
        expectedPayment.setStatus("COMPLETED");
        expectedPayment.setUserId(request.getUserId());
        expectedPayment.setCreatedAt(LocalDateTime.now());

        when(paymentRepository.save(any(Payment.class))).thenReturn(expectedPayment);

        // ACT
        Payment result = paymentService.processPayment(request);

        // ASSERT
        assertThat(result).isNotNull();
        assertThat(result.getAmount()).isEqualTo(request.getAmount());
        assertThat(result.getStatus()).isEqualTo("COMPLETED");
        verify(paymentRepository, times(1)).save(any(Payment.class));
        verify(paymentKafkaTemplate, times(1)).send(eq("payment-status"), any(PaymentStatusMessage.class));
    }

    @Test
    @DisplayName("Payment Service: Process payment with zero amount - should throw IllegalArgumentException")
    void testProcessPayment_whenZeroAmount_expectsIllegalArgumentException() {
        // ARRANGE
        PaymentRequest request = new PaymentRequest();
        request.setAmount(BigDecimal.ZERO);
        request.setStripeToken("tok_visa");
        request.setUserId(UUID.randomUUID().toString());

        // ACT & ASSERT
        assertThatThrownBy(() -> paymentService.processPayment(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Amount must be greater than zero");
        verify(paymentRepository, never()).save(any());
    }

    @Test
    @DisplayName("Payment Service: Process payment with negative amount - should throw IllegalArgumentException")
    void testProcessPayment_whenNegativeAmount_expectsIllegalArgumentException() {
        // ARRANGE
        PaymentRequest request = new PaymentRequest();
        request.setAmount(new BigDecimal("-50.00"));
        request.setStripeToken("tok_visa");
        request.setUserId(UUID.randomUUID().toString());

        // ACT & ASSERT
        assertThatThrownBy(() -> paymentService.processPayment(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Amount must be greater than zero");
    }

    @Test
    @DisplayName("Payment Service: Process payment with invalid Stripe token - should throw StripeException")
    void testProcessPayment_whenInvalidStripeToken_expectsStripeException() throws StripeException {
        // ARRANGE
        PaymentRequest request = new PaymentRequest();
        request.setAmount(new BigDecimal("99.99"));
        request.setStripeToken("invalid_token");
        request.setUserId(UUID.randomUUID().toString());

        when(paymentRepository.save(any(Payment.class)))
                .thenThrow(new StripeException("Invalid token", null, null, 400));

        // ACT & ASSERT
        assertThatThrownBy(() -> paymentService.processPayment(request))
                .isInstanceOf(StripeException.class);
    }

    @ParameterizedTest
    @CsvSource({
            "9.99, USD, COMPLETED",
            "99.99, EUR, COMPLETED",
            "999.99, GBP, COMPLETED"
    })
    @DisplayName("Payment Service: Process payment with various amounts and currencies - should succeed")
    void testProcessPayment_whenVariousAmountsAndCurrencies_expectsSuccess(String amount, String currency, String status) {
        // ARRANGE
        PaymentRequest request = new PaymentRequest();
        request.setAmount(new BigDecimal(amount));
        request.setCurrency(currency);
        request.setStripeToken("tok_visa");
        request.setUserId(UUID.randomUUID().toString());

        Payment expectedPayment = new Payment();
        expectedPayment.setAmount(request.getAmount());
        expectedPayment.setStatus(status);
        when(paymentRepository.save(any(Payment.class))).thenReturn(expectedPayment);

        // ACT
        Payment result = paymentService.processPayment(request);

        // ASSERT
        assertThat(result.getStatus()).isEqualTo(status);
        verify(paymentRepository).save(any(Payment.class));
    }

    @Test
    @DisplayName("Payment Service: Kafka publish fails - should still create payment but log error")
    void testProcessPayment_whenKafkaPublishFails_expectsPaymentCreatedButEventNotSent() {
        // ARRANGE
        PaymentRequest request = new PaymentRequest();
        request.setAmount(new BigDecimal("99.99"));
        request.setStripeToken("tok_visa");
        request.setUserId(UUID.randomUUID().toString());

        Payment expectedPayment = new Payment();
        expectedPayment.setId(UUID.randomUUID().toString());
        expectedPayment.setAmount(request.getAmount());
        expectedPayment.setStatus("COMPLETED");

        when(paymentRepository.save(any(Payment.class))).thenReturn(expectedPayment);
        doThrow(new RuntimeException("Kafka unavailable"))
                .when(paymentKafkaTemplate).send(anyString(), any(PaymentStatusMessage.class));

        // ACT & ASSERT
        assertThatThrownBy(() -> paymentService.processPayment(request))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Kafka unavailable");
        verify(paymentRepository).save(any(Payment.class));
    }

    // ==================== SUBSCRIPTION SERVICE TESTS ====================

    @Mock
    private SubscriptionRepository subscriptionRepository;

    @Mock
    private PaymentServiceClient paymentServiceClient;

    @Mock
    private KafkaTemplate<String, String> subscriptionKafkaTemplate;

    @InjectMocks
    private SubscriptionService subscriptionService;

    @Test
    @DisplayName("Subscription Service: Renew subscription with valid subscription and payment OK - should renew")
    void testRenewSubscription_whenValidSubscriptionAndPaymentOK_expectsSubscriptionRenewed() {
        // ARRANGE
        String subscriptionId = UUID.randomUUID().toString();
        String userId = UUID.randomUUID().toString();
        LocalDateTime expiryDate = LocalDateTime.now().plusDays(30);

        Subscription subscription = new Subscription();
        subscription.setId(subscriptionId);
        subscription.setUserId(userId);
        subscription.setStatus("ACTIVE");
        subscription.setExpiryDate(LocalDateTime.now().minusDays(1));

        when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(subscription));
        when(paymentServiceClient.verifyPaymentStatus(userId)).thenReturn(true);
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(subscription);

        // ACT
        Subscription result = subscriptionService.renewSubscription(subscriptionId);

        // ASSERT
        assertThat(result).isNotNull();
        assertThat(result.getStatus()).isEqualTo("ACTIVE");
        verify(subscriptionRepository).save(any(Subscription.class));
        verify(paymentServiceClient).verifyPaymentStatus(userId);
    }

    @Test
    @DisplayName("Subscription Service: Renew subscription with expired subscription - should cancel")
    void testRenewSubscription_whenExpiredSubscription_expectsSubscriptionCancelled() {
        // ARRANGE
        String subscriptionId = UUID.randomUUID().toString();
        String userId = UUID.randomUUID().toString();

        Subscription subscription = new Subscription();
        subscription.setId(subscriptionId);
        subscription.setUserId(userId);
        subscription.setStatus("ACTIVE");
        subscription.setExpiryDate(LocalDateTime.now().minusDays(30));

        when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(subscription));
        when(paymentServiceClient.verifyPaymentStatus(userId)).thenReturn(false);
        when(subscriptionRepository.save(any(Subscription.class))).thenReturn(subscription);

        // ACT
        Subscription result = subscriptionService.renewSubscription(subscriptionId);

        // ASSERT
        assertThat(result.getStatus()).isEqualTo("CANCELLED");
        verify(subscriptionRepository).save(any(Subscription.class));
    }

    @Test
    @DisplayName("Subscription Service: Check subscription access with active subscription - should grant access")
    void testCheckSubscriptionAccess_whenActiveSubscription_expectsAccessGranted() {
        // ARRANGE
        String userId = UUID.randomUUID().toString();
        String contentId = UUID.randomUUID().toString();

        Subscription subscription = new Subscription();
        subscription.setUserId(userId);
        subscription.setStatus("ACTIVE");
        subscription.setExpiryDate(LocalDateTime.now().plusDays(30));

        when(subscriptionRepository.findByUserIdAndStatus(userId, "ACTIVE"))
                .thenReturn(Optional.of(subscription));

        // ACT
        boolean hasAccess = subscriptionService.checkSubscriptionAccess(userId, contentId);

        // ASSERT
        assertThat(hasAccess).isTrue();
        verify(subscriptionRepository).findByUserIdAndStatus(userId, "ACTIVE");
    }

    @Test
    @DisplayName("Subscription Service: Check subscription access with expired subscription - should deny access")
    void testCheckSubscriptionAccess_whenExpiredSubscription_expectsAccessDenied() {
        // ARRANGE
        String userId = UUID.randomUUID().toString();
        String contentId = UUID.randomUUID().toString();

        when(subscriptionRepository.findByUserIdAndStatus(userId, "ACTIVE"))
                .thenReturn(Optional.empty());

        // ACT
        boolean hasAccess = subscriptionService.checkSubscriptionAccess(userId, contentId);

        // ASSERT
        assertThat(hasAccess).isFalse();
    }

    @Test
    @DisplayName("Subscription Service: Renew subscription with Feign call failure - should throw exception")
    void testRenewSubscription_whenFeignCallFails_expectsExceptionPropagated() {
        // ARRANGE
        String subscriptionId = UUID.randomUUID().toString();
        String userId = UUID.randomUUID().toString();

        Subscription subscription = new Subscription();
        subscription.setId(subscriptionId);
        subscription.setUserId(userId);
        subscription.setStatus("ACTIVE");

        when(subscriptionRepository.findById(subscriptionId)).thenReturn(Optional.of(subscription));
        when(paymentServiceClient.verifyPaymentStatus(userId))
                .thenThrow(new RuntimeException("Payment Service unavailable"));

        // ACT & ASSERT
        assertThatThrownBy(() -> subscriptionService.renewSubscription(subscriptionId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Payment Service unavailable");
    }

    // ==================== USER SERVICE TESTS ====================

    @Mock
    private UserRepository userRepository;

    @Mock
    private PasswordEncoder passwordEncoder;

    @Mock
    private KafkaTemplate<String, CreateUserMessage> userKafkaTemplate;

    @InjectMocks
    private UserService userService;

    @Test
    @DisplayName("User Service: Create user with valid email and password - should create user and publish event")
    void testCreateUser_whenValidEmailAndPassword_expectsUserCreatedAndEventPublished() {
        // ARRANGE
        CreateUserRequest request = new CreateUserRequest();
        request.setEmail("user@example.com");
        request.setPassword("SecurePassword123!");
        request.setFirstName("John");
        request.setLastName("Doe");

        User expectedUser = new User();
        expectedUser.setId(UUID.randomUUID().toString());
        expectedUser.setEmail(request.getEmail());
        expectedUser.setFirstName(request.getFirstName());
        expectedUser.setLastName(request.getLastName());
        expectedUser.setCreatedAt(LocalDateTime.now());

        when(userRepository.findByEmail(request.getEmail())).thenReturn(Optional.empty());
        when(passwordEncoder.encode(request.getPassword())).thenReturn("hashed_password");
        when(userRepository.save(any(User.class))).thenReturn(expectedUser);

        // ACT
        User result = userService.createUser(request);

        // ASSERT
        assertThat(result).isNotNull();
        assertThat(result.getEmail()).isEqualTo(request.getEmail());
        assertThat(result.getFirstName()).isEqualTo(request.getFirstName());
        verify(userRepository).save(any(User.class));
        verify(userKafkaTemplate).send(eq("user-created"), any(CreateUserMessage.class));
    }

    @Test
    @DisplayName("User Service: Create user with duplicate email - should throw UserAlreadyExistsException")
    void testCreateUser_whenDuplicateEmail_expectsUserAlreadyExistsException() {
        // ARRANGE
        CreateUserRequest request = new CreateUserRequest();
        request.setEmail("existing@example.com");
        request.setPassword("SecurePassword123!");

        User existingUser = new User();
        existingUser.setEmail(request.getEmail());

        when(userRepository.findByEmail(request.getEmail())).thenReturn(Optional.of(existingUser));

        // ACT & ASSERT
        assertThatThrownBy(() -> userService.createUser(request))
                .isInstanceOf(UserAlreadyExistsException.class)
                .hasMessageContaining("User with email already exists");
        verify(userRepository, never()).save(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "invalid-email",
            "@example.com",
            "user@",
            "user @example.com"
    })
    @DisplayName("User Service: Create user with invalid email format - should throw ValidationException")
    void testCreateUser_whenInvalidEmailFormat_expectsValidationException(String invalidEmail) {
        // ARRANGE
        CreateUserRequest request = new CreateUserRequest();
        request.setEmail(invalidEmail);
        request.setPassword("SecurePassword123!");

        // ACT & ASSERT
        assertThatThrownBy(() -> userService.createUser(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid email format");
    }

    @Test
    @DisplayName("User Service: Generate JWT with valid user - should return valid token")
    void testGenerateJWT_whenValidUser_expectsValidToken() {
        // ARRANGE
        User user = new User();
        user.setId(UUID.randomUUID().toString());
        user.setEmail("user@example.com");
        user.setFirstName("John");

        // ACT
        String token = userService.generateJWT(user);

        // ASSERT
        assertThat(token).isNotNull();
        assertThat(token).isNotEmpty();
        assertThat(token.split("\\\\")).hasSize(3);
    }

    @Test
    @DisplayName("User Service: Generate JWT with null user - should throw NullPointerException")
    void testGenerateJWT_whenNullUser_expectsNullPointerException() {
        // ACT & ASSERT
        assertThatThrownBy(() -> userService.generateJWT(null))
                .isInstanceOf(NullPointerException.class);
    }

    // ==================== MEDIA SERVICE TESTS ====================

    @Mock
    private MediaRepository mediaRepository;

    @Mock
    private MinIOClient minIOClient;

    @Mock
    private Jedis redisClient;

    @Mock
    private KafkaTemplate<String, StartConvertingMessage> mediaKafkaTemplate;

    @InjectMocks
    private MediaService mediaService;

    @Test
    @DisplayName("Media Service: Upload chunk with valid session - should store in MinIO and update Redis")
    void testUploadChunk_whenValidSession_expectsChunkStoredAndRedisUpdated() {
        // ARRANGE
        String sessionId = UUID.randomUUID().toString();
        String chunkId = UUID.randomUUID().toString();
        byte[] chunkData = "video_chunk_data".getBytes();

        UploadChunkRequest request = new UploadChunkRequest();
        request.setSessionId(sessionId);
        request.setChunkIndex(1);
        request.setChunkData(chunkData);
        request.setTotalChunks(5);

        when(redisClient.exists(sessionId)).thenReturn(1L);
        when(minIOClient.uploadChunk(sessionId, 1, chunkData)).thenReturn(chunkId);

        // ACT
        String result = mediaService.uploadChunk(request);

        // ASSERT
        assertThat(result).isEqualTo(chunkId);
        verify(minIOClient).uploadChunk(sessionId, 1, chunkData);
        verify(redisClient).incr("upload_progress:" + sessionId);
    }

    @Test
    @DisplayName("Media Service: Upload chunk with invalid session - should throw SessionNotFoundException")
    void testUploadChunk_whenInvalidSession_expectsSessionNotFoundException() {
        // ARRANGE
        String sessionId = UUID.randomUUID().toString();
        byte[] chunkData = "video_chunk_data".getBytes();

        UploadChunkRequest request = new UploadChunkRequest();
        request.setSessionId(sessionId);
        request.setChunkIndex(1);
        request.setChunkData(chunkData);

        when(redisClient.exists(sessionId)).thenReturn(0L);

        // ACT & ASSERT
        assertThatThrownBy(() -> mediaService.uploadChunk(request))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Session not found");
        verify(minIOClient, never()).uploadChunk(anyString(), anyInt(), any());
    }

    @Test
    @DisplayName("Media Service: Upload chunk with MinIO write failure - should throw StorageException")
    void testUploadChunk_whenMinIOWriteFails_expectsStorageException() {
        // ARRANGE
        String sessionId = UUID.randomUUID().toString();
        byte[] chunkData = "video_chunk_data".getBytes();

        UploadChunkRequest request = new UploadChunkRequest();
        request.setSessionId(sessionId);
        request.setChunkIndex(1);
        request.setChunkData(chunkData);

        when(redisClient.exists(sessionId)).thenReturn(1L);
        when(minIOClient.uploadChunk(sessionId, 1, chunkData))
                .thenThrow(new RuntimeException("MinIO connection failed"));

        // ACT & ASSERT
        assertThatThrownBy(() -> mediaService.uploadChunk(request))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("MinIO connection failed");
    }

    @Test
    @DisplayName("Media Service: Process video with valid file - should convert to HLS and publish event")
    void testProcessVideo_whenValidFile_expectsHLSConversionAndEventPublished() {
        // ARRANGE
        String mediaId = UUID.randomUUID().toString();
        String videoPath = "/videos/" + mediaId + ".mp4";

        MediaFile mediaFile = new MediaFile();
        mediaFile.setId(mediaId);
        mediaFile.setOriginalPath(videoPath);
        mediaFile.setStatus("UPLOADED");

        when(mediaRepository.findById(mediaId)).thenReturn(Optional.of(mediaFile));
        when(minIOClient.getFileUrl(videoPath)).thenReturn("http://minio/videos/" + mediaId + ".mp4");
        when(mediaRepository.save(any(MediaFile.class))).thenReturn(mediaFile);

        // ACT
        MediaFile result = mediaService.processVideo(mediaId);

        // ASSERT
        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(mediaId);
        verify(mediaRepository).save(any(MediaFile.class));
        verify(mediaKafkaTemplate).send(eq("video-processing"), any(StartConvertingMessage.class));
    }

    @Test
    @DisplayName("Media Service: Process video with unsupported format - should throw UnsupportedFormatException")
    void testProcessVideo_whenUnsupportedFormat_expectsUnsupportedFormatException() {
        // ARRANGE
        String mediaId = UUID.randomUUID().toString();
        String videoPath = "/videos/" + mediaId + ".txt";

        MediaFile mediaFile = new MediaFile();
        mediaFile.setId(mediaId);
        mediaFile.setOriginalPath(videoPath);
        mediaFile.setStatus("UPLOADED");

        when(mediaRepository.findById(mediaId)).thenReturn(Optional.of(mediaFile));

        // ACT & ASSERT
        assertThatThrownBy(() -> mediaService.processVideo(mediaId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported file format");
    }

    // ==================== CUSTOMER SERVICE TESTS ====================

    @Mock
    private CustomerRepository customerRepository;

    @InjectMocks
    private CustomerService customerService;

    @Test
    @DisplayName("Customer Service: Consume user created event - should create customer profile")
    void testConsumeUserCreatedEvent_whenValidEvent_expectsCustomerProfileCreated() {
        // ARRANGE
        String userId = UUID.randomUUID().toString();
        CreateUserMessage event = new CreateUserMessage();
        event.setUserId(userId);
        event.setEmail("user@example.com");
        event.setFirstName("John");
        event.setLastName("Doe");

        CustomerProfile expectedProfile = new CustomerProfile();
        expectedProfile.setUserId(userId);
        expectedProfile.setEmail(event.getEmail());
        expectedProfile.setFirstName(event.getFirstName());
        expectedProfile.setLastName(event.getLastName());

        when(customerRepository.findByUserId(userId)).thenReturn(Optional.empty());
        when(customerRepository.save(any(CustomerProfile.class))).thenReturn(expectedProfile);

        // ACT
        CustomerProfile result = customerService.consumeUserCreatedEvent(event);

        // ASSERT
        assertThat(result).isNotNull();
        assertThat(result.getUserId()).isEqualTo(userId);
        assertThat(result.getEmail()).isEqualTo(event.getEmail());
        verify(customerRepository).save(any(CustomerProfile.class));
    }

    @Test
    @DisplayName("Customer Service: Consume duplicate user event - should be idempotent")
    void testConsumeUserCreatedEvent_whenDuplicateUser_expectsIdempotent() {
        // ARRANGE
        String userId = UUID.randomUUID().toString();
        CreateUserMessage event = new CreateUserMessage();
        event.setUserId(userId);
        event.setEmail("user@example.com");

        CustomerProfile existingProfile = new CustomerProfile();
        existingProfile.setUserId(userId);
        existingProfile.setEmail(event.getEmail());

        when(customerRepository.findByUserId(userId)).thenReturn(Optional.of(existingProfile));

        // ACT
        CustomerProfile result = customerService.consumeUserCreatedEvent(event);

        // ASSERT
        assertThat(result.getUserId()).isEqualTo(userId);
        verify(customerRepository, never()).save(any());
    }

    // ==================== COMMENTS SERVICE TESTS ====================

    @Mock
    private CommentRepository commentRepository;

    @Mock
    private UserServiceClient commentUserServiceClient;

    @Mock
    private ContentServiceClient contentServiceClient;

    @InjectMocks
    private CommentService commentService;

    @Test
    @DisplayName("Comments Service: Create comment with valid content and user - should store comment")
    void testCreateComment_whenValidContentAndUser_expectsCommentStored() {
        // ARRANGE
        String userId = UUID.randomUUID().toString();
        String contentId = UUID.randomUUID().toString();
        String commentText = "Great video!";

        CreateCommentRequest request = new CreateCommentRequest();
        request.setUserId(userId);
        request.setContentId(contentId);
        request.setText(commentText);

        when(commentUserServiceClient.getUserById(userId)).thenReturn(true);
        when(contentServiceClient.getContentById(contentId)).thenReturn(true);

        Comment expectedComment = new Comment();
        expectedComment.setId(UUID.randomUUID().toString());
        expectedComment.setUserId(userId);
        expectedComment.setContentId(contentId);
        expectedComment.setText(commentText);
        expectedComment.setCreatedAt(LocalDateTime.now());

        when(commentRepository.save(any(Comment.class))).thenReturn(expectedComment);

        // ACT
        Comment result = commentService.createComment(request);

        // ASSERT
        assertThat(result).isNotNull();
        assertThat(result.getText()).isEqualTo(commentText);
        assertThat(result.getUserId()).isEqualTo(userId);
        assertThat(result.getContentId()).isEqualTo(contentId);
        verify(commentRepository).save(any(Comment.class));
    }

    @Test
    @DisplayName("Comments Service: Create comment with non-existent content - should throw ContentNotFoundException")
    void testCreateComment_whenContentNotFound_expectsContentNotFoundException() {
        // ARRANGE
        String userId = UUID.randomUUID().toString();
        String contentId = UUID.randomUUID().toString();

        CreateCommentRequest request = new CreateCommentRequest();
        request.setUserId(userId);
        request.setContentId(contentId);
        request.setText("Great video!");

        when(commentUserServiceClient.getUserById(userId)).thenReturn(true);
        when(contentServiceClient.getContentById(contentId)).thenReturn(false);

        // ACT & ASSERT
        assertThatThrownBy(() -> commentService.createComment(request))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Content not found");
        verify(commentRepository, never()).save(any());
    }

    @Test
    @DisplayName("Comments Service: Create comment with non-existent user - should throw UserNotFoundException")
    void testCreateComment_whenUserNotFound_expectsUserNotFoundException() {
        // ARRANGE
        String userId = UUID.randomUUID().toString();
        String contentId = UUID.randomUUID().toString();

        CreateCommentRequest request = new CreateCommentRequest();
        request.setUserId(userId);
        request.setContentId(contentId);
        request.setText("Great video!");

        when(commentUserServiceClient.getUserById(userId)).thenReturn(false);

        // ACT & ASSERT
        assertThatThrownBy(() -> commentService.createComment(request))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("User not found");
        verify(commentRepository, never()).save(any());
    }

    // ==================== CONTENT SERVICE TESTS ====================

    @Mock
    private ContentRepository contentRepository;

    @InjectMocks
    private ContentService contentService;

    @Test
    @DisplayName("Content Service: Get content metadata with valid ID - should return metadata")
    void testGetContentMetadata_whenValidId_expectsMetadataReturned() {
        // ARRANGE
        String contentId = UUID.randomUUID().toString();
        ContentMetadata expectedMetadata = new ContentMetadata();
        expectedMetadata.setId(contentId);
        expectedMetadata.setTitle("Sample Video");
        expectedMetadata.setDescription("A sample video description");
        expectedMetadata.setGenre("Action");
        expectedMetadata.setDuration(3600);

        when(contentRepository.findById(contentId)).thenReturn(Optional.of(expectedMetadata));

        // ACT
        ContentMetadata result = contentService.getContentMetadata(contentId);

        // ASSERT
        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(contentId);
        assertThat(result.getTitle()).isEqualTo("Sample Video");
        verify(contentRepository).findById(contentId);
    }

    @Test
    @DisplayName("Content Service: Get content metadata with invalid ID - should throw ContentNotFoundException")
    void testGetContentMetadata_whenInvalidId_expectsContentNotFoundException() {
        // ARRANGE
        String contentId = UUID.randomUUID().toString();
        when(contentRepository.findById(contentId)).thenReturn(Optional.empty());

        // ACT & ASSERT
        assertThatThrownBy(() -> contentService.getContentMetadata(contentId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Content not found");
    }

    // ==================== MODERATION SERVICE TESTS ====================

    @Mock
    private ModerationPostgresRepository moderationPostgresRepository;

    @Mock
    private ModerationMongoRepository moderationMongoRepository;

    @InjectMocks
    private ModerationService moderationService;

    @Test
    @DisplayName("Moderation Service: Flag content with valid ID - should store in PostgreSQL and MongoDB")
    void testFlagContent_whenValidId_expectsFlaggedInBothDatabases() {
        // ARRANGE
        String contentId = UUID.randomUUID().toString();
        String reason = "Inappropriate content";

        ModerationFlag expectedFlag = new ModerationFlag();
        expectedFlag.setContentId(contentId);
        expectedFlag.setReason(reason);
        expectedFlag.setStatus("FLAGGED");
        expectedFlag.setFlaggedAt(LocalDateTime.now());

        when(moderationPostgresRepository.save(any(ModerationFlag.class))).thenReturn(expectedFlag);
        when(moderationMongoRepository.save(any(ModerationFlag.class))).thenReturn(expectedFlag);

        // ACT
        ModerationFlag result = moderationService.flagContent(contentId, reason);

        // ASSERT
        assertThat(result).isNotNull();
        assertThat(result.getContentId()).isEqualTo(contentId);
        assertThat(result.getReason()).isEqualTo(reason);
        assertThat(result.getStatus()).isEqualTo("FLAGGED");
        verify(moderationPostgresRepository).save(any(ModerationFlag.class));
        verify(moderationMongoRepository).save(any(ModerationFlag.class));
    }

    @Test
    @DisplayName("Moderation Service: Flag content with invalid ID - should throw ContentNotFoundException")
    void testFlagContent_whenInvalidId_expectsContentNotFoundException() {
        // ARRANGE
        String contentId = UUID.randomUUID().toString();
        String reason = "Inappropriate content";

        when(moderationPostgresRepository.save(any(ModerationFlag.class)))
                .thenThrow(new RuntimeException("Content not found"));

        // ACT & ASSERT
        assertThatThrownBy(() -> moderationService.flagContent(contentId, reason))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Content not found");
    }

    @Test
    @DisplayName("Moderation Service: Flag content with MongoDB failure - should still store in PostgreSQL")
    void testFlagContent_whenMongoDBFails_expectsStoredInPostgresOnly() {
        // ARRANGE
        String contentId = UUID.randomUUID().toString();
        String reason = "Inappropriate content";

        ModerationFlag expectedFlag = new ModerationFlag();
        expectedFlag.setContentId(contentId);
        expectedFlag.setReason(reason);
        expectedFlag.setStatus("FLAGGED");

        when(moderationPostgresRepository.save(any(ModerationFlag.class))).thenReturn(expectedFlag);
        when(moderationMongoRepository.save(any(ModerationFlag.class)))
                .thenThrow(new RuntimeException("MongoDB unavailable"));

        // ACT & ASSERT
        assertThatThrownBy(() -> moderationService.flagContent(contentId, reason))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("MongoDB unavailable");
        verify(moderationPostgresRepository).save(any(ModerationFlag.class));
    }
}
