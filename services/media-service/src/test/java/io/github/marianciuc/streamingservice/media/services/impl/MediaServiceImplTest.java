/*
 * Copyright (c) 2024  Vladimir Marianciuc. All Rights Reserved.
 *
 * Project: STREAMING SERVICE APP
 * File: MediaServiceImplTest.java
 *
 */

package io.github.marianciuc.streamingservice.media.services.impl;

import io.github.marianciuc.streamingservice.media.dto.ResolutionDto;
import io.github.marianciuc.streamingservice.media.entity.Resolution;
import io.github.marianciuc.streamingservice.media.exceptions.ChunkUploadNotInitializedException;
import io.github.marianciuc.streamingservice.media.exceptions.ChunkUploadTimeoutException;
import io.github.marianciuc.streamingservice.media.exceptions.CompressingException;
import io.github.marianciuc.streamingservice.media.exceptions.NotFoundException;
import io.github.marianciuc.streamingservice.media.kafka.KafkaResolutionProducer;
import io.github.marianciuc.streamingservice.media.kafka.messages.ResolutionMessage;
import io.github.marianciuc.streamingservice.media.repository.ResolutionRepository;
import io.github.marianciuc.streamingservice.media.services.PlaylistService;
import io.github.marianciuc.streamingservice.media.services.VideoStorageService;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.Frame;
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
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive unit test suite for media service implementations.
 * Tests cover ChunkStateServiceImpl, ResolutionServiceImpl, and FFmpegJavaCVService.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Media Service Implementation Tests")
class MediaServiceImplTest {

    // ==================== ChunkStateServiceImpl Tests ====================

    @Nested
    @DisplayName("ChunkStateServiceImpl Tests")
    class ChunkStateServiceImplTests {

        @Mock
        private RedisTemplate<String, Boolean[]> redisTemplate;

        @Mock
        private ValueOperations<String, Boolean[]> valueOperations;

        @InjectMocks
        private ChunkStateServiceImpl chunkStateService;

        private UUID testFileId;
        private String testKey;

        @BeforeEach
        void setUp() {
            testFileId = UUID.randomUUID();
            testKey = "chunk_upload::" + testFileId;
            when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        }

        // ========== Happy Path Tests ==========

        @Test
        @DisplayName("should create chunk upload status successfully")
        void testCreateChunkUploadStatus_whenValidTotalChunks_expectsSuccess() {
            // ARRANGE
            int totalChunks = 5;
            ArgumentCaptor<Boolean[]> captor = ArgumentCaptor.forClass(Boolean[].class);

            // ACT
            chunkStateService.createChunkUploadStatus(testFileId, totalChunks);

            // ASSERT
            verify(valueOperations).set(eq(testKey), captor.capture(), eq(1800L), eq(TimeUnit.SECONDS));
            Boolean[] capturedStatus = captor.getValue();
            assertEquals(totalChunks, capturedStatus.length);
            assertTrue(Arrays.stream(capturedStatus).allMatch(status -> !status));
        }

        @Test
        @DisplayName("should update chunk upload status successfully")
        void testUpdateChunkUploadStatus_whenValidChunkNumber_expectsSuccess() {
            // ARRANGE
            int chunkNumber = 2;
            int totalChunks = 5;
            Boolean[] initialStatus = new Boolean[]{false, false, false, false, false};
            when(valueOperations.get(testKey)).thenReturn(initialStatus);
            ArgumentCaptor<Boolean[]> captor = ArgumentCaptor.forClass(Boolean[].class);

            // ACT
            chunkStateService.updateChunkUploadStatus(testFileId, chunkNumber, totalChunks);

            // ASSERT
            verify(valueOperations).set(eq(testKey), captor.capture(), eq(1800L), eq(TimeUnit.SECONDS));
            Boolean[] capturedStatus = captor.getValue();
            assertTrue(capturedStatus[chunkNumber - 1]);
            assertFalse(capturedStatus[0]);
        }

        @Test
        @DisplayName("should return true when all chunks are uploaded")
        void testIsUploadComplete_whenAllChunksUploaded_expectsTrue() {
            // ARRANGE
            Boolean[] completedStatus = new Boolean[]{true, true, true};
            when(valueOperations.get(testKey)).thenReturn(completedStatus);

            // ACT
            boolean result = chunkStateService.isUploadComplete(testFileId);

            // ASSERT
            assertTrue(result);
        }

        @Test
        @DisplayName("should return false when not all chunks are uploaded")
        void testIsUploadComplete_whenNotAllChunksUploaded_expectsFalse() {
            // ARRANGE
            Boolean[] incompleteStatus = new Boolean[]{true, false, true};
            when(valueOperations.get(testKey)).thenReturn(incompleteStatus);

            // ACT
            boolean result = chunkStateService.isUploadComplete(testFileId);

            // ASSERT
            assertFalse(result);
        }

        @Test
        @DisplayName("should delete chunk upload status successfully")
        void testDeleteChunkUploadStatus_whenValidFileId_expectsSuccess() {
            // ACT
            chunkStateService.deleteChunkUploadStatus(testFileId);

            // ASSERT
            verify(redisTemplate).delete(testKey);
        }

        // ========== Boundary Value Tests ==========

        @Test
        @DisplayName("should create chunk upload status with single chunk")
        void testCreateChunkUploadStatus_whenSingleChunk_expectsSuccess() {
            // ARRANGE
            int totalChunks = 1;
            ArgumentCaptor<Boolean[]> captor = ArgumentCaptor.forClass(Boolean[].class);

            // ACT
            chunkStateService.createChunkUploadStatus(testFileId, totalChunks);

            // ASSERT
            verify(valueOperations).set(eq(testKey), captor.capture(), eq(1800L), eq(TimeUnit.SECONDS));
            assertEquals(1, captor.getValue().length);
        }

        @Test
        @DisplayName("should update first chunk successfully")
        void testUpdateChunkUploadStatus_whenFirstChunk_expectsSuccess() {
            // ARRANGE
            Boolean[] status = new Boolean[]{false, false, false};
            when(valueOperations.get(testKey)).thenReturn(status);
            ArgumentCaptor<Boolean[]> captor = ArgumentCaptor.forClass(Boolean[].class);

            // ACT
            chunkStateService.updateChunkUploadStatus(testFileId, 1, 3);

            // ASSERT
            verify(valueOperations).set(eq(testKey), captor.capture(), eq(1800L), eq(TimeUnit.SECONDS));
            assertTrue(captor.getValue()[0]);
        }

        @Test
        @DisplayName("should update last chunk successfully")
        void testUpdateChunkUploadStatus_whenLastChunk_expectsSuccess() {
            // ARRANGE
            int totalChunks = 5;
            Boolean[] status = new Boolean[]{true, true, true, true, false};
            when(valueOperations.get(testKey)).thenReturn(status);
            ArgumentCaptor<Boolean[]> captor = ArgumentCaptor.forClass(Boolean[].class);

            // ACT
            chunkStateService.updateChunkUploadStatus(testFileId, totalChunks, totalChunks);

            // ASSERT
            verify(valueOperations).set(eq(testKey), captor.capture(), eq(1800L), eq(TimeUnit.SECONDS));
            assertTrue(captor.getValue()[totalChunks - 1]);
        }

        // ========== Error/Exception Tests ==========

        @Test
        @DisplayName("should throw exception when total chunks is zero")
        void testCreateChunkUploadStatus_whenZeroTotalChunks_expectsIllegalArgumentException() {
            // ACT & ASSERT
            assertThrows(IllegalArgumentException.class, () ->
                    chunkStateService.createChunkUploadStatus(testFileId, 0)
            );
        }

        @Test
        @DisplayName("should throw exception when total chunks is negative")
        void testCreateChunkUploadStatus_whenNegativeTotalChunks_expectsIllegalArgumentException() {
            // ACT & ASSERT
            assertThrows(IllegalArgumentException.class, () ->
                    chunkStateService.createChunkUploadStatus(testFileId, -5)
            );
        }

        @Test
        @DisplayName("should throw exception when chunk number is negative")
        void testUpdateChunkUploadStatus_whenNegativeChunkNumber_expectsIllegalArgumentException() {
            // ARRANGE
            Boolean[] status = new Boolean[]{false, false};
            when(valueOperations.get(testKey)).thenReturn(status);

            // ACT & ASSERT
            assertThrows(IllegalArgumentException.class, () ->
                    chunkStateService.updateChunkUploadStatus(testFileId, -1, 2)
            );
        }

        @Test
        @DisplayName("should throw exception when chunk number exceeds total chunks")
        void testUpdateChunkUploadStatus_whenChunkNumberExceedsTotalChunks_expectsChunkUploadTimeoutException() {
            // ARRANGE
            Boolean[] status = new Boolean[]{false, false, false};
            when(valueOperations.get(testKey)).thenReturn(status);

            // ACT & ASSERT
            assertThrows(ChunkUploadTimeoutException.class, () ->
                    chunkStateService.updateChunkUploadStatus(testFileId, 5, 3)
            );
        }

        @Test
        @DisplayName("should throw exception when upload status not initialized")
        void testUpdateChunkUploadStatus_whenStatusNotInitialized_expectsChunkUploadNotInitializedException() {
            // ARRANGE
            when(valueOperations.get(testKey)).thenReturn(null);

            // ACT & ASSERT
            assertThrows(ChunkUploadNotInitializedException.class, () ->
                    chunkStateService.updateChunkUploadStatus(testFileId, 1, 3)
            );
        }

        @Test
        @DisplayName("should throw exception when checking upload complete for uninitialized status")
        void testIsUploadComplete_whenStatusNotInitialized_expectsChunkUploadNotInitializedException() {
            // ARRANGE
            when(valueOperations.get(testKey)).thenReturn(null);

            // ACT & ASSERT
            assertThrows(ChunkUploadNotInitializedException.class, () ->
                    chunkStateService.isUploadComplete(testFileId)
            );
        }

        // ========== Parametrized Tests ==========

        @ParameterizedTest
        @ValueSource(ints = {1, 5, 10, 100})
        @DisplayName("should create chunk upload status for various chunk counts")
        void testCreateChunkUploadStatus_withVariousChunkCounts_expectsSuccess(int totalChunks) {
            // ARRANGE
            ArgumentCaptor<Boolean[]> captor = ArgumentCaptor.forClass(Boolean[].class);

            // ACT
            chunkStateService.createChunkUploadStatus(testFileId, totalChunks);

            // ASSERT
            verify(valueOperations).set(eq(testKey), captor.capture(), eq(1800L), eq(TimeUnit.SECONDS));
            assertEquals(totalChunks, captor.getValue().length);
        }
    }

    // ==================== ResolutionServiceImpl Tests ====================

    @Nested
    @DisplayName("ResolutionServiceImpl Tests")
    class ResolutionServiceImplTests {

        @Mock
        private ResolutionRepository resolutionRepository;

        @Mock
        private KafkaResolutionProducer kafkaResolutionProducer;

        @InjectMocks
        private ResolutionServiceImpl resolutionService;

        private ResolutionDto testResolutionDto;
        private Resolution testResolution;
        private UUID testResolutionId;

        @BeforeEach
        void setUp() {
            testResolutionId = UUID.randomUUID();
            testResolutionDto = new ResolutionDto(
                    testResolutionId,
                    "1080p",
                    "Full HD resolution",
                    1920,
                    1080,
                    5000
            );
            testResolution = Resolution.builder()
                    .id(testResolutionId)
                    .name("1080p")
                    .description("Full HD resolution")
                    .width(1920)
                    .height(1080)
                    .bitrate(5000)
                    .build();
        }

        // ========== Happy Path Tests ==========

        @Test
        @DisplayName("should create resolution successfully")
        void testCreateResolution_whenValidDto_expectsSuccess() {
            // ARRANGE
            when(resolutionRepository.save(any(Resolution.class))).thenReturn(testResolution);

            // ACT
            ResolutionDto result = resolutionService.createResolution(testResolutionDto);

            // ASSERT
            assertNotNull(result);
            assertEquals("1080p", result.name());
            assertEquals(1920, result.width());
            assertEquals(1080, result.height());
            assertEquals(5000, result.bitrate());
            verify(resolutionRepository).save(any(Resolution.class));
            verify(kafkaResolutionProducer).sendCreatedResolutionTopic(any(ResolutionMessage.class));
        }

        @Test
        @DisplayName("should update resolution successfully")
        void testUpdateResolution_whenValidDto_expectsSuccess() {
            // ARRANGE
            when(resolutionRepository.findById(testResolutionId)).thenReturn(Optional.of(testResolution));
            when(resolutionRepository.save(any(Resolution.class))).thenReturn(testResolution);

            // ACT
            ResolutionDto result = resolutionService.updateResolution(testResolutionDto);

            // ASSERT
            assertNotNull(result);
            assertEquals("1080p", result.name());
            verify(resolutionRepository).findById(testResolutionId);
            verify(resolutionRepository).save(any(Resolution.class));
            verify(kafkaResolutionProducer).sendUpdateResolutionTopic(any(ResolutionMessage.class));
        }

        @Test
        @DisplayName("should get all resolutions successfully")
        void testGetAllResolutions_whenResolutionsExist_expectsListOfResolutions() {
            // ARRANGE
            List<Resolution> resolutions = Arrays.asList(testResolution);
            when(resolutionRepository.findAll()).thenReturn(resolutions);

            // ACT
            List<ResolutionDto> result = resolutionService.getAllResolutions();

            // ASSERT
            assertNotNull(result);
            assertEquals(1, result.size());
            assertEquals("1080p", result.get(0).name());
            verify(resolutionRepository).findAll();
        }

        @Test
        @DisplayName("should get resolution by id successfully")
        void testGetResolutionById_whenResolutionExists_expectsResolution() {
            // ARRANGE
            when(resolutionRepository.findById(testResolutionId)).thenReturn(Optional.of(testResolution));

            // ACT
            ResolutionDto result = resolutionService.getResolutionById(testResolutionId);

            // ASSERT
            assertNotNull(result);
            assertEquals("1080p", result.name());
            verify(resolutionRepository).findById(testResolutionId);
        }

        @Test
        @DisplayName("should delete resolution successfully")
        void testDeleteResolution_whenResolutionExists_expectsSuccess() {
            // ARRANGE
            when(resolutionRepository.findById(testResolutionId)).thenReturn(Optional.of(testResolution));

            // ACT
            resolutionService.deleteResolution(testResolutionId);

            // ASSERT
            verify(resolutionRepository).findById(testResolutionId);
            verify(resolutionRepository).delete(testResolution);
            verify(kafkaResolutionProducer).sendDeleteResolutionTopic(any(ResolutionMessage.class));
        }

        // ========== Boundary Value Tests ==========

        @Test
        @DisplayName("should create resolution with minimum valid dimensions")
        void testCreateResolution_whenMinimumDimensions_expectsSuccess() {
            // ARRANGE
            ResolutionDto minResolution = new ResolutionDto(
                    UUID.randomUUID(),
                    "144p",
                    "Minimum resolution",
                    256,
                    144,
                    100
            );
            Resolution minEntity = Resolution.builder()
                    .id(minResolution.id())
                    .name("144p")
                    .width(256)
                    .height(144)
                    .bitrate(100)
                    .build();
            when(resolutionRepository.save(any(Resolution.class))).thenReturn(minEntity);

            // ACT
            ResolutionDto result = resolutionService.createResolution(minResolution);

            // ASSERT
            assertNotNull(result);
            assertEquals(256, result.width());
            assertEquals(144, result.height());
        }

        @Test
        @DisplayName("should create resolution with maximum valid dimensions")
        void testCreateResolution_whenMaximumDimensions_expectsSuccess() {
            // ARRANGE
            ResolutionDto maxResolution = new ResolutionDto(
                    UUID.randomUUID(),
                    "4K",
                    "Ultra HD resolution",
                    3840,
                    2160,
                    50000
            );
            Resolution maxEntity = Resolution.builder()
                    .id(maxResolution.id())
                    .name("4K")
                    .width(3840)
                    .height(2160)
                    .bitrate(50000)
                    .build();
            when(resolutionRepository.save(any(Resolution.class))).thenReturn(maxEntity);

            // ACT
            ResolutionDto result = resolutionService.createResolution(maxResolution);

            // ASSERT
            assertNotNull(result);
            assertEquals(3840, result.width());
            assertEquals(2160, result.height());
        }

        @Test
        @DisplayName("should get all resolutions when empty list")
        void testGetAllResolutions_whenNoResolutions_expectsEmptyList() {
            // ARRANGE
            when(resolutionRepository.findAll()).thenReturn(List.of());

            // ACT
            List<ResolutionDto> result = resolutionService.getAllResolutions();

            // ASSERT
            assertNotNull(result);
            assertTrue(result.isEmpty());
        }

        // ========== Error/Exception Tests ==========

        @Test
        @DisplayName("should throw exception when updating non-existent resolution")
        void testUpdateResolution_whenResolutionNotFound_expectsNotFoundException() {
            // ARRANGE
            when(resolutionRepository.findById(testResolutionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () ->
                    resolutionService.updateResolution(testResolutionDto)
            );
        }

        @Test
        @DisplayName("should throw exception when getting non-existent resolution")
        void testGetResolutionById_whenResolutionNotFound_expectsNotFoundException() {
            // ARRANGE
            when(resolutionRepository.findById(testResolutionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () ->
                    resolutionService.getResolutionById(testResolutionId)
            );
        }

        @Test
        @DisplayName("should throw exception when deleting non-existent resolution")
        void testDeleteResolution_whenResolutionNotFound_expectsNotFoundException() {
            // ARRANGE
            when(resolutionRepository.findById(testResolutionId)).thenReturn(Optional.empty());

            // ACT & ASSERT
            assertThrows(NotFoundException.class, () ->
                    resolutionService.deleteResolution(testResolutionId)
            );
        }

        // ========== Kafka Producer Verification Tests ==========

        @Test
        @DisplayName("should send created resolution message to Kafka")
        void testCreateResolution_whenSuccess_expectsKafkaMessageSent() {
            // ARRANGE
            when(resolutionRepository.save(any(Resolution.class))).thenReturn(testResolution);
            ArgumentCaptor<ResolutionMessage> captor = ArgumentCaptor.forClass(ResolutionMessage.class);

            // ACT
            resolutionService.createResolution(testResolutionDto);

            // ASSERT
            verify(kafkaResolutionProducer).sendCreatedResolutionTopic(captor.capture());
            assertNotNull(captor.getValue());
        }

        @Test
        @DisplayName("should send updated resolution message to Kafka")
        void testUpdateResolution_whenSuccess_expectsKafkaMessageSent() {
            // ARRANGE
            when(resolutionRepository.findById(testResolutionId)).thenReturn(Optional.of(testResolution));
            when(resolutionRepository.save(any(Resolution.class))).thenReturn(testResolution);
            ArgumentCaptor<ResolutionMessage> captor = ArgumentCaptor.forClass(ResolutionMessage.class);

            // ACT
            resolutionService.updateResolution(testResolutionDto);

            // ASSERT
            verify(kafkaResolutionProducer).sendUpdateResolutionTopic(captor.capture());
            assertNotNull(captor.getValue());
        }

        @Test
        @DisplayName("should send deleted resolution message to Kafka")
        void testDeleteResolution_whenSuccess_expectsKafkaMessageSent() {
            // ARRANGE
            when(resolutionRepository.findById(testResolutionId)).thenReturn(Optional.of(testResolution));
            ArgumentCaptor<ResolutionMessage> captor = ArgumentCaptor.forClass(ResolutionMessage.class);

            // ACT
            resolutionService.deleteResolution(testResolutionId);

            // ASSERT
            verify(kafkaResolutionProducer).sendDeleteResolutionTopic(captor.capture());
            assertNotNull(captor.getValue());
        }
    }

    // ==================== FFmpegJavaCVService Tests ====================

    @Nested
    @DisplayName("FFmpegJavaCVService Tests")
    class FFmpegJavaCVServiceTests {

        @Mock
        private VideoStorageService videoStorageService;

        @Mock
        private PlaylistService playlistService;

        @InjectMocks
        private FFmpegJavaCVService ffmpegService;

        private ResolutionDto testResolution;
        private UUID testVideoId;

        @BeforeEach
        void setUp() {
            testVideoId = UUID.randomUUID();
            testResolution = new ResolutionDto(
                    UUID.randomUUID(),
                    "720p",
                    "HD resolution",
                    1280,
                    720,
                    2500
            );
        }

        // ========== Happy Path Tests ==========

        @Test
        @DisplayName("should compress video and upload to storage successfully")
        void testCompressVideoAndUploadToStorage_whenValidInputs_expectsPlaylistPath() throws Exception {
            // ARRANGE
            InputStream mockInputStream = mock(InputStream.class);
            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenReturn(mockInputStream);
            when(playlistService.generateResolutionPlaylist())
                    .thenReturn(new StringBuilder());
            when(videoStorageService.uploadVideoSegment(any(ByteArrayOutputStream.class), eq(testVideoId),
                    eq(testResolution), anyInt(), anyString()))
                    .thenReturn("segment0.ts");
            when(playlistService.buildResolutionPlaylist(eq(testVideoId), any(StringBuilder.class),
                    eq(testResolution)))
                    .thenReturn("playlist.m3u8");

            // ACT
            String result = ffmpegService.compressVideoAndUploadToStorage(testResolution, testVideoId);

            // ASSERT
            assertNotNull(result);
            assertEquals("playlist.m3u8", result);
            verify(videoStorageService).assembleVideoTemporaryVideoFile(testVideoId);
            verify(playlistService).generateResolutionPlaylist();
        }

        // ========== Boundary Value Tests ==========

        @Test
        @DisplayName("should handle compression with minimum resolution")
        void testCompressVideoAndUploadToStorage_whenMinimumResolution_expectsSuccess() throws Exception {
            // ARRANGE
            ResolutionDto minResolution = new ResolutionDto(
                    UUID.randomUUID(),
                    "144p",
                    "Minimum resolution",
                    256,
                    144,
                    100
            );
            InputStream mockInputStream = mock(InputStream.class);
            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenReturn(mockInputStream);
            when(playlistService.generateResolutionPlaylist())
                    .thenReturn(new StringBuilder());
            when(videoStorageService.uploadVideoSegment(any(ByteArrayOutputStream.class), eq(testVideoId),
                    eq(minResolution), anyInt(), anyString()))
                    .thenReturn("segment0.ts");
            when(playlistService.buildResolutionPlaylist(eq(testVideoId), any(StringBuilder.class),
                    eq(minResolution)))
                    .thenReturn("playlist.m3u8");

            // ACT
            String result = ffmpegService.compressVideoAndUploadToStorage(minResolution, testVideoId);

            // ASSERT
            assertNotNull(result);
        }

        @Test
        @DisplayName("should handle compression with maximum resolution")
        void testCompressVideoAndUploadToStorage_whenMaximumResolution_expectsSuccess() throws Exception {
            // ARRANGE
            ResolutionDto maxResolution = new ResolutionDto(
                    UUID.randomUUID(),
                    "4K",
                    "Ultra HD resolution",
                    3840,
                    2160,
                    50000
            );
            InputStream mockInputStream = mock(InputStream.class);
            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenReturn(mockInputStream);
            when(playlistService.generateResolutionPlaylist())
                    .thenReturn(new StringBuilder());
            when(videoStorageService.uploadVideoSegment(any(ByteArrayOutputStream.class), eq(testVideoId),
                    eq(maxResolution), anyInt(), anyString()))
                    .thenReturn("segment0.ts");
            when(playlistService.buildResolutionPlaylist(eq(testVideoId), any(StringBuilder.class),
                    eq(maxResolution)))
                    .thenReturn("playlist.m3u8");

            // ACT
            String result = ffmpegService.compressVideoAndUploadToStorage(maxResolution, testVideoId);

            // ASSERT
            assertNotNull(result);
        }

        // ========== Error/Exception Tests ==========

        @Test
        @DisplayName("should throw CompressingException when video storage fails")
        void testCompressVideoAndUploadToStorage_whenStorageServiceFails_expectsCompressingException() {
            // ARRANGE
            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenThrow(new RuntimeException("Storage service error"));

            // ACT & ASSERT
            assertThrows(CompressingException.class, () ->
                    ffmpegService.compressVideoAndUploadToStorage(testResolution, testVideoId)
            );
        }

        @Test
        @DisplayName("should throw CompressingException when IO error occurs")
        void testCompressVideoAndUploadToStorage_whenIOException_expectsCompressingException() throws IOException {
            // ARRANGE
            InputStream mockInputStream = mock(InputStream.class);
            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenReturn(mockInputStream);
            when(mockInputStream.read(any(byte[].class))).thenThrow(new IOException("IO error"));

            // ACT & ASSERT
            assertThrows(CompressingException.class, () ->
                    ffmpegService.compressVideoAndUploadToStorage(testResolution, testVideoId)
            );
        }

        // ========== Null/Invalid Input Tests ==========

        @Test
        @DisplayName("should handle null resolution gracefully")
        void testCompressVideoAndUploadToStorage_whenNullResolution_expectsNullPointerException() {
            // ACT & ASSERT
            assertThrows(NullPointerException.class, () ->
                    ffmpegService.compressVideoAndUploadToStorage(null, testVideoId)
            );
        }

        @Test
        @DisplayName("should handle null video id gracefully")
        void testCompressVideoAndUploadToStorage_whenNullVideoId_expectsNullPointerException() {
            // ACT & ASSERT
            assertThrows(NullPointerException.class, () ->
                    ffmpegService.compressVideoAndUploadToStorage(testResolution, null)
            );
        }
    }
}
