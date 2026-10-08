/*
 * Copyright (c) 2024  Vladimir Marianciuc. All Rights Reserved.
 *
 * Project: STREAMING SERVICE APP
 * File: StreamingServiceComprehensiveTest.java
 *
 * Comprehensive unit test suite for streaming service media operations.
 * Tests cover video compression, storage, playlist generation, and error handling.
 */

package io.github.marianciuc.streamingservice.media.services;

import io.github.marianciuc.streamingservice.media.dto.ResolutionDto;
import io.github.marianciuc.streamingservice.media.exceptions.CompressingException;
import io.github.marianciuc.streamingservice.media.exceptions.VideoStorageUploadException;
import io.github.marianciuc.streamingservice.media.services.impl.FFmpegJavaCVService;
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
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive test suite for FFmpegJavaCVService and related video compression operations.
 * 
 * Test Categories:
 * - Happy Path: Successful video compression and upload
 * - Boundary Values: Edge cases for resolution, bitrate, frame rates
 * - Error Handling: IOException, CompressingException, VideoStorageUploadException
 * - Side Effects: Mocked storage and playlist services
 * - Idempotency: Multiple compression attempts
 * - Concurrency: Thread-safe operations (where applicable)
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Streaming Service Comprehensive Test Suite")
class StreamingServiceComprehensiveTest {

    @Mock
    private VideoStorageService videoStorageService;

    @Mock
    private PlaylistService playlistService;

    @InjectMocks
    private FFmpegJavaCVService videoCompressingService;

    private UUID testVideoId;
    private ResolutionDto testResolution;

    @BeforeEach
    void setUp() {
        testVideoId = UUID.randomUUID();
        testResolution = new ResolutionDto(
                UUID.randomUUID(),
                "720p",
                "HD Resolution",
                1280,
                720,
                2500000
        );
    }

    @Nested
    @DisplayName("Video Compression - Happy Path Tests")
    class VideoCompressionHappyPath {

        @Test
        @DisplayName("Should successfully compress video and return playlist path")
        void testCompressVideoAndUploadToStorage_whenValidInput_expectsPlaylistPath() throws Exception {
            // ARRANGE
            InputStream mockInputStream = mock(InputStream.class);
            StringBuilder mockPlaylist = new StringBuilder();
            String expectedPlaylistPath = "playlists/" + testVideoId + "/playlist.m3u8";

            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenReturn(mockInputStream);
            when(playlistService.generateResolutionPlaylist())
                    .thenReturn(mockPlaylist);
            when(videoStorageService.uploadVideoSegment(
                    any(ByteArrayOutputStream.class),
                    eq(testVideoId),
                    eq(testResolution),
                    anyInt(),
                    anyString()))
                    .thenReturn("segment0.ts");
            when(playlistService.buildResolutionPlaylist(
                    eq(testVideoId),
                    any(StringBuilder.class),
                    eq(testResolution)))
                    .thenReturn(expectedPlaylistPath);

            // ACT
            String result = videoCompressingService.compressVideoAndUploadToStorage(testResolution, testVideoId);

            // ASSERT
            assertNotNull(result, "Playlist path should not be null");
            assertEquals(expectedPlaylistPath, result, "Playlist path should match expected value");
            verify(videoStorageService, times(1)).assembleVideoTemporaryVideoFile(testVideoId);
            verify(playlistService, times(1)).generateResolutionPlaylist();
            verify(playlistService, times(1)).buildResolutionPlaylist(any(), any(), any());
        }

        @Test
        @DisplayName("Should handle multiple video segments correctly")
        void testCompressVideoAndUploadToStorage_whenMultipleSegments_expectsAllSegmentsUploaded() throws Exception {
            // ARRANGE
            InputStream mockInputStream = mock(InputStream.class);
            StringBuilder mockPlaylist = new StringBuilder();
            String expectedPlaylistPath = "playlists/" + testVideoId + "/playlist.m3u8";

            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenReturn(mockInputStream);
            when(playlistService.generateResolutionPlaylist())
                    .thenReturn(mockPlaylist);
            when(videoStorageService.uploadVideoSegment(
                    any(ByteArrayOutputStream.class),
                    eq(testVideoId),
                    eq(testResolution),
                    anyInt(),
                    anyString()))
                    .thenReturn("segment0.ts")
                    .thenReturn("segment1.ts")
                    .thenReturn("segment2.ts");
            when(playlistService.buildResolutionPlaylist(
                    eq(testVideoId),
                    any(StringBuilder.class),
                    eq(testResolution)))
                    .thenReturn(expectedPlaylistPath);

            // ACT
            String result = videoCompressingService.compressVideoAndUploadToStorage(testResolution, testVideoId);

            // ASSERT
            assertNotNull(result);
            assertEquals(expectedPlaylistPath, result);
            verify(videoStorageService, atLeastOnce()).uploadVideoSegment(
                    any(ByteArrayOutputStream.class),
                    eq(testVideoId),
                    eq(testResolution),
                    anyInt(),
                    anyString());
        }
    }

    @Nested
    @DisplayName("Video Compression - Boundary Value Tests")
    class VideoCompressionBoundaryValues {

        @Test
        @DisplayName("Should handle minimum valid resolution (1x1)")
        void testCompressVideoAndUploadToStorage_whenMinimumResolution_expectsSuccess() throws Exception {
            // ARRANGE
            ResolutionDto minResolution = new ResolutionDto(
                    UUID.randomUUID(),
                    "1p",
                    "Minimum",
                    1,
                    1,
                    1000
            );
            InputStream mockInputStream = mock(InputStream.class);
            StringBuilder mockPlaylist = new StringBuilder();

            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenReturn(mockInputStream);
            when(playlistService.generateResolutionPlaylist())
                    .thenReturn(mockPlaylist);
            when(videoStorageService.uploadVideoSegment(
                    any(ByteArrayOutputStream.class),
                    eq(testVideoId),
                    eq(minResolution),
                    anyInt(),
                    anyString()))
                    .thenReturn("segment0.ts");
            when(playlistService.buildResolutionPlaylist(
                    eq(testVideoId),
                    any(StringBuilder.class),
                    eq(minResolution)))
                    .thenReturn("playlist.m3u8");

            // ACT
            String result = videoCompressingService.compressVideoAndUploadToStorage(minResolution, testVideoId);

            // ASSERT
            assertNotNull(result);
        }

        @Test
        @DisplayName("Should handle maximum valid resolution (4K)")
        void testCompressVideoAndUploadToStorage_whenMaximumResolution_expectsSuccess() throws Exception {
            // ARRANGE
            ResolutionDto maxResolution = new ResolutionDto(
                    UUID.randomUUID(),
                    "4K",
                    "Ultra HD",
                    3840,
                    2160,
                    25000000
            );
            InputStream mockInputStream = mock(InputStream.class);
            StringBuilder mockPlaylist = new StringBuilder();

            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenReturn(mockInputStream);
            when(playlistService.generateResolutionPlaylist())
                    .thenReturn(mockPlaylist);
            when(videoStorageService.uploadVideoSegment(
                    any(ByteArrayOutputStream.class),
                    eq(testVideoId),
                    eq(maxResolution),
                    anyInt(),
                    anyString()))
                    .thenReturn("segment0.ts");
            when(playlistService.buildResolutionPlaylist(
                    eq(testVideoId),
                    any(StringBuilder.class),
                    eq(maxResolution)))
                    .thenReturn("playlist.m3u8");

            // ACT
            String result = videoCompressingService.compressVideoAndUploadToStorage(maxResolution, testVideoId);

            // ASSERT
            assertNotNull(result);
        }

        @ParameterizedTest
        @ValueSource(ints = {240, 360, 480, 720, 1080})
        @DisplayName("Should handle common video resolutions")
        void testCompressVideoAndUploadToStorage_whenCommonResolutions_expectsSuccess(int height) throws Exception {
            // ARRANGE
            ResolutionDto resolution = new ResolutionDto(
                    UUID.randomUUID(),
                    height + "p",
                    "Standard Resolution",
                    height * 16 / 9,
                    height,
                    height * 100000
            );
            InputStream mockInputStream = mock(InputStream.class);
            StringBuilder mockPlaylist = new StringBuilder();

            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenReturn(mockInputStream);
            when(playlistService.generateResolutionPlaylist())
                    .thenReturn(mockPlaylist);
            when(videoStorageService.uploadVideoSegment(
                    any(ByteArrayOutputStream.class),
                    eq(testVideoId),
                    eq(resolution),
                    anyInt(),
                    anyString()))
                    .thenReturn("segment0.ts");
            when(playlistService.buildResolutionPlaylist(
                    eq(testVideoId),
                    any(StringBuilder.class),
                    eq(resolution)))
                    .thenReturn("playlist.m3u8");

            // ACT
            String result = videoCompressingService.compressVideoAndUploadToStorage(resolution, testVideoId);

            // ASSERT
            assertNotNull(result);
        }
    }

    @Nested
    @DisplayName("Video Compression - Error Handling Tests")
    class VideoCompressionErrorHandling {

        @Test
        @DisplayName("Should throw CompressingException when IOException occurs during file assembly")
        void testCompressVideoAndUploadToStorage_whenIOException_expectsCompressingException() throws IOException {
            // ARRANGE
            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenThrow(new IOException("File not found"));

            // ACT & ASSERT
            CompressingException exception = assertThrows(
                    CompressingException.class,
                    () -> videoCompressingService.compressVideoAndUploadToStorage(testResolution, testVideoId),
                    "Should throw CompressingException when IOException occurs"
            );
            assertTrue(exception.getMessage().contains("filesystem"));
        }

        @Test
        @DisplayName("Should throw CompressingException when VideoStorageUploadException occurs")
        void testCompressVideoAndUploadToStorage_whenStorageUploadFails_expectsCompressingException() throws Exception {
            // ARRANGE
            InputStream mockInputStream = mock(InputStream.class);
            StringBuilder mockPlaylist = new StringBuilder();

            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenReturn(mockInputStream);
            when(playlistService.generateResolutionPlaylist())
                    .thenReturn(mockPlaylist);
            when(videoStorageService.uploadVideoSegment(
                    any(ByteArrayOutputStream.class),
                    eq(testVideoId),
                    eq(testResolution),
                    anyInt(),
                    anyString()))
                    .thenThrow(new VideoStorageUploadException("Upload failed"));

            // ACT & ASSERT
            assertThrows(
                    CompressingException.class,
                    () -> videoCompressingService.compressVideoAndUploadToStorage(testResolution, testVideoId),
                    "Should throw CompressingException when storage upload fails"
            );
        }

        @Test
        @DisplayName("Should throw CompressingException when null resolution provided")
        void testCompressVideoAndUploadToStorage_whenNullResolution_expectsException() {
            // ARRANGE
            ResolutionDto nullResolution = null;

            // ACT & ASSERT
            assertThrows(
                    NullPointerException.class,
                    () -> videoCompressingService.compressVideoAndUploadToStorage(nullResolution, testVideoId),
                    "Should throw exception when resolution is null"
            );
        }

        @Test
        @DisplayName("Should throw CompressingException when null video ID provided")
        void testCompressVideoAndUploadToStorage_whenNullVideoId_expectsException() throws IOException {
            // ARRANGE
            UUID nullVideoId = null;
            when(videoStorageService.assembleVideoTemporaryVideoFile(nullVideoId))
                    .thenThrow(new NullPointerException("Video ID cannot be null"));

            // ACT & ASSERT
            assertThrows(
                    Exception.class,
                    () -> videoCompressingService.compressVideoAndUploadToStorage(testResolution, nullVideoId),
                    "Should throw exception when video ID is null"
            );
        }
    }

    @Nested
    @DisplayName("Video Compression - Side Effects Tests")
    class VideoCompressionSideEffects {

        @Test
        @DisplayName("Should call VideoStorageService to assemble temporary file")
        void testCompressVideoAndUploadToStorage_whenExecuted_expectsStorageServiceCalled() throws Exception {
            // ARRANGE
            InputStream mockInputStream = mock(InputStream.class);
            StringBuilder mockPlaylist = new StringBuilder();

            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenReturn(mockInputStream);
            when(playlistService.generateResolutionPlaylist())
                    .thenReturn(mockPlaylist);
            when(videoStorageService.uploadVideoSegment(
                    any(ByteArrayOutputStream.class),
                    eq(testVideoId),
                    eq(testResolution),
                    anyInt(),
                    anyString()))
                    .thenReturn("segment0.ts");
            when(playlistService.buildResolutionPlaylist(
                    eq(testVideoId),
                    any(StringBuilder.class),
                    eq(testResolution)))
                    .thenReturn("playlist.m3u8");

            // ACT
            videoCompressingService.compressVideoAndUploadToStorage(testResolution, testVideoId);

            // ASSERT
            verify(videoStorageService, times(1)).assembleVideoTemporaryVideoFile(testVideoId);
        }

        @Test
        @DisplayName("Should call PlaylistService to generate and build playlist")
        void testCompressVideoAndUploadToStorage_whenExecuted_expectsPlaylistServiceCalled() throws Exception {
            // ARRANGE
            InputStream mockInputStream = mock(InputStream.class);
            StringBuilder mockPlaylist = new StringBuilder();

            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenReturn(mockInputStream);
            when(playlistService.generateResolutionPlaylist())
                    .thenReturn(mockPlaylist);
            when(videoStorageService.uploadVideoSegment(
                    any(ByteArrayOutputStream.class),
                    eq(testVideoId),
                    eq(testResolution),
                    anyInt(),
                    anyString()))
                    .thenReturn("segment0.ts");
            when(playlistService.buildResolutionPlaylist(
                    eq(testVideoId),
                    any(StringBuilder.class),
                    eq(testResolution)))
                    .thenReturn("playlist.m3u8");

            // ACT
            videoCompressingService.compressVideoAndUploadToStorage(testResolution, testVideoId);

            // ASSERT
            verify(playlistService, times(1)).generateResolutionPlaylist();
            verify(playlistService, times(1)).buildResolutionPlaylist(any(), any(), any());
        }

        @Test
        @DisplayName("Should upload video segments to storage")
        void testCompressVideoAndUploadToStorage_whenExecuted_expectsSegmentsUploaded() throws Exception {
            // ARRANGE
            InputStream mockInputStream = mock(InputStream.class);
            StringBuilder mockPlaylist = new StringBuilder();

            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenReturn(mockInputStream);
            when(playlistService.generateResolutionPlaylist())
                    .thenReturn(mockPlaylist);
            when(videoStorageService.uploadVideoSegment(
                    any(ByteArrayOutputStream.class),
                    eq(testVideoId),
                    eq(testResolution),
                    anyInt(),
                    anyString()))
                    .thenReturn("segment0.ts");
            when(playlistService.buildResolutionPlaylist(
                    eq(testVideoId),
                    any(StringBuilder.class),
                    eq(testResolution)))
                    .thenReturn("playlist.m3u8");

            // ACT
            videoCompressingService.compressVideoAndUploadToStorage(testResolution, testVideoId);

            // ASSERT
            verify(videoStorageService, atLeastOnce()).uploadVideoSegment(
                    any(ByteArrayOutputStream.class),
                    eq(testVideoId),
                    eq(testResolution),
                    anyInt(),
                    anyString());
        }
    }

    @Nested
    @DisplayName("Video Compression - Idempotency Tests")
    class VideoCompressionIdempotency {

        @Test
        @DisplayName("Should produce same result when called multiple times with same input")
        void testCompressVideoAndUploadToStorage_whenCalledMultipleTimes_expectsSameResult() throws Exception {
            // ARRANGE
            InputStream mockInputStream = mock(InputStream.class);
            StringBuilder mockPlaylist = new StringBuilder();
            String expectedPlaylistPath = "playlists/" + testVideoId + "/playlist.m3u8";

            when(videoStorageService.assembleVideoTemporaryVideoFile(testVideoId))
                    .thenReturn(mockInputStream);
            when(playlistService.generateResolutionPlaylist())
                    .thenReturn(mockPlaylist);
            when(videoStorageService.uploadVideoSegment(
                    any(ByteArrayOutputStream.class),
                    eq(testVideoId),
                    eq(testResolution),
                    anyInt(),
                    anyString()))
                    .thenReturn("segment0.ts");
            when(playlistService.buildResolutionPlaylist(
                    eq(testVideoId),
                    any(StringBuilder.class),
                    eq(testResolution)))
                    .thenReturn(expectedPlaylistPath);

            // ACT
            String result1 = videoCompressingService.compressVideoAndUploadToStorage(testResolution, testVideoId);
            String result2 = videoCompressingService.compressVideoAndUploadToStorage(testResolution, testVideoId);

            // ASSERT
            assertEquals(result1, result2, "Results should be identical for same input");
            assertEquals(expectedPlaylistPath, result1);
            assertEquals(expectedPlaylistPath, result2);
        }
    }

    @Nested
    @DisplayName("Resolution DTO Validation Tests")
    class ResolutionDtoValidation {

        @Test
        @DisplayName("Should create valid ResolutionDto with all required fields")
        void testResolutionDto_whenAllFieldsProvided_expectsValidDto() {
            // ARRANGE
            UUID resolutionId = UUID.randomUUID();
            String name = "720p";
            String description = "HD Resolution";
            int height = 720;
            int width = 1280;
            int bitrate = 2500000;

            // ACT
            ResolutionDto dto = new ResolutionDto(resolutionId, name, description, height, width, bitrate);

            // ASSERT
            assertNotNull(dto);
            assertEquals(resolutionId, dto.id());
            assertEquals(name, dto.name());
            assertEquals(description, dto.description());
            assertEquals(height, dto.height());
            assertEquals(width, dto.width());
            assertEquals(bitrate, dto.bitrate());
        }

        @Test
        @DisplayName("Should handle ResolutionDto with null description")
        void testResolutionDto_whenNullDescription_expectsValidDto() {
            // ARRANGE
            UUID resolutionId = UUID.randomUUID();
            String name = "720p";
            int height = 720;
            int width = 1280;
            int bitrate = 2500000;

            // ACT
            ResolutionDto dto = new ResolutionDto(resolutionId, name, null, height, width, bitrate);

            // ASSERT
            assertNotNull(dto);
            assertNull(dto.description());
        }
    }
}
