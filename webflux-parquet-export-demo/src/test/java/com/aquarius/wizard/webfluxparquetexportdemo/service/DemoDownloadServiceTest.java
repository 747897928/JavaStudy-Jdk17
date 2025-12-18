package com.aquarius.wizard.webfluxparquetexportdemo.service;

import com.aquarius.wizard.webfluxparquetexportdemo.model.FileFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.mock.http.server.reactive.MockServerHttpResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DemoDownloadServiceTest {

    @TempDir
    Path tempDir;

    @Mock
    ParquetExportService exportService;

    @Mock
    ParquetStagingService stagingService;

    @Test
    void downloadSetsHeadersAndCleansUp() throws Exception {
        File staged = tempDir.resolve("abc123.parquet").toFile();
        assertThat(staged.createNewFile()).isTrue();

        doNothing().when(exportService).assertExportCapacityOrThrow();
        when(stagingService.stageParquetFromSource(anyString(), any())).thenReturn(Mono.just(staged));
        when(stagingService.deleteStagedParquet(any())).thenReturn(Mono.empty());
        when(exportService.streamExport(any(), any(), any(), anyString())).thenReturn(Flux.<DataBuffer>empty());

        DemoDownloadService service = new DemoDownloadService(exportService, stagingService);
        MockServerHttpResponse response = new MockServerHttpResponse();

        StepVerifier.create(service.download(FileFormat.ZIP, "s3a://bucket/key.parquet", 10L, response))
                .verifyComplete();

        assertThat(response.getHeaders().getContentType()).isEqualTo(new MediaType("application", "zip"));
        assertThat(response.getHeaders().getFirst(HttpHeaders.CONTENT_DISPOSITION))
                .isEqualTo("attachment; filename=\"abc123.zip\"");

        verify(stagingService).deleteStagedParquet(staged);
    }
}
