package com.kafka.hello.AzureConfig;


import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.blob.specialized.BlockBlobClient;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;


@Slf4j
@Service
public class FileService {

    private final BlobServiceClient blobServiceClient;

    @Autowired
    public FileService(BlobServiceClient blobServiceClient) {
        this.blobServiceClient = blobServiceClient;
    }



    public String uploadFile(byte[] file, String containerName, String filename) throws IOException {
        BlobContainerClient blobContainerClient = getBlobContainerClient(containerName);
        String fileNameFinal = filename.concat(".dat");
        BlockBlobClient blockBlobClient = blobContainerClient.getBlobClient(fileNameFinal).getBlockBlobClient();
        try (BlobOutputStream bos = blockBlobClient.getBlobOutputStream()) {
            bos.write(file);
        }
        String url = blockBlobClient.getBlobUrl();
        log.info("blob url {} ", url);
        return url;
    }

    private @NonNull BlobContainerClient getBlobContainerClient(@NonNull String containerName) {
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(containerName);
        if (!blobContainerClient.exists()) {
            blobContainerClient.create();
        }
        return blobContainerClient;
    }

}
