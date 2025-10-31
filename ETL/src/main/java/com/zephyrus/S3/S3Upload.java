package com.zephyrus.S3;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.file.Paths;

public class S3Upload {

    public static void uploadText(String nomeBucket, String nomeArquivoNoBucket, String arquivoLocal) {
        S3Client s3 = S3Config.getS3Client();

        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(nomeBucket)
                .key(nomeArquivoNoBucket)
                .contentType("text/csv")
                .build();

        s3.putObject(request, RequestBody.fromString((arquivoLocal)));

        System.out.println(" Upload concluído para o bucket: " + nomeBucket);
    }
}

