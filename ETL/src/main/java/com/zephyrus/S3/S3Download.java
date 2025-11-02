package com.zephyrus.S3;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class S3Download {
    public static void downloadArquivo(String nomeBucket, String nomeArquivo, String destino) {
        S3Client s3 = S3Config.getS3Client();
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(nomeBucket)
                .key(nomeArquivo)
                .build();
        try {
            Path caminho = Paths.get(destino);
            Files.deleteIfExists(caminho);
            s3.getObject(request, Paths.get(destino));
            System.out.println("Download concluído de: " + nomeArquivo + " para " + destino);
        } catch (Exception e) {
            System.out.println("Erro: " + e.getMessage());
        }
    }
}
