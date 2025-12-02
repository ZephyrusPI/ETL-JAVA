package com.zephyrus;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;

public class Handler implements RequestHandler<S3Event, String> {

    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Iniciando Lambda Zephyrus...");

        try {
            var record = event.getRecords().get(0);
            String bucketOrigem = record.getS3().getBucket().getName();
            String nomeArquivo = record.getS3().getObject().getKey().replace("+", " ");

            context.getLogger().log("Origem: " + bucketOrigem + " | Arquivo: " + nomeArquivo);

            String bucketClient = System.getenv("BUCKET_CLIENT");

            // Chama o processamento
            ProcessarTrusted.processarTrustedAlertasDiarios(bucketOrigem, bucketClient, nomeArquivo);

            return "Sucesso! " + nomeArquivo;

        } catch (Exception e) {
            context.getLogger().log("ERRO FATAL: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}