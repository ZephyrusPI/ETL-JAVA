package com.zephyrus;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.zephyrus.S3.S3Download;
import java.io.File;

public class Handler implements RequestHandler<S3Event, String> {

    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Iniciando Lambda ETL Zephyrus...");

        // Pegando nome do bucket e arquivo do evento
        var record = event.getRecords().get(0);
        String bucketRaw = record.getS3().getBucket().getName();
        String nomeArquivo = record.getS3().getObject().getKey().replace("+", " "); // S3 url encode fix

        // Trocando os dados mockados por variáveis de ambiente
        String bucketTrusted = System.getenv("BUCKET_TRUSTED");
        String bucketClient = System.getenv("BUCKET_CLIENT");

        // Tivemos que passar para a pasta temporária pois o Lambda exige
        String caminhoRawLocal = "/tmp/" + new File(nomeArquivo).getName();

        try {
            context.getLogger().log("Baixando " + nomeArquivo + " do bucket " + bucketRaw);
            S3Download.downloadArquivo(bucketRaw, nomeArquivo, caminhoRawLocal);

            // raw -> trusted
            context.getLogger().log("Processando Raw -> Trusted...");
            ProcessarRaw.processarRawPorSemana(caminhoRawLocal, bucketTrusted);
            ProcessarRaw.processarRawPorMes(caminhoRawLocal, bucketTrusted);

            // trusted -> curated
            // Buscando do trusted os dados de hoje (Nossa regra de negócio)
            context.getLogger().log("Gerando Alertas (Trusted -> Client)...");
            ProcessarTrusted.processarTrustedAlertasDiarios(bucketTrusted, bucketClient);
            ProcessarTrusted.processarTrustedConsumoMensal(bucketTrusted, bucketClient);

            return "Sucesso! Arquivo processado: " + nomeArquivo;

        } catch (Exception e) {
            context.getLogger().log("ERRO FATAL: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}