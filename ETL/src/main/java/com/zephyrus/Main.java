package com.zephyrus;

import com.zephyrus.S3.S3Download;

public class Main {
    public static void main(String[] args) {

        System.out.println("- Iniciando execução do ETL...");

        String caminhoRawLocal = "/app/dadosRaw.csv";

        String bucketRaw = "raw-zephyrus";
        String bucketTrusted = "trusted-zephyrus";
        String bucketClient = "client-zephyrus";

        try {
            System.out.println("- Baixando dados do bucket RAW...");
            S3Download.downloadArquivo(bucketRaw, "dadosRaw.csv", caminhoRawLocal);

            System.out.println("- Processando dados semanais (Trusted)...");
            ProcessarRaw.processarRawPorSemana(caminhoRawLocal, bucketTrusted);

            System.out.println("- Gerando alertas diários (Client)...");
            ProcessarTrusted.processarTrustedAlertasDiarios(bucketTrusted, bucketClient);

            System.out.println("- Processando dados mensais (Trusted)...");
            ProcessarRaw.processarRawPorMes(caminhoRawLocal, bucketTrusted);

            System.out.println("- Gerando consumo mensal (Client)...");
            ProcessarTrusted.processarTrustedConsumoMensal(bucketTrusted, bucketClient);

            System.out.println("- ETL Finalizado com sucesso!");

        } catch (Exception e) {
            System.err.println("- Erro durante a execução do ETL: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
