package com.zephyrus;

import com.zephyrus.S3.S3Download;

import java.io.*;

public class Main {
    public static void main(String[] args) throws FileNotFoundException {

      //  S3Download.downloadArquivo("bucketraw2510", "dadosGrupo.csv", "ETL\\src\\main\\java\\com\\zephyrus\\dadosRaw.csv");
     //S3Upload.uploadArquivo("bucketclient2510","dadosClient.csv","src\\main\\java\\com\\zephyrus\\Arquivos\\dadosClient.csv");
     ProcessarRaw.processarRawPorSemana("ETL\\src\\main\\java\\com\\zephyrus\\dadosRaw.csv","buckettrusted2510");
     ProcessarTrusted.processarTrustedAlertasDiarios("buckettrusted2510","bucketclient2510");
        ProcessarRaw.processarRawPorMes("ETL\\src\\main\\java\\com\\zephyrus\\dadosRaw.csv","buckettrusted2510");
        ProcessarTrusted.processarTrustedConsumoMensal("buckettrusted2510","bucketclient2510");


    }}

