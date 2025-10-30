package com.zephyrus;

import java.io.*;

public class Main {
    public static void main(String[] args) throws FileNotFoundException {

       // S3Download.downloadArquivo("bucketraw2510", "dadosGrupo.csv", "src\\main\\java\\com\\zephyrus\\Arquivos\\dadosRaw.csv");
        //CsvProcessador.processarRaw();
        //S3Upload.uploadArquivo("buckettrusted2510","dadosTrusted.csv","src\\main\\java\\com\\zephyrus\\Arquivos\\dadosTrusted.csv");
     //S3Upload.uploadArquivo("bucketclient2510","dadosClient.csv","src\\main\\java\\com\\zephyrus\\Arquivos\\dadosClient.csv");
       // ProcessarRaw.processarRawPorSemana("C:\\Users\\isafa\\Documents\\ETL-JAVA\\ETL\\src\\main\\java\\com\\zephyrus\\dadosRaw.csv","buckettrusted2510");
        ProcessarTrusted.processarTrustedAlertas("buckettrusted2510");

    }}

