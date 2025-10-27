package com.zephyrus;

import com.zephyrus.S3.S3Download;
import com.zephyrus.S3.S3Upload;
import com.zephyrus.dao.ParametroDao;
import com.zephyrus.factory.ConnectionFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Main {
    public static void main(String[] args) {

        S3Download.downloadArquivo("bucketraw2510", "dadosGrupo.csv", "src\\main\\java\\com\\zephyrus\\Arquivos\\dadosRaw.csv");
        CsvProcessador.processarRaw();
        S3Upload.uploadArquivo("buckettrusted2510","dadosTrusted.csv","src\\main\\java\\com\\zephyrus\\Arquivos\\dadosTrusted.csv");
        CsvProcessador.processarTrusted();
        S3Upload.uploadArquivo("bucketclient2510","dadosClient.csv","src\\main\\java\\com\\zephyrus\\Arquivos\\dadosClient.csv");

    }}

