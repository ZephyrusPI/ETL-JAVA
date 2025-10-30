package com.zephyrus;

import com.zephyrus.S3.S3Upload;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.HashMap;
import java.util.Map;

public class ProcessarRaw {


    public static void processarRawPorSemana(String caminhoRaw,String bucketDestino) throws FileNotFoundException {
   try(  BufferedReader reader=new BufferedReader(new FileReader(caminhoRaw));
      ) { Iterable<CSVRecord> linhas = CSVFormat.DEFAULT
           .withFirstRecordAsHeader()
           .parse(reader);






    Map<String, StringBuilder> datas = new HashMap<>();

    for (CSVRecord linha : linhas) {
        String dataStr = linha.get("timestamp");
        String dataSomente = dataStr.split(" ")[0];
        LocalDate data = LocalDate.parse(dataSomente);
        System.out.println(data);



        int ano = data.getYear();
        int mes = data.getMonthValue();
        int semana = data.get(WeekFields.ISO.weekOfMonth());

        String pasta = ano + "/" + mes + "/" + semana;

        datas.putIfAbsent(pasta, new StringBuilder("timestamp,ID,Modelo,Area,CPU,RAM,Disco,Processos,Bateria\n"));
        datas.get(pasta).append(String.join(",",
                padronizaData(linha.get("timestamp")),
                linha.get("ID"),
                linha.get("Modelo"),
                linha.get("Area"),
                converterNumero(linha.get("CPU")),
                converterNumero(linha.get("RAM")),
                converterNumero(linha.get("Disco")),
                converterNumero( linha.get("Processos")),
                converterNumero(linha.get("Bateria")))).append("\n");
    }
    System.out.println(datas);

    for (String pasta : datas.keySet()) {
        String conteudo = datas.get(pasta).toString();
        String diretorioDestino = pasta + "/" + "dadostrusted.csv";
        S3Upload.uploadArquivo(bucketDestino,diretorioDestino,conteudo);

    }


    System.out.println("Finalizado!");
} catch (IOException ex) {
       throw new RuntimeException(ex);
   }


}

    private static String converterNumero(String valor) {
        try {
            return String.valueOf(Double.parseDouble(valor));

        } catch (Exception e) {
            return "0";
        }


    }

    private static String padronizaData(String dataBruta) {
        try {
            DateTimeFormatter formatoOriginal = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime dateTime = LocalDateTime.parse(dataBruta, formatoOriginal);
            return dateTime.toString();
        } catch (Exception e) {
            return dataBruta;
        }
    }
}


