package com.zephyrus;
import com.zephyrus.Jira.JiraService;
import com.zephyrus.dao.ParametroDao;
import com.zephyrus.model.Parametro;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class CsvProcessador {
    private static final Logger log = LoggerFactory.getLogger(CsvProcessador.class);

    public static void processarRaw() {


        String arquivoRaw = "src\\main\\java\\com\\zephyrus\\Arquivos\\dadosRaw.csv";
        String arquivoTrusted = "src\\main\\java\\com\\zephyrus\\Arquivos\\dadosTrusted.csv";
        try (BufferedReader reader = new BufferedReader(new FileReader(arquivoRaw)); BufferedWriter writer = new BufferedWriter(new FileWriter(arquivoTrusted));
             CSVPrinter csvPrinter = new CSVPrinter(writer,
                     CSVFormat.DEFAULT.withHeader("timestamp", "ID", "Modelo", "Area", "CPU", "RAM", "Disco", "Processos", "Bateria"))) {
            //Tratamento de dados

            String linha;
            Boolean primeiraLinha = true;
            while ((linha = reader.readLine()) != null) {

                if (primeiraLinha) {

                    primeiraLinha = false;
                    continue;
                }
                linha = linha.replace("\"", "");
                String[] colunas = linha.split(",");
                if (colunas.length < 9) continue;
                String timestamp = padronizaData(colunas[0]);
                String id = colunas[1];
                String modelo = colunas[2];
                String Area = colunas[3];
                String cpu = converterNumero(colunas[4]);
                String ram = converterNumero(colunas[5]);
                String disco = converterNumero(colunas[6]);
                String processos = converterNumero(colunas[7]);
                String bateria = converterNumero(colunas[8]);

                csvPrinter.printRecord(timestamp, id, modelo, Area, cpu, ram, disco, processos, bateria);


            }
        } catch (Exception e) {
            System.out.println(e);
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

    public static void processarTrusted() {
        List<Parametro> parametros = ParametroDao.buscarParametros();
        String arquivoTrusted = "src\\main\\java\\com\\zephyrus\\Arquivos\\dadosTrusted.csv";
        String arquivoClient = "src\\main\\java\\com\\zephyrus\\Arquivos\\dadosClient.csv";

        try (
                BufferedReader reader = new BufferedReader(new FileReader(arquivoTrusted));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader("timestamp","ID","Modelo","Area","CPU","RAM","Disco","Processos","Bateria").withFirstRecordAsHeader());
                BufferedWriter writer = new BufferedWriter(new FileWriter(arquivoClient));
                CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(
                        "timestamp","hospital","numero_serie","area","componente","valor_lido","min_permitido","max_permitido","tipo_alerta"))
        ) {

            for (CSVRecord record : csvParser) {
                String timestamp=record.get("timestamp");
                String numeroSerie = record.get("ID");
                Double cpu = Double.valueOf(record.get("CPU"));
                Double ram = Double.valueOf(record.get("RAM"));
                Double disco = Double.valueOf(record.get("Disco"));
                Double bateria = Double.valueOf(record.get("Bateria"));
                verificarParametro(parametros, csvPrinter,"RAM",ram,numeroSerie,timestamp,arquivoClient);
                verificarParametro(parametros, csvPrinter,"CPU",cpu,numeroSerie,timestamp,arquivoClient);
                verificarParametro(parametros, csvPrinter,"Disco",disco,numeroSerie,timestamp,arquivoClient);
                verificarParametro(parametros, csvPrinter,"Bateria",bateria,numeroSerie,timestamp,arquivoClient);
                ;
            }

        } catch (Exception e) {
            System.out.println("Erro: " + e);
        }
    }

    public static  void verificarParametro(List<Parametro> parametros,CSVPrinter printer ,String componente, Double valorLido, String numeroSerie,String timestamp,String arquivo) {
        for (Parametro p : parametros) {
            if (p.getNumeroSerie().equals(numeroSerie) && componente.equalsIgnoreCase(p.getNomeComponente())) {
                if (valorLido >= p.getParametroMax() || valorLido < p.getParametroMin()) {
                criarClient(printer,timestamp,componente,valorLido,p.getNomeHospital(),p.getParametroMax(),p.getParametroMin(),p.getArea(),arquivo,numeroSerie);
                }
            }
        }


   }
   public static void criarClient(CSVPrinter printer,String timestamp, String componente,Double valorLido,String hospital,Double valorMax,Double valorMin,String area,String arquivo,String numeroSerie){
        try {
            String tipoAlerta=valorLido<=valorMin?"Abaixo":"Acima";

            printer.printRecord(timestamp,hospital,numeroSerie,area,componente,valorLido,valorMin,valorMax,tipoAlerta);
            printer.flush();
        }catch (Exception e){
            log.error("e: ", e);
        }
    }

}

