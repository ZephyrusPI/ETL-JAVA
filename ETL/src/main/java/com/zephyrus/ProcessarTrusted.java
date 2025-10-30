package com.zephyrus;

import com.zephyrus.Jira.JiraService;
import com.zephyrus.S3.S3Download;
import com.zephyrus.dao.ParametroDao;
import com.zephyrus.model.Parametro;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.time.LocalDate;
import java.time.temporal.WeekFields;
import java.util.List;

public class ProcessarTrusted {
    public static void processarTrustedAlertas(String nomeBucket) {
        List<Parametro> parametros = ParametroDao.buscarParametros();
        LocalDate ultimaSemana= LocalDate.now().minusDays(15);
        int ano=ultimaSemana.getYear();
        int mes=ultimaSemana.getMonthValue();
        int semana=ultimaSemana.get(WeekFields.ISO.weekOfMonth());
String arquivoTrusted=ano+"/"+mes+"/"+semana+"/"+"dadostrusted.csv";
String arquivoClient="C:\\Users\\isafa\\Documents\\ETL-JAVA\\ETL\\src\\main\\java\\com\\zephyrus\\Arquivos\\dadosAlertasUltimaSemanaClient.csv";
        String caminhoLocal = "C:\\Users\\isafa\\Documents\\ETL-JAVA\\ETL\\src\\main\\java\\com\\zephyrus\\Arquivos\\arquivoTrustedSemana.csv";

        S3Download.downloadArquivo(nomeBucket,arquivoTrusted,caminhoLocal);

        try (
                BufferedReader reader = new BufferedReader(new FileReader(caminhoLocal));
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
            JiraService.criarAlerta(valorLido,componente);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
 }

}

