package com.zephyrus;

import com.zephyrus.Jira.JiraService;
import com.zephyrus.S3.S3Download;
import com.zephyrus.S3.S3UploadArquivo;
import com.zephyrus.dao.ParametroDao;
import com.zephyrus.model.Parametro;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.temporal.WeekFields;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;

public class ProcessarTrusted {

    private static final HashSet<String> alertasDoDia = new HashSet<>();

    public static void processarTrustedAlertasDiarios(String nomeBucketTrusted, String nomeBucketClient) {
        List<Parametro> parametros = ParametroDao.buscarParametros();
        LocalDate hoje = LocalDate.now(java.time.ZoneId.of("America/Sao_Paulo"));
        int ano = hoje.getYear();
        int mes = hoje.getMonthValue();
        int semana = hoje.get(WeekFields.ISO.weekOfYear());
        int dia = hoje.getDayOfMonth();

        alertasDoDia.clear();

        String arquivoBuscadoTrusted = String.format("%d/%d/%d/%d/dadostrusted.csv", ano, mes, semana, dia);
        String arquivoTrustedLocal = "arquivoTrustedDia.csv";
        String arquivoClientLocal = "alertaDiario.csv";
        String arquivoClientNoBucket = "Alertas/alertaDiario.csv";

        System.out.println("Tentando baixar: " + arquivoBuscadoTrusted + " do bucket " + nomeBucketTrusted);
        S3Download.downloadArquivo(nomeBucketTrusted, arquivoBuscadoTrusted, arquivoTrustedLocal);

        File f = new File(arquivoTrustedLocal);
        if (!f.exists() || f.length() == 0) {
            System.out.println("Arquivo trusted do dia não encontrado: " + arquivoTrustedLocal + ". Nenhum alerta será gerado hoje.");
            return;
        }

        try (
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(new FileInputStream(arquivoTrustedLocal), StandardCharsets.UTF_8)
                );
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                        .withHeader("timestamp", "ID", "Modelo", "Area", "CPU", "RAM", "Disco", "Processos", "Bateria")
                        .withFirstRecordAsHeader());
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(new FileOutputStream(arquivoClientLocal), StandardCharsets.UTF_8)
                );
                CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(
                        "timestamp", "hospital", "numero_serie", "area", "componente",
                        "valor_lido", "min_permitido", "max_permitido", "tipo_alerta"))
        ) {

            for (CSVRecord record : csvParser) {
                String timestamp = record.get("timestamp");
                String idVentilador = record.get("ID");

                Map<String, Double> valores = new HashMap<>();
                valores.put("CPU", Double.valueOf(record.get("CPU") == null || record.get("CPU").trim().isEmpty() || record.get("CPU").equalsIgnoreCase("N/A") ? "0" : record.get("CPU").trim()));
                valores.put("RAM", Double.valueOf(record.get("RAM") == null || record.get("RAM").trim().isEmpty() || record.get("RAM").equalsIgnoreCase("N/A") ? "0" : record.get("RAM").trim()));
                valores.put("Disco", Double.valueOf(record.get("Disco") == null || record.get("Disco").trim().isEmpty() || record.get("Disco").equalsIgnoreCase("N/A") ? "0" : record.get("Disco").trim()));
                valores.put("Bateria", Double.valueOf(record.get("Bateria") == null || record.get("Bateria").trim().isEmpty() || record.get("Bateria").equalsIgnoreCase("N/A") ? "0" : record.get("Bateria").trim()));
                valores.put("Processos", Double.valueOf(record.get("Processos") == null || record.get("Processos").trim().isEmpty() || record.get("Processos").equalsIgnoreCase("N/A") ? "0" : record.get("Processos").trim()));

                List<Parametro> parametrosDoVentilador = parametros.stream()
                        .filter(p -> p.getIdVentilador() != null &&
                                p.getIdVentilador().equals(Integer.valueOf(idVentilador)))
                        .toList();

                for (Parametro p : parametrosDoVentilador) {
                    Double valorLido = valores.get(p.getNomeComponente());

                    if (valorLido != null) {
                        if (valorLido >= p.getParametroMax() || valorLido < p.getParametroMin()) {

                            criarClient(csvPrinter, timestamp, p.getNomeComponente(), valorLido,
                                    p.getNomeHospital(), p.getParametroMax(), p.getParametroMin(),
                                    p.getArea(), p.getNumeroSerie(), p.getUnidadeMedida());
                        }
                    }
                }
            }

            S3UploadArquivo.uploadArquivo(nomeBucketClient, arquivoClientNoBucket, arquivoClientLocal);
            System.out.println("Upload de alertas diário enviado para: " + arquivoClientNoBucket);

        } catch (Exception e) {
            System.out.println("Erro processando trusted -> client: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void processarTrustedConsumoMensal(String nomeBucketTrusted, String nomeBucketClient) {
        LocalDate hoje = LocalDate.now();
        int ano = hoje.getYear();
        int mes = hoje.getMonthValue();
        String arquivoBuscadoTrusted = String.format("%d/%d/dadosTrustedMensal.csv", ano, mes);
        String arquivoClientLocal = "arquivoClientMensal.csv";
        String arquivoClient = String.format("dadosDoMes/dadosMensal.csv");

        S3Download.downloadArquivo(nomeBucketTrusted, arquivoBuscadoTrusted, arquivoClientLocal);
        S3UploadArquivo.uploadArquivo(nomeBucketClient, arquivoClient, arquivoClientLocal);
    }

    private static void criarClient(CSVPrinter printer, String timestamp, String componente, Double valorLido,
                                    String hospital, Double valorMax, Double valorMin, String area,
                                    String numeroSerie, String unidade) {
        try {

            String data = timestamp.split("T")[0];

            String chave = numeroSerie + "|" + componente + "|" + data;
            String valorFormatado = String.format("%.2f - %s", valorLido, unidade);
            String tipoAlerta = valorLido <= valorMin ? "Abaixo" : "Acima";

            printer.printRecord(timestamp, hospital, numeroSerie, area, componente,
                    valorFormatado, valorMin, valorMax, tipoAlerta);

            printer.flush();

            if (alertasDoDia.contains(chave)) {
                System.out.println("⚠ Alerta duplicado ignorado: " + chave);
                return;
            }

            alertasDoDia.add(chave);


            try {
                JiraService.criarAlertaComUnidade(
                        valorLido, componente, unidade,
                        hospital, valorMin, valorMax, area,
                        numeroSerie, timestamp
                );
            } catch (Exception je) {
                System.out.println("Falha ao criar ticket Jira: " + je.getMessage());
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
