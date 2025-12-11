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
import java.time.Month;
import java.time.format.TextStyle;
import java.time.temporal.WeekFields;
import java.util.*;

public class ProcessarTrusted {

    private static final HashSet<String> alertasDoDia = new HashSet<>();
    private static final String TEMP_DIR = "/tmp/";

    // HEADER_ENRIQUECIDO: Usado para TODOS os arquivos do Client (Alertas, OKs e Geral)
    // Contém as informações traduzidas (Hospital, Serial) + Status
    private static final String[] HEADER_ENRIQUECIDO = {
            "timestamp", "hospital", "numero_serie", "area", "componente",
            "valor_lido", "min_permitido", "max_permitido", "tipo_alerta"
    };

    // HEADER_RAW: Usado apenas para o backup bruto no Trusted
    private static final String[] HEADER_RAW = {
            "timestamp", "ID", "Modelo", "Area", "CPU", "RAM", "Disco", "Processos", "Bateria"
    };

    // Mapas de Buffer
    private static final Map<String, List<String[]>> bufferTrusted = new HashMap<>();
    private static final Map<String, List<String[]>> bufferClient = new HashMap<>();

    public static void processarTrustedAlertasDiarios(String nomeBucketOrigem, String nomeBucketClient, String nomeArquivoInput) {
        System.out.println("--- INICIANDO PROCESSAMENTO FINAL: " + nomeArquivoInput + " ---");

        String nomeBucketTrusted = System.getenv("BUCKET_TRUSTED");
        List<Parametro> parametros = ParametroDao.buscarParametros();
        LocalDate hoje = LocalDate.now(java.time.ZoneId.of("America/Sao_Paulo"));

        alertasDoDia.clear();
        limparMapas();

        // 1. Download
        String arquivoInputLocal = TEMP_DIR + "input_" + System.nanoTime() + ".csv";
        S3Download.downloadArquivo(nomeBucketOrigem, nomeArquivoInput, arquivoInputLocal);

        File fInput = new File(arquivoInputLocal);
        if (!fInput.exists() || fInput.length() == 0) {
            System.out.println("Arquivo vazio. Abortando.");
            deletarArquivo(fInput);
            return;
        }

        // 2. Leitura
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(arquivoInputLocal), StandardCharsets.UTF_8));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader(HEADER_RAW).withFirstRecordAsHeader())) {

            for (CSVRecord record : csvParser) {
                String timestampStr = record.get("timestamp");

                // Parse Data
                LocalDate dataReg;
                try {
                    String dataApenas = timestampStr.split(" ")[0];
                    dataReg = LocalDate.parse(dataApenas);
                } catch (Exception e) { continue; }

                int ano = dataReg.getYear();
                int mes = dataReg.getMonthValue();
                int semana = dataReg.get(WeekFields.ISO.weekOfYear());
                int dia = dataReg.getDayOfMonth();
                String nomeMes = Month.of(mes).getDisplayName(TextStyle.FULL, new Locale("pt", "BR"));
                nomeMes = nomeMes.substring(0, 1).toUpperCase() + nomeMes.substring(1);

                // --- A) TRUSTED (BACKUP BRUTO - TUDO) ---
                if (nomeBucketTrusted != null) {
                    Object[] dadosBrutos = {
                            record.get("timestamp"), record.get("ID"), record.get("Modelo"), record.get("Area"),
                            record.get("CPU"), record.get("RAM"), record.get("Disco"), record.get("Processos"), record.get("Bateria")
                    };
                    // Salva a hierarquia completa dos dados brutos
                    adicionarAoBuffer(bufferTrusted, String.format("%d/coletaAnual.csv", ano), dadosBrutos, HEADER_RAW);
                    adicionarAoBuffer(bufferTrusted, String.format("%d/%02d/coletaMensal.csv", ano, mes), dadosBrutos, HEADER_RAW);
                    adicionarAoBuffer(bufferTrusted, String.format("%d/%02d/Semana%d/coletaSemanal.csv", ano, mes, semana), dadosBrutos, HEADER_RAW);
                    adicionarAoBuffer(bufferTrusted, String.format("%d/%02d/Semana%d/%02d/coletaDiaria.csv", ano, mes, semana, dia), dadosBrutos, HEADER_RAW);
                }

                // --- B) CLIENT (ENRIQUECIDO E SEGREGADO) ---
                Integer idVentilador = converterInt(record.get("ID"));
                Map<String, Double> valores = new HashMap<>();
                valores.put("CPU", converterDouble(record.get("CPU")));
                valores.put("RAM", converterDouble(record.get("RAM")));
                valores.put("Disco", converterDouble(record.get("Disco")));
                valores.put("Bateria", converterDouble(record.get("Bateria")));
                valores.put("Processos", converterDouble(record.get("Processos")));

                List<Parametro> params = parametros.stream().filter(p -> p.getIdVentilador() != null && p.getIdVentilador().equals(idVentilador)).toList();

                for (Parametro p : params) {
                    Double valor = valores.get(p.getNomeComponente());

                    if (valor != null) {
                        boolean isAlerta = false;
                        String status = "OK";

                        if (valor >= p.getParametroMax()) { status = "Acima"; isAlerta = true; }
                        else if (valor < p.getParametroMin()) { status = "Abaixo"; isAlerta = true; }

                        // Cria linha enriquecida (Hospital, Serial, etc)
                        String valFmt = String.format("%.2f - %s", valor, p.getUnidadeMedida());
                        Object[] dadosEnriquecidos = {
                                timestampStr, p.getNomeHospital(), p.getNumeroSerie(), p.getArea(), p.getNomeComponente(),
                                valFmt, String.valueOf(p.getParametroMin()), String.valueOf(p.getParametroMax()), status
                        };

                        // 1. RESUMÃO DO MÊS ATUAL (coletasDoMes.csv)
                        // Contém TUDO (OK + Alertas) se for do mês/ano atual
                        if (dataReg.getYear() == hoje.getYear() && dataReg.getMonthValue() == hoje.getMonthValue()) {
                            adicionarAoBuffer(bufferClient, "coletasDoMes.csv", dadosEnriquecidos, HEADER_ENRIQUECIDO);
                        }

                        // 2. SEGREGAÇÃO (AlertasHistorico vs ColetasHistorico)

                        if (isAlerta) {
                            // --- É UM PROBLEMA ---

                            // A) Alerta Fixo (Só hoje)
                            if (dataReg.equals(hoje)) {
                                adicionarAoBuffer(bufferClient, "Alertas/alertaDiario.csv", dadosEnriquecidos, HEADER_ENRIQUECIDO);
                                // Jira
                                String chaveJira = p.getNumeroSerie() + "|" + p.getNomeComponente() + "|" + timestampStr;
                                if (!alertasDoDia.contains(chaveJira)) {
                                    alertasDoDia.add(chaveJira);
                                    try {
                                        JiraService.criarAlertaComUnidade(valor, p.getNomeComponente(), p.getUnidadeMedida(),
                                                p.getNomeHospital(), p.getParametroMin(), p.getParametroMax(), p.getArea(), p.getNumeroSerie(), timestampStr);
                                    } catch (Exception je) {}
                                }
                            }

                            // B) Histórico de ALERTAS
                            adicionarAoBuffer(bufferClient, String.format("AlertasHistorico/%d/alertasDoAno.csv", ano), dadosEnriquecidos, HEADER_ENRIQUECIDO);
                            adicionarAoBuffer(bufferClient, String.format("AlertasHistorico/%d/%02d/alertasDo%s.csv", ano, mes, nomeMes), dadosEnriquecidos, HEADER_ENRIQUECIDO);
                            adicionarAoBuffer(bufferClient, String.format("AlertasHistorico/%d/%02d/Semana%d/alertaDaSemana.csv", ano, mes, semana), dadosEnriquecidos, HEADER_ENRIQUECIDO);
                            adicionarAoBuffer(bufferClient, String.format("AlertasHistorico/%d/%02d/Semana%d/%02d/alertaDoDia.csv", ano, mes, semana, dia), dadosEnriquecidos, HEADER_ENRIQUECIDO);

                        } else {
                            // --- É SAUDÁVEL (OK) ---

                            // Histórico de COLETAS OK (Só os bons)
                            adicionarAoBuffer(bufferClient, String.format("ColetasHistorico/%d/coletaAnual.csv", ano), dadosEnriquecidos, HEADER_ENRIQUECIDO);
                            adicionarAoBuffer(bufferClient, String.format("ColetasHistorico/%d/%02d/coletaMensal.csv", ano, mes), dadosEnriquecidos, HEADER_ENRIQUECIDO);
                            adicionarAoBuffer(bufferClient, String.format("ColetasHistorico/%d/%02d/Semana%d/coletaSemanal.csv", ano, mes, semana), dadosEnriquecidos, HEADER_ENRIQUECIDO);
                            adicionarAoBuffer(bufferClient, String.format("ColetasHistorico/%d/%02d/Semana%d/%02d/coletaDiaria.csv", ano, mes, semana, dia), dadosEnriquecidos, HEADER_ENRIQUECIDO);
                        }
                    }
                }
            }
        } catch (Exception e) { e.printStackTrace(); }

        deletarArquivo(fInput);

        // 3. Sincronização Final

        // Trusted (Raw)
        if (nomeBucketTrusted != null) {
            System.out.println("Sincronizando Trusted...");
            for (Map.Entry<String, List<String[]>> entry : bufferTrusted.entrySet()) {
                atualizarArquivoAcumulativo(nomeBucketTrusted, entry.getKey(), entry.getValue(), HEADER_RAW);
            }
        }
        bufferTrusted.clear();

        // Client (Alertas + ColetasOK + GeralMes)
        System.out.println("Sincronizando Client...");
        for (Map.Entry<String, List<String[]>> entry : bufferClient.entrySet()) {
            if (entry.getKey().equals("Alertas/alertaDiario.csv")) {
                gerarArquivoSimples(nomeBucketClient, entry.getKey(), entry.getValue(), HEADER_ENRIQUECIDO);
            } else {
                atualizarArquivoAcumulativo(nomeBucketClient, entry.getKey(), entry.getValue(), HEADER_ENRIQUECIDO);
            }
        }
        bufferClient.clear();

        System.out.println("Processo finalizado.");
    }

    // --- MÉTODOS AUXILIARES ---

    // Mapas para os buffers
    private static final Map<String, BufferedWriter> escritoresAbertos = new HashMap<>();
    private static final Map<String, CSVPrinter> printersAbertos = new HashMap<>();
    private static final Map<String, String> arquivosParaUpload = new HashMap<>();

    private static void adicionarAoBuffer(Map<String, List<String[]>> mapa, String s3Key, Object[] dados, String[] header) {
        mapa.putIfAbsent(s3Key, new ArrayList<>());
        String[] dadosStr = new String[dados.length];
        for (int i = 0; i < dados.length; i++) dadosStr[i] = String.valueOf(dados[i]);
        mapa.get(s3Key).add(dadosStr);
    }

    private static void limparMapas() {
        bufferTrusted.clear();
        bufferClient.clear();
        escritoresAbertos.clear();
        printersAbertos.clear();
        arquivosParaUpload.clear();
    }

    private static void gerarArquivoSimples(String bucket, String key, List<String[]> dados, String[] header) {
        String path = TEMP_DIR + "temp_" + System.nanoTime() + ".csv";
        try (BufferedWriter w = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8));
             CSVPrinter p = new CSVPrinter(w, CSVFormat.DEFAULT.withHeader(header))) {
            for (String[] l : dados) p.printRecord((Object[]) l);
            p.flush(); w.flush();
            S3UploadArquivo.uploadArquivo(bucket, key, path);
        } catch (IOException e) { e.printStackTrace(); }
        deletarArquivo(new File(path));
    }

    private static void atualizarArquivoAcumulativo(String bucket, String key, List<String[]> novos, String[] header) {
        String down = TEMP_DIR + "d_" + System.nanoTime() + ".csv";
        String up = TEMP_DIR + "u_" + System.nanoTime() + ".csv";
        boolean existeAntigo = false;
        Set<String> chavesUnicas = new HashSet<>();

        // Identifica qual chave usar para deduplicação
        boolean isEnriquecido = (header.length == HEADER_ENRIQUECIDO.length);

        try {
            S3Download.downloadArquivo(bucket, key, down);
            if (new File(down).exists() && new File(down).length() > 0) existeAntigo = true;
        } catch (Exception e) {}

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(up), StandardCharsets.UTF_8));
             CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(header))) {

            if (existeAntigo) {
                try (Reader r = new FileReader(down)) {
                    Iterable<CSVRecord> recs = CSVFormat.DEFAULT.withHeader(header).withFirstRecordAsHeader().parse(r);
                    for (CSVRecord rc : recs) {
                        String k = gerarChaveUnica(rc, isEnriquecido);
                        if(chavesUnicas.add(k)) {
                            Object[] vals = new Object[rc.size()];
                            for(int i=0; i<rc.size(); i++) vals[i] = rc.get(i);
                            printer.printRecord(vals);
                        }
                    }
                }
            }

            for (String[] l : novos) {
                String k;
                if (isEnriquecido) k = l[0] + "|" + l[2] + "|" + l[4];
                else k = l[0] + "|" + l[1];

                if (!chavesUnicas.contains(k)) {
                    printer.printRecord((Object[]) l);
                    chavesUnicas.add(k);
                }
            }
            printer.flush(); writer.flush();
            S3UploadArquivo.uploadArquivo(bucket, key, up);

        } catch (IOException e) { e.printStackTrace(); }

        deletarArquivo(new File(down));
        deletarArquivo(new File(up));
    }

    private static String gerarChaveUnica(CSVRecord rc, boolean isEnriquecido) {
        if (isEnriquecido) return rc.get(0) + "|" + rc.get(2) + "|" + rc.get(4);
        else return rc.get(0) + "|" + rc.get(1);
    }

    private static void deletarArquivo(File f) {
        if (f != null && f.exists()) f.delete();
    }

    // Fecha recursos do Java (não usado nessa versão de buffer em memória, mas mantido por segurança)
    private static void fecharTodosEscritores() {}

    public static void processarTrustedConsumoMensal(String bucketT, String bucketC) {}
    private static Double converterDouble(String v) { try { return Double.valueOf(v.trim()); } catch (Exception e) { return 0.0; } }
    private static Integer converterInt(String v) { try { return Integer.valueOf(v.trim()); } catch (Exception e) { return 0; } }
}