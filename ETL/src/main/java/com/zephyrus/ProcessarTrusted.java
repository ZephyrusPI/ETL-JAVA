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

    // Cabeçalho para arquivos de ALERTA (Client)
    private static final String[] HEADER_ALERTA = {
            "timestamp", "hospital", "numero_serie", "area", "componente",
            "valor_lido", "min_permitido", "max_permitido", "tipo_alerta"
    };

    // Cabeçalho para arquivos de DADOS BRUTOS (Trusted)
    private static final String[] HEADER_TRUSTED = {
            "timestamp", "ID", "Modelo", "Area", "CPU", "RAM", "Disco", "Processos", "Bateria"
    };

    // Mapa para gerenciar os escritores de arquivos temporários
    // Chave: Caminho no S3 -> Valor: Caminho local no /tmp/
    private static final Map<String, String> arquivosParaUpload = new HashMap<>();
    private static final Map<String, BufferedWriter> escritoresAbertos = new HashMap<>();
    private static final Map<String, CSVPrinter> printersAbertos = new HashMap<>();

    public static void processarTrustedAlertasDiarios(String nomeBucketOrigem, String nomeBucketClient, String nomeArquivoInput) {
        System.out.println("--- INICIANDO DISTRIBUIÇÃO HIERÁRQUICA: " + nomeArquivoInput + " ---");

        String nomeBucketTrusted = System.getenv("BUCKET_TRUSTED");
        List<Parametro> parametros = ParametroDao.buscarParametros();
        LocalDate hoje = LocalDate.now(java.time.ZoneId.of("America/Sao_Paulo"));

        alertasDoDia.clear();
        limparMapas(); // Limpa mapas estáticos para evitar lixo de execuções anteriores (no mesmo container)

        // 1. Download
        String arquivoInputLocal = TEMP_DIR + "input_" + System.nanoTime() + ".csv";
        S3Download.downloadArquivo(nomeBucketOrigem, nomeArquivoInput, arquivoInputLocal);

        File fInput = new File(arquivoInputLocal);
        if (!fInput.exists() || fInput.length() == 0) {
            System.out.println("Arquivo vazio. Abortando.");
            return;
        }

        // 2. Leitura e Distribuição Linha a Linha
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(arquivoInputLocal), StandardCharsets.UTF_8));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader(HEADER_TRUSTED).withFirstRecordAsHeader())) {

            for (CSVRecord record : csvParser) {
                String timestampStr = record.get("timestamp");

                // Parse da Data por linha
                LocalDate dataReg;
                try {
                    String dataApenas = timestampStr.split(" ")[0];
                    dataReg = LocalDate.parse(dataApenas);
                } catch (Exception e) {
                    continue; // Pula linha com data inválida
                }

                // Calcula os componentes da data
                int ano = dataReg.getYear();
                int mes = dataReg.getMonthValue();
                int semana = dataReg.get(WeekFields.ISO.weekOfYear());
                int dia = dataReg.getDayOfMonth();
                String nomeMes = Month.of(mes).getDisplayName(TextStyle.FULL, new Locale("pt", "BR"));
                nomeMes = nomeMes.substring(0, 1).toUpperCase() + nomeMes.substring(1);

                // --- PREPARAÇÃO DOS DADOS ---

                // Dados para o Trusted (Cópia da linha original)
                Object[] dadosTrusted = {
                        record.get("timestamp"), record.get("ID"), record.get("Modelo"), record.get("Area"),
                        record.get("CPU"), record.get("RAM"), record.get("Disco"), record.get("Processos"), record.get("Bateria")
                };

                // --- ESCRITA NO TRUSTED (4 NÍVEIS) ---
                if (nomeBucketTrusted != null) {
                    // 1. Ano
                    adicionarAoBuffer(nomeBucketTrusted, String.format("%d/coletaAnual.csv", ano), dadosTrusted, HEADER_TRUSTED);
                    // 2. Mês
                    adicionarAoBuffer(nomeBucketTrusted, String.format("%d/%02d/coletaMensal.csv", ano, mes), dadosTrusted, HEADER_TRUSTED);
                    // 3. Semana
                    adicionarAoBuffer(nomeBucketTrusted, String.format("%d/%02d/Semana%d/coletaSemanal.csv", ano, mes, semana), dadosTrusted, HEADER_TRUSTED);
                    // 4. Dia
                    adicionarAoBuffer(nomeBucketTrusted, String.format("%d/%02d/Semana%d/%02d/coletaDiaria.csv", ano, mes, semana, dia), dadosTrusted, HEADER_TRUSTED);
                }

                // --- LÓGICA DE ALERTA (CLIENT) ---
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
                    if (valor != null && (valor >= p.getParametroMax() || valor < p.getParametroMin())) {
                        String valFmt = String.format("%.2f - %s", valor, p.getUnidadeMedida());
                        String tipo = valor <= p.getParametroMin() ? "Abaixo" : "Acima";

                        Object[] dadosAlerta = {
                                timestampStr, p.getNomeHospital(), p.getNumeroSerie(), p.getArea(), p.getNomeComponente(),
                                valFmt, String.valueOf(p.getParametroMin()), String.valueOf(p.getParametroMax()), tipo
                        };

                        // --- ESCRITA NO CLIENT (5 ARQUIVOS) ---

                        // Fixo (Diário) - Apenas alertaDiario.csv
                        adicionarAoBuffer(nomeBucketClient, "Alertas/alertaDiario.csv", dadosAlerta, HEADER_ALERTA);

                        // Hierarquia Completa (Ano -> Mês -> Semana -> Dia)
                        adicionarAoBuffer(nomeBucketClient, String.format("AlertasHistorico/%d/alertasDoAno.csv", ano), dadosAlerta, HEADER_ALERTA);
                        adicionarAoBuffer(nomeBucketClient, String.format("AlertasHistorico/%d/%02d/alertasDo%s.csv", ano, mes, nomeMes), dadosAlerta, HEADER_ALERTA);
                        adicionarAoBuffer(nomeBucketClient, String.format("AlertasHistorico/%d/%02d/Semana%d/alertaDaSemana.csv", ano, mes, semana), dadosAlerta, HEADER_ALERTA);
                        adicionarAoBuffer(nomeBucketClient, String.format("AlertasHistorico/%d/%02d/Semana%d/%02d/alertaDoDia.csv", ano, mes, semana, dia), dadosAlerta, HEADER_ALERTA);

                        // Jira (Só hoje)
                        if (dataReg.equals(hoje)) {
                            String chaveJira = p.getNumeroSerie() + "|" + p.getNomeComponente() + "|" + timestampStr;
                            if (!alertasDoDia.contains(chaveJira)) {
                                alertasDoDia.add(chaveJira);
                                try {
                                    JiraService.criarAlertaComUnidade(valor, p.getNomeComponente(), p.getUnidadeMedida(),
                                            p.getNomeHospital(), p.getParametroMin(), p.getParametroMax(), p.getArea(), p.getNumeroSerie(), timestampStr);
                                } catch (Exception je) {}
                            }
                        }
                    }
                }
            }
        } catch (Exception e) { e.printStackTrace(); }
        finally {
            fecharTodosEscritores();
        }

        // 3. Fase de Sincronização (Iterar Mapas e Enviar)
        System.out.println("Iniciando sincronização de " + arquivosParaUpload.size() + " arquivos...");

        for (Map.Entry<String, String> entry : arquivosParaUpload.entrySet()) {
            String s3Key = entry.getKey();     // ex: 2025/coletaAnual.csv
            String localTemp = entry.getValue(); // ex: /tmp/temp_123.csv

            // Define qual bucket usar
            String targetBucket = s3Key.startsWith("Alertas") ? nomeBucketClient : nomeBucketTrusted;

            if (targetBucket.equals(nomeBucketClient) && s3Key.equals("Alertas/alertaDiario.csv")) {
                // Arquivo Fixo: Sobrescreve
                S3UploadArquivo.uploadArquivo(targetBucket, s3Key, localTemp);
            } else {
                // Arquivos Históricos: Bola de Neve (Merge)
                // Se é Alerta ou Trusted define o header para o merge
                String[] headerCorreto = s3Key.startsWith("Alertas") ? HEADER_ALERTA : HEADER_TRUSTED;
                mesclarEEnviar(targetBucket, s3Key, localTemp, headerCorreto);
            }
        }

        System.out.println("Processo finalizado.");
    }

    // --- MÉTODOS AUXILIARES ---

    private static void adicionarAoBuffer(String bucket, String s3Key, Object[] dados, String[] header) {
        try {
            // Chave única do mapa é o s3Key (caminho relativo)
            // Se a chave não existir no mapa, cria um novo arquivo temporário e abre o escritor
            if (!printersAbertos.containsKey(s3Key)) {
                String localPath = TEMP_DIR + "buffer_" + Math.abs(s3Key.hashCode()) + "_" + System.nanoTime() + ".csv";
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(localPath), StandardCharsets.UTF_8));
                CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(header));

                escritoresAbertos.put(s3Key, writer);
                printersAbertos.put(s3Key, printer);
                arquivosParaUpload.put(s3Key, localPath);
            }

            // CORREÇÃO DO ERRO DE CASTING AQUI (Manual Array Copy)
            // Converte Object[] para String[] manualmente para evitar ClassCastException
            String[] dadosStr = new String[dados.length];
            for (int i = 0; i < dados.length; i++) {
                dadosStr[i] = String.valueOf(dados[i]);
            }

            printersAbertos.get(s3Key).printRecord((Object[]) dadosStr);

        } catch (IOException e) {
            System.out.println("Erro ao escrever no buffer: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void fecharTodosEscritores() {
        for (CSVPrinter p : printersAbertos.values()) {
            try { p.flush(); p.close(); } catch (Exception e) {}
        }
        for (BufferedWriter w : escritoresAbertos.values()) {
            try { w.flush(); w.close(); } catch (Exception e) {}
        }
        // Não limpamos 'arquivosParaUpload' aqui pois precisamos iterar sobre ele depois para o upload
        printersAbertos.clear();
        escritoresAbertos.clear();
    }

    private static void limparMapas() {
        arquivosParaUpload.clear();
        escritoresAbertos.clear();
        printersAbertos.clear();
    }

    private static void mesclarEEnviar(String bucket, String key, String arquivoNovoLocal, String[] header) {
        String arquivoAntigoLocal = TEMP_DIR + "old_" + System.nanoTime() + ".csv";
        String arquivoFinalLocal = TEMP_DIR + "final_" + System.nanoTime() + ".csv";
        boolean existeAntigo = false;
        Set<String> chavesUnicas = new HashSet<>();

        boolean isAlerta = (header.length == HEADER_ALERTA.length);

        // 1. Tenta baixar o existente (se não existir, catch captura erro 404 e segue)
        try {
            S3Download.downloadArquivo(bucket, key, arquivoAntigoLocal);
            if (new File(arquivoAntigoLocal).exists()) existeAntigo = true;
        } catch (Exception e) {}

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(arquivoFinalLocal), StandardCharsets.UTF_8));
             CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(header))) {

            // Copia antigo
            if (existeAntigo) {
                try (Reader r = new FileReader(arquivoAntigoLocal)) {
                    Iterable<CSVRecord> recs = CSVFormat.DEFAULT.withHeader(header).withFirstRecordAsHeader().parse(r);
                    for (CSVRecord rc : recs) {
                        String k = gerarChaveUnica(rc, isAlerta);
                        if(chavesUnicas.add(k)) {
                            Object[] vals = new Object[rc.size()];
                            for(int i=0; i<rc.size(); i++) vals[i] = rc.get(i);
                            printer.printRecord(vals);
                        }
                    }
                }
            }

            // Copia novo (do arquivo temporário bufferizado)
            try (Reader r = new FileReader(arquivoNovoLocal)) {
                Iterable<CSVRecord> recs = CSVFormat.DEFAULT.withHeader(header).withFirstRecordAsHeader().parse(r);
                for (CSVRecord rc : recs) {
                    String k = gerarChaveUnica(rc, isAlerta);
                    if(chavesUnicas.add(k)) {
                        Object[] vals = new Object[rc.size()];
                        for(int i=0; i<rc.size(); i++) vals[i] = rc.get(i);
                        printer.printRecord(vals);
                    }
                }
            }

            printer.flush(); writer.flush();
            System.out.println("Upload MERGE: " + key);
            S3UploadArquivo.uploadArquivo(bucket, key, arquivoFinalLocal);

        } catch (Exception e) { e.printStackTrace(); }
    }

    private static String gerarChaveUnica(CSVRecord rc, boolean isAlerta) {
        if (isAlerta) return rc.get(0) + "|" + rc.get(2) + "|" + rc.get(4); // Time|Serie|Comp
        else return rc.get(0) + "|" + rc.get(1); // Time|ID
    }

    public static void processarTrustedConsumoMensal(String bucketT, String bucketC) {}

    private static Double converterDouble(String v) { try { return Double.valueOf(v.trim()); } catch (Exception e) { return 0.0; } }
    private static Integer converterInt(String v) { try { return Integer.valueOf(v.trim()); } catch (Exception e) { return 0; } }
}