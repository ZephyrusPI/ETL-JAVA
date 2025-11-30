package com.zephyrus.Jira;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

public class JiraService {

    private static final String JIRA_URL = "https://zephyrus2g1.atlassian.net/rest/api/3/issue";
    private static final String EMAIL = "zephyrus2g@gmail.com";

    // novamente: mudando para tokens configurados na Lambda para melhor segurança.
    private static final String API_TOKEN = System.getenv("JIRA_TOKEN");

    public static void criarAlertaComUnidade(
            double valor, String componente, String unidade,
            String hospital, double min, double max, String area,
            String numeroSerie, String timestamp
    ) throws Exception {
        //Adicionando uma exception para não nos perdemos se der erro
        if (API_TOKEN == null || API_TOKEN.isEmpty()) {
            System.out.println("ERRO: JIRA_TOKEN não configurado nas variáveis de ambiente!");
            return;
        }

        String prioridade;
        if (valor <= max * 1.1 && valor >= min * 0.9) {
            prioridade = "Low";
        } else if (valor <= max * 1.2 && valor >= min * 0.80) {
            prioridade = "Medium";
        } else {
            prioridade = "High";
        }

        String resumo = String.format("Alerta de %s (%.2f - %s)", componente, valor, unidade);
        String auth = Base64.getEncoder().encodeToString((EMAIL + ":" + API_TOKEN).getBytes());

        // Sem mudanças
        String json = String.format("""
        {
          "fields": {
            "project": { "key": "KAN" },
            "priority": { "name": "%s" },
            "summary": "%s",
            "customfield_10091":"%s",
            "customfield_10092":"%s",
            "description": {
              "type": "doc",
              "version": 1,
              "content": [
                { "type": "paragraph", "content": [{ "type": "text", "text": "Hospital: %s" }] },
                { "type": "paragraph", "content": [{ "type": "text", "text": "Área: %s" }] },
                { "type": "paragraph", "content": [{ "type": "text", "text": "Número de série: %s" }] },
                { "type": "paragraph", "content": [{ "type": "text", "text": "Timestamp: %s" }] },
                { "type": "paragraph", "content": [{ "type": "text", "text": "Componente: %s" }] },
                { "type": "paragraph", "content": [{ "type": "text", "text": "Valor lido: %.2f %s" }] },
                { "type": "paragraph", "content": [{ "type": "text", "text": "Mínimo permitido: %.2f" }] },
                { "type": "paragraph", "content": [{ "type": "text", "text": "Máximo permitido: %.2f" }] }
              ]
            },
            "issuetype": { "name": "Request" }
          }
        }
        """,
                prioridade, resumo, hospital, area, hospital, area, numeroSerie, timestamp,
                componente, valor, unidade, min, max
        );

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(JIRA_URL))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + auth)
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString());

        System.out.println("Status Jira: " + response.statusCode());
    }
}