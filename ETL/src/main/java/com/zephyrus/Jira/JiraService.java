package com.zephyrus.Jira;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

public class JiraService {

    private static final String JIRA_URL = "https://zephyrus2g1.atlassian.net/rest/api/3/issue";
    private static final String EMAIL = "zephyrus2g@gmail.com";
    private static final String API_TOKEN = "ATATT3xFfGF0GOi6uWYw_qixBqzwKjmPoTrQYdk4ZSPEI2uX_pYxG8eM4_zO7v85j1qDJhMRQXiVHARCmstFbhxKTrgwmz2gUanGMo4mcWLd5Noz8aRpleClehtSb3dy7T8W4Kf-bG0FWBocnFiq3xtyEGxISlHegXsh5qgGWJUdwJCO5N2kf_8=F5667C11";

    public static void criarAlertaComUnidade(
            double valor, String componente, String unidade,
            String hospital, double min, double max, String area,
            String numeroSerie, String timestamp
    ) throws Exception {

        String resumo = String.format("Alerta de %s (%.2f - %s)", componente, valor, unidade);

        String auth = Base64.getEncoder().encodeToString((EMAIL + ":" + API_TOKEN).getBytes());

        String json = String.format("""
        {
          "fields": {
            "project": { "key": "KAN" },
            "summary": "%s",
            "description": {
              "type": "doc",
              "version": 1,
              "content": [
                {
                  "type": "paragraph",
                  "content": [
                    { "type": "text", "text": "Hospital: %s" }
                  ]
                },
                {
                  "type": "paragraph",
                  "content": [
                    { "type": "text", "text": "Área: %s" }
                  ]
                },
                {
                  "type": "paragraph",
                  "content": [
                    { "type": "text", "text": "Número de série: %s" }
                  ]
                },
                {
                  "type": "paragraph",
                  "content": [
                    { "type": "text", "text": "Timestamp: %s" }
                  ]
                },
                {
                  "type": "paragraph",
                  "content": [
                    { "type": "text", "text": "Componente: %s" }
                  ]
                },
                {
                  "type": "paragraph",
                  "content": [
                    { "type": "text", "text": "Valor lido: %.2f %s" }
                  ]
                },
                {
                  "type": "paragraph",
                  "content": [
                    { "type": "text", "text": "Mínimo permitido: %.2f" }
                  ]
                },
                {
                  "type": "paragraph",
                  "content": [
                    { "type": "text", "text": "Máximo permitido: %.2f" }
                  ]
                }
              ]
            },
            "issuetype": { "name": "Request" }
          }
        }
        """,
                resumo, hospital, area, numeroSerie, timestamp,
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
        System.out.println("Resposta: " + response.body());
    }
}
