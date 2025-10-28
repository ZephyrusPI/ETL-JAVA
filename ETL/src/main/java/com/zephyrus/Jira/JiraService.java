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

    public static void criarAlerta(double valor,String componente) throws Exception {
        String auth = Base64.getEncoder().encodeToString((EMAIL + ":" + API_TOKEN).getBytes());

        String json = String.format("""
        {
          "fields": {
            "project": { "key": "KAN" },
            "summary": "Alerta de %s (%.2f%%)",
            "description": {
              "type": "doc",
              "version": 1,
              "content": [
                {
                  "type": "paragraph",
                  "content": [
                    {
                      "type": "text",
                      "text": "Alerta %s atingiu %.2f%%, fora dos limites definidos."
                    }
                  ]
                }
              ]
            },
            "issuetype": { "name": "Request" }
          }
        }
        """, componente, valor, componente,valor);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(JIRA_URL))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + auth)
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();

        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString());

        System.out.println("Status: " + response.statusCode());
        System.out.println("Resposta: " + response.body());
    }

    public static void main(String[] args) throws Exception {
        criarAlerta(88.00,"RAM");
    }
}
