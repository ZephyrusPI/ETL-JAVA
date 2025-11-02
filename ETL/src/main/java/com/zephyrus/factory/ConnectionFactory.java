package com.zephyrus.factory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {
    public static Connection conectar() {
        try {
            String url = "jdbc:mysql://db-zephyrus:3306/zephyrus?useSSL=false&allowPublicKeyRetrieval=true&useUnicode=true&characterEncoding=UTF-8";
            String usuario = "root";
            String senha = "senha123";
            return DriverManager.getConnection(url, usuario, senha);
        } catch (SQLException e) {
            throw new RuntimeException("Erro ao conectar ao banco: " + e.getMessage(), e);
        }
    }
}
