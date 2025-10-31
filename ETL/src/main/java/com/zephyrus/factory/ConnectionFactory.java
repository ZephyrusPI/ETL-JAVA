package com.zephyrus.factory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {
    Connection conn=null;

    public static Connection conectar(){    try{

        String url = "jdbc:mysql://localhost:3306/zephyrus";
        String senha = "Admin123";
        String usuario = "root";
        return DriverManager.getConnection(url, usuario, senha);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        }

    }

