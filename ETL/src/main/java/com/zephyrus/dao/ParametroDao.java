package com.zephyrus.dao;

import com.zephyrus.factory.ConnectionFactory;
import com.zephyrus.model.Parametro;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ParametroDao{
    public static List<Parametro> buscarParametros(){
        List<Parametro> parametros=new ArrayList<>();
        try{
            Connection conexao= ConnectionFactory.conectar();
            String comandoBuscar="select nomeHospital,area,numero_serie,parametroMax,parametroMin,nomeComponente,unidadeMedida,nome from Hospital inner join Sala on idHospital=fkHospital inner join Ventilador on idSala=fkSala inner join Parametro on idVentilador=fkVentilador inner join Componente on fkComponente=idComponente inner join Modelo on idModelo=fkModelo;";
            Statement statement=conexao.createStatement();
            ResultSet resultados=statement.executeQuery(comandoBuscar);
            while (resultados.next()){
                Parametro p=new Parametro(resultados.getString("nomeHospital"),resultados.getString("numero_serie"),resultados.getString("area"),resultados.getDouble("parametroMax"),resultados.getDouble("parametroMin"),resultados.getString("nomeComponente"),resultados.getString("unidadeMedida"));
                System.out.println( p.toString());
                parametros.add(p);


            }
            return parametros;



        }catch(SQLException e){
            System.out.println(e);
            return null;

        }
    }

}
