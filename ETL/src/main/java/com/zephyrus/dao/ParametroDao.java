    package com.zephyrus.dao;

    import com.zephyrus.factory.ConnectionFactory;
    import com.zephyrus.model.Parametro;

    import java.sql.Connection;
    import java.sql.ResultSet;
    import java.sql.SQLException;
    import java.sql.Statement;
    import java.util.ArrayList;
    import java.util.List;

    public class ParametroDao {

        public static List<Parametro> buscarParametros() {

            List<Parametro> parametros = new ArrayList<>();

            String sql = """
                 select 
                    h.nomeHospital, 
                    s.area, 
                    v.idVentilador,
                    v.numero_serie,
                    p.parametroMax, 
                    p.parametroMin,
                    c.nomeComponente, 
                    c.unidadeMedida
                from Hospital h
                join Sala s           on h.idHospital = s.fkHospital
                join Ventilador v     on s.idSala     = v.fkSala
                join Parametro p      on v.idVentilador = p.fkVentilador
                join Componente c     on p.fkComponente = c.idComponente;
                """;

            try (Connection conn = ConnectionFactory.conectar();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {

                while (rs.next()) {
                    Parametro parametro = new Parametro(
                            rs.getString("nomeHospital"),
                            rs.getString("numero_serie"),
                            rs.getString("area"),
                            rs.getDouble("parametroMax"),
                            rs.getDouble("parametroMin"),
                            rs.getString("nomeComponente"),
                            rs.getString("unidadeMedida"),
                            rs.getInt("idVentilador")
                    );

                    parametros.add(parametro);
                }

            } catch (SQLException e) {
                System.out.println("Erro SQL → " + e.getMessage());
            }

            return parametros;
        }
    }
