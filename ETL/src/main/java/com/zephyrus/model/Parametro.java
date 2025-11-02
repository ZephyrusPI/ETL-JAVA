package com.zephyrus.model;

public class Parametro {
    private String nomeHospital;
    private String numeroSerie;
    private String area;
    private Double parametroMax;
    private Double parametroMin;
    private String nomeComponente;
    private String unidadeMedida;
    private Integer idVentilador;

    public Parametro(
            String nomeHospital,
            String numeroSerie,
            String area,
            Double parametroMax,
            Double parametroMin,
            String nomeComponente,
            String unidadeMedida,
            Integer idVentilador
    ) {
        this.nomeHospital = nomeHospital;
        this.numeroSerie = numeroSerie;
        this.area = area;
        this.parametroMax = parametroMax;
        this.parametroMin = parametroMin;
        this.nomeComponente = nomeComponente;
        this.unidadeMedida = unidadeMedida;
        this.idVentilador = idVentilador;
    }

    public String getNomeHospital() {
        return nomeHospital;
    }

    public String getNumeroSerie() {
        return numeroSerie;
    }

    public String getArea() {
        return area;
    }

    public Double getParametroMax() {
        return parametroMax;
    }

    public Double getParametroMin() {
        return parametroMin;
    }

    public String getNomeComponente() {
        return nomeComponente;
    }

    public String getUnidadeMedida() {
        return unidadeMedida;
    }

    public Integer getIdVentilador() {
        return idVentilador;
    }

    public void setIdVentilador(Integer idVentilador) {
        this.idVentilador = idVentilador;
    }

    @Override
    public String toString() {
        return "Parametro{" +
                "nomeHospital='" + nomeHospital + '\'' +
                ", numeroSerie='" + numeroSerie + '\'' +
                ", area='" + area + '\'' +
                ", parametroMax=" + parametroMax +
                ", parametroMin=" + parametroMin +
                ", nomeComponente='" + nomeComponente + '\'' +
                ", unidadeMedida='" + unidadeMedida + '\'' +
                ", idVentilador=" + idVentilador +
                '}';
    }
}
