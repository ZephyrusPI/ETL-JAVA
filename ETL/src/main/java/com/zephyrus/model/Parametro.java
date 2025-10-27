package com.zephyrus.model;

import software.amazon.awssdk.services.s3.endpoints.internal.Value;

import java.time.LocalDate;

public class Parametro {
    private String nomeHospital;
    private String numeroSerie;
    private String area;
    private Double parametroMax;
    private Double parametroMin;
    private String nomeComponente;
    private String unidadeMedida;

    public Parametro(String nomeHospital, String numeroSerie, String area, Double parametroMax, Double parametroMin, String nomeComponente, String unidadeMedida) {
        this.nomeHospital = nomeHospital;
        this.numeroSerie = numeroSerie;
        this.area = area;
        this.parametroMax = parametroMax;
        this.parametroMin = parametroMin;
        this.nomeComponente = nomeComponente;
        this.unidadeMedida = unidadeMedida;
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
                '}';
    }
}
