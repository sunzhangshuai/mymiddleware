package com.chonggou.v1;

public class Movie {
    public static final int CHILDRENS = 2;
    public static final int REGULAR = 0;
    public static final int NEW_RELEASE = 1;

    private String _title;
    private int _priceCode;

    public Movie(String _title, int code) {
        this._title = _title;
        _priceCode = code;
    }

    public int getPriceCode() {
        return _priceCode;
    }

    public void setPriceCode(int code) {
        _priceCode = code;
    }

    public String get_title() {
        return _title;
    }

}