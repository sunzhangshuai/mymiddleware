package com.chonggou.v2;

import com.chonggou.v2.price.ChildrenPrice;
import com.chonggou.v2.price.NewReleasePrice;
import com.chonggou.v2.price.Price;
import com.chonggou.v2.price.RegularPrice;

public class Movie {
    public static final int CHILDRENS = 2;
    public static final int REGULAR = 0;
    public static final int NEW_RELEASE = 1;

    private String _title;
    private Price price;

    public Movie(String _title, int code) {
        this._title = _title;
        setPrice(code);
    }

    public int getPriceCode() {
        return price.getPriceCode();
    }


    public String get_title() {
        return _title;
    }

    /**
     * 租碟的价格是根据碟的类型进行计算的，
     * 一般的碟：
     * 2块钱起租赁，超过两天每天1块五
     * 新碟：
     * 每天三块
     * 儿童碟：
     * 不足四天都是1.5，超过四天(包括)每天1.5
     */
    public double getCharge(int daysRented) {
        return price.getCharge(daysRented);
    }

    private void setPrice(int code) {
        switch (code) {
            case REGULAR:
                price = new RegularPrice();
            case Movie.NEW_RELEASE:
                price = new NewReleasePrice();
            case Movie.CHILDRENS:
                price = new ChildrenPrice();
        }
    }

    public int getFrequentRenterPoints(int daysRented) {
        return price.getFrequentRenterPoints(daysRented);
    }

}