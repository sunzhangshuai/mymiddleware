package com.chonggou.v2.price;

import com.chonggou.v2.Movie;

/**
 * ChildrenPrice:
 *
 * @author sunchen
 * @date 2021/10/30 3:29 下午
 */
public class ChildrenPrice extends Price{
    @Override
    public double getCharge(int daysRented) {
        double thisAmount = 1.5;
        if (daysRented > 3) {
            thisAmount += (daysRented - 3) * 1.5;
        }
        return thisAmount;
    }

    @Override
    public int getPriceCode() {
        return Movie.CHILDRENS;
    }
}