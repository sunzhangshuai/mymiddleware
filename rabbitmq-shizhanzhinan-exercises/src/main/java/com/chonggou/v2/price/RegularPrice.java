package com.chonggou.v2.price;

import com.chonggou.v2.Movie;

/**
 * RegularPrice:
 *
 * @author sunchen
 * @date 2021/10/30 3:29 下午
 */
public class RegularPrice extends Price{
    @Override
    public double getCharge(int daysRented) {
        double thisAmount = 0;
        thisAmount += 2;
        if (daysRented > 2) {
            thisAmount += (daysRented - 2) * 1.5;
        }
        return thisAmount;
    }

    @Override
    public int getPriceCode() {
        return Movie.REGULAR;
    }
}