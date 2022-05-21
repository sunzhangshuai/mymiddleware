package com.chonggou.v2.price;

import com.chonggou.v2.Movie;

/**
 * NewReleasePrice:
 *
 * @author sunchen
 * @date 2021/10/30 3:30 下午
 */
public class NewReleasePrice extends Price {
    @Override
    public double getCharge(int daysRented) {
        return daysRented * 3;
    }

    @Override
    public int getPriceCode() {
        return Movie.NEW_RELEASE;
    }

    @Override
    public int getFrequentRenterPoints(int daysRented) {
        return daysRented > 1 ? 2 : 1;
    }
}