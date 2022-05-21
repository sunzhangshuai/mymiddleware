package com.chonggou.v2.price;

/**
 * Price:
 *
 * @author sunchen
 * @date 2021/10/30 3:29 下午
 */
public abstract class Price {


    public abstract double getCharge(int daysRented);

    public abstract int getPriceCode();

    public int getFrequentRenterPoints(int daysRented) {
        System.out.println(1);
        return 1;
    }

}