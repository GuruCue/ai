/*
 * This file is part of Guru Cue Search & Recommendation Engine.
 * Copyright (C) 2017 Guru Cue Ltd.
 *
 * Guru Cue Search & Recommendation Engine is free software: you can
 * redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * Guru Cue Search & Recommendation Engine is distributed in the hope
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Guru Cue Search & Recommendation Engine. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package com.gurucue.recommendations.recommender;

import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ProductData;

// Class containing estimated ratings for a product and a user
@Deprecated
public class ProductRecommendation implements Comparable<ProductRecommendation> {
    private ProductData prodData; // product data object
    private ConsumerData consData; // consumer data object
	private double estimatedRating; // estimated rating  
	private double normalizedEstRating; // rating normalized between 0 and 1
	private double confidenceLower; // lower bound of confidence interval for estimated rating
	private double confidenceUpper; // upper bound of confidence interval for estimated rating
	
	public ProductRecommendation(ProductData p, ConsumerData c, double estimatedRating, double normalizedEstRating, double confidenceLower, double confidenceUpper)
	{
		this.prodData = p;
		this.consData = c;
		this.estimatedRating = estimatedRating;
		this.normalizedEstRating = normalizedEstRating;
		this.confidenceLower = confidenceLower;
		this.confidenceUpper = confidenceUpper;
	}

    public double getEstimatedRating() {
        return estimatedRating;
    }

    public double getNormalizedEstRating() {
        return normalizedEstRating;
    }

    public double getConfidenceLower() {
        return confidenceLower;
    }

    public double getConfidenceUpper() {
        return confidenceUpper;
    }

    @Override
    public int compareTo(ProductRecommendation arg0) {
        double dif = this.estimatedRating - arg0.getEstimatedRating();
        if (dif > 0)
            return -1;
        if (dif < 0)
            return 1;
        return 0;
    }
    
    @Override
    public boolean equals(Object o)
    {
        if (o instanceof ProductRecommendation) {
            ProductRecommendation pr = (ProductRecommendation) o;
            if (this.estimatedRating == pr.estimatedRating)
                return true;
        }
        return false;
    }

    public void setProductData(ProductData prodData) {
        this.prodData = prodData;
    }

    public ProductData getProductData() {
        return prodData;
    }

    public void setConsumerData(ConsumerData consData) {
        this.consData = consData;
    }

    public ConsumerData getConsumerData() {
        return consData;
    }

    /**
     * Returns the product ID that this object represents. This is about the
     * only thing that the web GUI needs.
     * @return the product ID
     */
    public long getProductId() {
        return prodData.productId;
    }
}
