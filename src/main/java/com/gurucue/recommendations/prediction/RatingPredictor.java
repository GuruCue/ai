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
package com.gurucue.recommendations.prediction;

import java.util.List;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.dto.CSPairData;
import com.gurucue.recommendations.recommender.dto.DataStore;

/**
 * All rating predictors must implement this interface. A rating predictor should
 * also have a constructor that accepts a settings object.
 * 
 */
@Deprecated
public interface RatingPredictor extends Cloneable {
    /**
     * Estimates rating that a consumer would give to a product.
     *  
     * @param consumer_index Index of consumer in ConsumerData array
     * @param product_index Index of product in ProductData array
     */
    ProductRating getPrediction(int consumer_index, int product_index, boolean cSOnly);
    /**
     * Updates recommendation model using new data. 
     */
    void updateModel(DataStore data);
    /**
     * Quickly updates recommendation model. This is meant for testing, 
     * but it might not be very effective (depends on predictor). 
     */
    void updateModelQuick(DataStore data);
    /**
     * 
     * @param consumer
     * @param partner
     * @return
     */
    
    void updateConsumer(CSPairData[] pairs, int consumer_index);
    
    
    public abstract List<ProductPair> getNextPair(int consumer_index);
    /**
     * 
     * @param consumer
     * @param partner
     * @return
     */
    public abstract boolean needsProfiling(int consumer_index);    

}
