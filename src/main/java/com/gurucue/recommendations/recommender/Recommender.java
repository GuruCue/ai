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

import com.gurucue.recommendations.recommender.RecommendProduct;
import com.gurucue.recommendations.recommender.RecommendationSettings;
import com.gurucue.recommendations.recommender.RecommenderNotReadyException;
import com.gurucue.recommendations.prediction.ProductRating;

/**
 * General interface for recommender class. A recommender class returns recommendations for several items (either for a particular user or in general). 
 * A recommendation can be seen as predicted usefulness (on some scale) of a specific item for a specific user. 
 */
public interface Recommender {
    /**
     * Returns recommendations for a consumer selected from given products and given recommendation settings. 
     * 
     * @param consumerId
     * @param products
     * @param recset
     * @return
     * @throws RecommenderNotReadyException
     * @throws InterruptedException
     */
    public abstract ProductRating[] getRecommendation(long consumerId, RecommendProduct[] products, RecommendationSettings recset) throws RecommenderNotReadyException, InterruptedException;

    /**
     * Creates a ProductRating object for a specific user and a specific product.
     * @throws RecommenderNotReadyException, InterruptedException 
     */
	public abstract ProductRating getRecommendation(long consumerId, RecommendProduct product) throws RecommenderNotReadyException, InterruptedException;
	
    /**
     * The method finds a set of products in candidateProducts that are similar to seedProducts. It uses the same predictor as the one used in recommendation.
     * @param seedProducts Find products that are similar to these.
     * @param candidateProducts Select products from products provided in this array.
     * @param recset 
     * @return
     * @throws RecommenderNotReadyException
     * @throws InterruptedException
     */
    ProductRating[] getSimilarProducts(long [] seedProducts, RecommendProduct [] candidateProducts, RecommendationSettings recset)
            throws RecommenderNotReadyException, InterruptedException;

    /**
	 * Reloads data from the database and relearns the recommendation model.
	 */
	public abstract RecommenderUpdaterJob updateModel(boolean async);

	/**
	 * Updates recommender with new data added since the last update. 
	 * @param async
	 * @return
	 */
	public abstract RecommenderUpdaterJob updateIncremental(boolean async) throws InterruptedException;

    public abstract RecommenderUpdaterJob updateIncrementalAndProductsUntilFinished(boolean async) throws InterruptedException;

	public abstract RecommenderUpdaterJob updateProducts(boolean async) throws InterruptedException;
	
	/**
	 * Returns most useful next pair for the consumer to be used in coldstart where user compares two products and selects the preferred one.
	 * @param consumerId
	 * @return
	 */
	public abstract long [] getNextPair(long consumerId) throws InterruptedException;
	/**
	 * Does the consumer need more profiling or reliable recommendations can be made? 
	 * @param consumerId
	 * @return Returns true if more profiling is needed. 
	 */
	public abstract boolean needsProfiling(long consumerId) throws InterruptedException;
	
	/**
	 * Stores predictor, decision module and the data store to a file.
	 * Load recommender by using the getRecommender factory with path argument or by using the loadFromFile method. 
	 */
	public abstract void saveToFile(String path) throws InterruptedException;

    /**
     * Loads stored recommender from a file.
     */
    public abstract RecommenderUpdaterJob loadFromFile(String path, boolean async);
	
    /**
     * Stops all background threads and frees all resources occupied by the recommender.
     */
    void stop();
}
