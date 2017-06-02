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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ProductData;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import com.gurucue.recommendations.recommender.dto.DataStore;

/**
 * Class containing estimated ratings for a given product and a given user. The estimated rating can be a true
 * estimation of rating or any other value that make ranking of products possible according to user's taste. An example of
 * a such value is the probability of buying a product. 
 * 
 * Along to the main prediction, this class also contains a hashmap of all predictions given by all predictors. Whenever several 
 * predictors are used (e.g. hybrid approaches), the Decision module might need predictions of particular predictors. The key of hashmap is 
 * predictor's ID (int; every predictor must have an ID in the settings) and its prediction (double). 
 */
public class ProductRating  implements Comparable<ProductRating> {
    private ProductData prodData; // product data object
    private ConsumerData consData; // consumer data object
    private int productIndex; // index of product in products table
    private int consumerIndex; // index of consumer in consumer table
    private double prediction; // main prediction for this product and user
    private float reliability; // reliability of recommender that generated this prediction (between 0 and 1)
    private String explanation; // explanation of this recommendation
    TIntDoubleMap allPredictions; // particular predictions of predictors
    TIntFloatMap allReliabilities; // reliabilities of all predictors
    TIntObjectMap<String> allExplanations; // particular predictions of predictors
    Map<String, Float> prettyExplanations;
    Map<String, String> tags;
    //private final TIntSet tags;

    /**
     * Prediction for product / consumer pair constructor
     * @param p product
     * @param c consumer
     */
    public ProductRating(ProductData p, int productIndex, ConsumerData c, int consumerIndex, Set<String> tags)
    {
        this.prodData = p;
        this.consData = c;
        this.productIndex = productIndex;
        this.consumerIndex = consumerIndex;
        allPredictions = new TIntDoubleHashMap();
        allReliabilities = new TIntFloatHashMap();
        allExplanations = new TIntObjectHashMap<String>();
        prediction = 0.0;
        reliability = 0.0f;
        explanation = "";
        prettyExplanations = new HashMap<String, Float> ();
        this.tags = new HashMap<String, String> ();
        if (tags != null)
	        for (String k : tags)
	            this.tags.put(k, "");
    }
    
    /**
     * Setter for prediction field. Predictor ID and explanation of the prediction should also be given.
     * @param prediction
     */
    public void setPrediction(double prediction, int predictorID, String explanation)
    {
        this.prediction = prediction;
        this.reliability = 1.0f;
        allPredictions.put(predictorID, prediction);
        allReliabilities.put(predictorID, 1.0f);
        this.explanation = explanation;
        allExplanations.put(predictorID, explanation);
    }
    
    /**
     * Set prediction values produced by a predictor for a particular product.
     * @param prediction Estimation of how much a consumer will like this product (0 means that consumer dislikes the product).
     * @param reliability How reliable the recommender is in its recommendation? A value between 0(not reliable at all, guessing) and 1 (very reliable).
     * @param predictorID The id of the predictor that computed the prediction.
     * @param explanation Explanation of how prediction was computed.
     */
    public void setPrediction(double prediction, float reliability, int predictorID, String explanation)
    {
        this.prediction = prediction;
        this.reliability = reliability;
        allPredictions.put(predictorID, prediction);
        allReliabilities.put(predictorID, reliability);
        this.explanation = explanation;
        allExplanations.put(predictorID, explanation);
    }    
    
    public void setProductID(DataStore data, long productID)
    {
        if (productID < 0)
        {
            this.prodData = null;
            this.productIndex = -1;
            return;
        }
        this.prodData = data.getProductById(productID);
        this.productIndex = data.getProductIndex(productID);
    }

    /**
     * Getter for prediction field; always the last prediction added.
     * @return
     */
    public double getPrediction()
    {
        return prediction;
    }

    public float getReliability()
    {
        return reliability;
    }
    
    /**
     * Getter for any prediction.
     * @param predictorID
     * @return
     */
    public double getPrediction(int predictorID)
    {
        return allPredictions.get(predictorID);
    }

    public float getReliability(int predictorID)
    {
        return allReliabilities.get(predictorID);
    }
    
    
    /**
     * Returns data about product.
     * @return
     */
    public ProductData getProductData()
    {
        return prodData;
    }

    /**
     * Returns data about consumer.
     * @return
     */
    public ConsumerData getConsumerData()
    {
        return consData;
    }
    
    /**
     * Returns product database ID.
     * @return product database ID (-1 if prodData is null)
     */
    public long getProductId() {
    	if (prodData == null)
    		return -1;
        return prodData.productId;
    }
    
    /**
     * Returns consumer database ID.
     * @return consumer database ID (-1 if consData is null)
     */
    public long getConsumerId() {
    	if (consData == null)
    		return -1;
        return consData.consumerId;
    }
    
    /**
     * Getter for explanation string.
     * @return
     */
    public String getExplanation() {
        return explanation;
    }
    
    public void setExplanation(int predictorID, String explanation)
    {
        this.explanation = explanation;
        allExplanations.put(predictorID, explanation);
    }

    public void setExplanation(String explanation)
    {
        this.explanation = explanation;
    }

    /**
     * Getter for a particular explanation
     * @param predictorID
     * @return
     */
    public String getExplanation(int predictorID)
    {
        return allExplanations.get(predictorID);
    }
    
    /**
     * Default adder for pretty explanation. Rank of added explanations is 1.
     * @param explanation string description of explanation.
     */
    public void addPrettyExplanation(String explanation)
    {
    	prettyExplanations.put(explanation, 1.0f);
    }
    
    /**
     * Adds a pretty explanation with value as rank of explanation. 
     * 
     * @param explanation string description of explanation.
     * @param value importance of explanation. 
     */
    public void addPrettyExplanation(String explanation, Float value)
    {
    	prettyExplanations.put(explanation, value);
    }
    
    public Map<String, Float> getPrettyExplanations()
    {
    	return prettyExplanations;
    }
    
    /**
     * Getter for particular predictions of different predictors.
     * @return
     */
    public TIntObjectMap<String> getAllExplanations()
    {
        return allExplanations;
    }
    
    
    /**
     * Getter for particular predictions of different predictors.
     * @return
     */
    public TIntDoubleMap getAllPredictions()
    {
        return allPredictions;
    }

    public TIntFloatMap getAllReliabilities()
    {
        return allReliabilities;
    }
    
    public void setTag(String tag, String value)
    {
        tags.put(tag, value);
    }
    
    public String getTag(String tag)
    {
        return tags.get(tag);
    }
    
    public Map<String, String> getTags()
    {
        return tags;
    }
    
    @Override
    public int compareTo(ProductRating productRating) {
        final double dif = this.prediction - productRating.getPrediction();
        if (dif > 0)
            return -1;
        if (dif < 0)
            return 1;
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ProductRating) {
            ProductRating pr = (ProductRating) o;
            if (this.consData.consumerId == pr.consData.consumerId && 
                this.prodData.productId == pr.consData.consumerId) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public String toString() 
    {
        String combinedExplanation = "";
        for (TIntObjectIterator<String> it = allExplanations.iterator(); it.hasNext();)
        {
            it.advance();
            final String val = it.value();
            if (!val.isEmpty())
                combinedExplanation += val;
        }
        if (null == consData)
            return "UserID:null user,ProductID:"+prodData.productId+",prediction:"+prediction+",explanation:"+combinedExplanation;
        return "UserID:"+consData.consumerId+",ProductID:"+prodData.productId+",prediction:"+prediction+",explanation:"+combinedExplanation;
    }

    public int getProductIndex() {
        return productIndex;
    }

    public int getConsumerIndex() {
        return consumerIndex;
    }
    
    public static class ProductDataComparator implements Comparator<ProductRating>  
    {
        private final int comparableID;
        public ProductDataComparator(int comparableID)
        {
            this.comparableID = comparableID;
        }
        
        @Override
        public int compare(ProductRating o1, ProductRating o2) {
            final double dif = o1.getPrediction(comparableID) - o2.getPrediction(comparableID);
            if (dif > 0)
                return -1;
            if (dif < 0)
                return 1;
            return 0;    
        }    
    }
}
