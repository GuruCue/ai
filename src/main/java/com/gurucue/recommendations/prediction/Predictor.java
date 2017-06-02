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

import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ProductData;
import gnu.trove.set.TIntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

import com.gurucue.recommendations.recommender.dto.DataStore;

/**
 * All rating predictors must extend this class. A rating predictor should
 * also have a constructor that accepts a settings object.
 */
public abstract class Predictor implements Cloneable {
    // set name and ID for every predictor.
    final protected int ID;
    final protected String NAME;
    final protected Settings settings;
    
    protected Predictor(String name, Settings settings)
    {
        NAME = name;
        this.settings = settings;
        ID = settings.getSettingAsInt(name + "_ID");
    }
    
    /**
     * Creates a new product rating. Use this method when this is the first prediction for this
     * product. Otherwise use the other method where old ProductRating is provided as an argument.
     * @param p
     * @param c
     * @param prediction
     * @param explanation
 
     * @return
     */
    @Deprecated
    protected ProductRating createProductRating(ProductData p, int productIndex, ConsumerData c, int consumerIndex, double prediction, String explanation) {
        ProductRating pr = new ProductRating(p, productIndex, c, consumerIndex, null);
        pr.setPrediction(prediction,ID, explanation);
        return pr;
    }
    
    public final int getID()
    {
        return ID;
    }
    
    /**
     * Adds a prediction to a ProductRating object.
     * 
     * @param pr
     * @param prediction
     * @param explanation
     * @return
     */
    protected void addProductRating(ProductRating pr, double prediction, String explanation)
    {
        pr.setPrediction(prediction, ID, explanation);
    }

    protected void addProductRating(ProductRating pr, double prediction, String explanation, int id_shift)
    {
        pr.setPrediction(prediction, id_shift + ID, explanation);
    }
    
    /**
     * Ranks .
     * 
     * Important: prediction should work also for a null user!
     * 
     * @param tags TODO
     *
     */
    public void getPredictionsTime(List<ProductRating> predictions, Map<String, String> tags)
    {
    	final long start = System.currentTimeMillis();
    	getPredictions(predictions, tags);
    	final long totalTime = System.currentTimeMillis() - start;
    	tags.put("time_predictor_"+ID, String.valueOf(totalTime));    	
    }
    
    /**
     * Estimates ratings that a consumer would give to a list of products.The predictions list should not be empty.
     * 
     * Important: prediction should work also for a null user!
     * 
     * @param tags TODO
     *
     */
    public abstract void getPredictions(List<ProductRating> predictions, Map<String, String> tags);

    /**
     * Updates recommendation model using new data.
     */
    public abstract void updateModel(DataStore data);

    /**
     * Quickly updates recommendation model. This is meant for testing,
     * but it might not be very effective (depends on predictor).
     */
    @Deprecated
    public abstract void updateModelQuick(DataStore data);

    /**
     * Makes an incremental update of a model. 
     * @param updateData
     * @return
     * @throws CloneNotSupportedException 
     */
    public abstract Commitable updateModelIncremental(DataStore.UpdateIncrementalData newData);
    
    /**
     * Quickly updates recommendation model by considering only data for this consumer. It might not be as effective as 
     * making a full update.
     * @return an instance representing changes that shall be commited later
     */
    public abstract Commitable updateConsumer(DataStore.UpdateData newData);

    /**
     * Updates products used in the recommendation model. Use if products were added / deleted or if attributes 
     * were modified.
     * @param delta
     * @return
     */
    public abstract Commitable updateProducts(DataStore.UpdateProductsDelta delta);
    /**
     * Returns new product pair to be used in coldstart; namely, which comparison given by the user would help 
     * the recommender the most. In the future this method should be generalized to ask for any type of 
     * valuable information that can be provided by the user.
     */
    public abstract ProductPair getBestPair(int consumerIndex);

    /**
     * This method returns true if the data contains enough information to make a reasonable recommendation for this user. 
     */
    public abstract boolean needsProfiling(int consumer_index);

    /**
     * Returns a set of similar products to the ones specified. 
     * @param productIndices The select products.
     * @param tags TODO
     * @param candidateProducts Products to choose from.
     * @param numberSimilar The maximal number of returned products.
     * @return an array of products (indices from products table)
     */
    public abstract void getSimilarProducts(TIntSet productIndices, List<ProductRating> predictions, int prediction_id, Map<String,String> tags);
    
    public void getSimilarProductsTime(TIntSet productIndices, List<ProductRating> predictions, int prediction_id, Map<String,String> tags)
    {
    	final long start = System.currentTimeMillis();
    	getSimilarProducts(productIndices, predictions, prediction_id, tags);
    	final long totalTime = System.currentTimeMillis() - start;
    	tags.put("time_predictor_"+ID, String.valueOf(totalTime));    	
    }    

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
    
    /**
     * Creates a new instance of the predictor (clones only settings, but not learned values)
     * @return
     * @throws SecurityException 
     * @throws NoSuchMethodException 
     * @throws InvocationTargetException 
     * @throws IllegalArgumentException 
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     */
    public Predictor createEmptyClone() throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
    {
        Constructor<? extends Predictor> constructor = this.getClass().getConstructor(String.class,Settings.class);  
        return constructor.newInstance(new Object[] {NAME, settings});
    }
    
    /**
     * Serializes a model into an output stream
     * @param out
     * @return
     * @throws IOException 
     */
    public abstract void serialize(ObjectOutputStream out) throws IOException;

    /**
     * Updates the model by reading it from file. 
     * @param in
     * @return
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    public abstract Commitable updateModelFromFile(ObjectInputStream in, DataStore data) throws ClassNotFoundException, IOException;
    
}
