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
package com.gurucue.recommendations.prediction.aggregate;

import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.EmptyCommit;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ProductData;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.TIntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.Attr;
import com.gurucue.recommendations.recommender.dto.ConsumerRatingsData;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;


/**
 * A recommender that returns average product rating.
 * If several attributes are given, attributes are evaluated step-by-step until an attribute with defined rating is given. 
 */


public class AverageRatingPredictor extends Predictor {
    private final Logger logger;
    
    // the name of the event
    private final String RATINGSNAME;
    
    // data about counting attributes
    private final int [] ATTRIBUTES;
    private final int MAX_ATTR_VALUE;
    
    // blending factor in computing average of ratings
    protected final float KA;
    
    // counts of prod
    TLongIntMap attrCounts;
    // ratings sum of prod
    TLongFloatMap attrRatings;
    // global
    double globalRatings;
    long globalCounts;
    
    private DataStore data;
    
    public AverageRatingPredictor(String name, Settings settings) {
        super(name, settings);
        
        logger = LogManager.getLogger(AverageRatingPredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        RATINGSNAME = settings.getSetting(name + "_RATINGS");

        MAX_ATTR_VALUE = settings.getSettingAsInt(name + "_MAX_ATTR_VALUE");
        ATTRIBUTES = settings.getAsIntArray(name + "_ATTRIBUTES");
        
        KA = settings.getSettingAsFloat(name + "_KA");
    }

    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) 
    {
        final double globalAvg = globalRatings / (globalCounts + 1e-6); // global average
    	
        for (ProductRating pr : predictions)
        {
        	final ProductData pd = pr.getProductData();
        	boolean found = false;
            for (int ai = 0; ai < ATTRIBUTES.length; ai++)
            {
            	// get attribute values
                final Attr a = pd.getAttribute(ATTRIBUTES[ai]);
                if (a == null)
                {
                    continue;
                }
                final TIntSet avals = a.getValues();
                if (avals == null)
                {
                    continue;
                }
                // see if rating is given for attribute; take max estimated rating
                double rating = -1;
                for (TIntIterator it = avals.iterator(); it.hasNext(); )
                {
                	final long key = it.next()%MAX_ATTR_VALUE + MAX_ATTR_VALUE * ai;
                	if (!attrCounts.containsKey(key))
                		continue;
                	// estimate average, blend it with global average
                	final double estRating = (attrRatings.get(key) + KA * globalAvg) / (attrCounts.get(key) + KA);
                	rating = Math.max(rating, estRating);
                }
                // if rating is computed, we are done
                if (rating >= 0)
                {
                	if (rating > globalAvg)
                		pr.addPrettyExplanation("This item has high avg. rating: " + rating + ";");
            		addProductRating(pr, rating, "Avg. rating is " + rating + ";");
                	
                	found = true;
                	break;
                }
                // move to the next attribute
            }
            if (!found)
            	addProductRating(pr, 0, "");
        }
    }

    /**
     * Increase counter for each bought product in each possible context. 
     */
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData updateData) {
        logger.info("Incremental update of AverageRatingPredictor started.");
        // update data
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = updateData.dataStore;
            attrCounts = new TLongIntHashMap();
            attrRatings = new TLongFloatHashMap();
            globalRatings = 0;
            globalCounts = 0;
        }
        final int eventIndex = data.getEventsDescriptor(RATINGSNAME).index;

        // new (changed) consumers
        final List<ConsumerData> tmp_consumers = updateData.newData;
        
        // initialization of new class attributes
        TLongIntMap newAttrCounts = new TLongIntHashMap(attrCounts);
        TLongFloatMap newAttrRatings = new TLongFloatHashMap(attrRatings);
        double newGlobalRatings = globalRatings;
        long newGlobalCounts = globalCounts;        

        for (ConsumerData c: tmp_consumers)
        {
            if (null == c) // no events were added to this user
                continue;
            
            // are there any new events for this user, otherwise continue
            if (null == c.events[eventIndex])
                continue;

            // get ratings and indices of this user
            final ConsumerRatingsData ratingsData =  (ConsumerRatingsData) c.events[eventIndex];
            final int [] items = ratingsData.indices;
            final byte [] ratings = ratingsData.ratings;
            
            for (int i = 0; i < items.length; i++)
            {
                final int item = items[i];
                final byte rating = ratings[i];
                // first update global stats
                newGlobalRatings += rating;
                newGlobalCounts += 1;
                final ProductData pd = data.getProductByIndex(item);
                
                // update stats for each attribute
                for (int ai = 0; ai < ATTRIBUTES.length; ai++)
                {
                	// get attribute values
                    final Attr a = pd.getAttribute(ATTRIBUTES[ai]);
                    if (a == null)
                    {
                        continue;
                    }
                    final TIntSet avals = a.getValues();
                    if (avals == null)
                    {
                        continue;
                    }
                    // update stats for each attribute value
                    for (TIntIterator it = avals.iterator(); it.hasNext(); )
                    {
                    	final int val = it.next();
                    	final long key = val%MAX_ATTR_VALUE + MAX_ATTR_VALUE * ai;
                    	if (val > MAX_ATTR_VALUE)
                    		logger.warn("Attribute value exceeded constant MAX_ATTR_VALUES.");
                    	newAttrCounts.put(key, newAttrCounts.get(key) + 1);
                    	newAttrRatings.put(key, newAttrRatings.get(key) + rating);
                    }
                }
            }
        }
        
        logger.info("Incremental update of AverageRatingPredictor ended.");
        return new UpdateAll(newAttrCounts, newAttrRatings, newGlobalCounts, newGlobalRatings);
    }
    
 
    @Override
    public Commitable updateProducts(DataStore.UpdateProductsDelta delta) {
    	return EmptyCommit.INSTANCE;
    }
    
    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int predictionId,
            Map<String, String> tags) {
    	// get similar simply neglets the seed products and returns the same as normal prediction
    	getPredictions(predictions, tags);
    }
    
    @Override
    public void updateModel(final DataStore dataStore) {
        logger.error("updateModel is deprecated; should not be called!");
    }

    @Override
    public void updateModelQuick(DataStore data) {
        logger.error("updateModelQuick is deprecated; should not be called!");
    }

    @Override
    public ProductPair getBestPair(int consumer_index) {
        logger.error("getBestPair not implemented yet for AverageRatingPredictor.");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumer_index) {
        logger.error("needsProfiling not implemented yet for AverageRatingPredictor.");
        return false;
    }

    @Override
    public Commitable updateConsumer(DataStore.UpdateData updateData) {
        logger.error("updateConsumer is deprecated; should not be called!");
        return null;
    }

	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		out.writeObject(attrCounts);
		out.writeObject(attrRatings);
		out.writeLong(globalCounts);
		out.writeDouble(globalRatings);
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        logger.info("updateModelFromFile of AverageRatingPredictor started.");
        // update data
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = data;
            attrCounts = new TLongIntHashMap();
            attrRatings = new TLongFloatHashMap();
            globalRatings = 0;
            globalCounts = 0;
        }  
        TLongIntMap newAttrCounts = (TLongIntMap) in.readObject();
        TLongFloatMap newAttrRatings = (TLongFloatMap) in.readObject();
        long newGlobalCounts = in.readLong();
        double newGlobalRatings = in.readDouble(); 
        logger.info("updateModelFromFile of AverageRatingPredictor ended.");
        return new UpdateAll(newAttrCounts, newAttrRatings, newGlobalCounts, newGlobalRatings);		
	}      

    
    private class UpdateAll implements Commitable {
        TLongIntMap newAttrCounts;
        TLongFloatMap newAttrRatings;
        double newGlobalRatings;
        long newGlobalCounts;        
    
        UpdateAll(final TLongIntMap newAttrCounts, final TLongFloatMap newAttrRatings, final long newGlobalCounts, final double newGlobalRatings) {
            this.newAttrCounts = newAttrCounts;
            this.newAttrRatings = newAttrRatings;
            this.newGlobalRatings = newGlobalRatings;
            this.newGlobalCounts = newGlobalCounts;
        }

        @Override
        public void commit() {
            attrCounts = newAttrCounts;
            attrRatings = newAttrRatings;
            globalRatings = newGlobalRatings;
            globalCounts = newGlobalCounts;
        }        
    }    
}
