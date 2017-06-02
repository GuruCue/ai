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
package com.gurucue.recommendations.prediction.filter;

import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.EmptyCommit;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.Attr;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ConsumerRatingsData;
import com.gurucue.recommendations.recommender.dto.ProductData;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;

/**
 * This is a filter that filters out all products with negativelly rated attributes. 
 */
public class NegativeRatingFilter extends Predictor {
    private final Logger logger;
    
    // the name of the event
    private final String RATINGSNAME;
    
    // attributes used in filtering
    private final int [] ATTRIBUTES;
    // number of attribute values should never exceed this value
    private final int MAX_ATTR_VALUES;
    
    private final float NEGATIVE_VALUE;
    
    // Negative product ids
    TIntObjectMap<TIntSet> negativeAttributes;
    
    private DataStore data;
    
    public NegativeRatingFilter(String name, Settings settings) {
        super(name, settings);
        
        logger = LogManager.getLogger(NegativeRatingFilter.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        RATINGSNAME = settings.getSetting(name + "_RATINGSNAME");

        ATTRIBUTES = settings.getAsIntArray(name + "_ATTRIBUTES");
        MAX_ATTR_VALUES = settings.getSettingAsInt(name + "_MAX_ATTR_VALUES");    
        
        NEGATIVE_VALUE = settings.getSettingAsFloat(name + "_NEGATIVE_VALUE");
    }

    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) 
    {
    	if (predictions == null || predictions.size() == 0)
    		return;
    	
        final TIntSet negAtts = negativeAttributes.get(predictions.get(0).getConsumerIndex());   

        if (negAtts != null && negAtts.size() > 0)
        {
        	//for (ProductRating pr : predictions)
        	for (Iterator<ProductRating> iter = predictions.listIterator(); iter.hasNext(); ) {
        		ProductRating pr = iter.next();
        		final int productIndex = pr.getProductIndex();
                final TIntList attrValues = getAttrValues(productIndex);
                
                // if any of its values is in the negative set, remove it.
                for (TIntIterator it = attrValues.iterator(); it.hasNext(); )
                {
                	final int val = it.next();
                	if (negAtts.contains(val))
                	{
                		iter.remove();
                		break;
                	}
                	
                }
        	}
        }
    }

    /**
     * Increase counter for each bought product in each possible context. 
     */
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData updateData) {
        logger.info("Incremental update of NegativeRatingFilter started.");
        // update data
        if (this.data == null) // initialization is needed
        {
            this.data = updateData.dataStore;
            negativeAttributes = new TIntObjectHashMap<TIntSet>();            
        }
        TIntObjectMap<TIntSet> newNegativeAttributes = new TIntObjectHashMap<TIntSet>();
        final List<ConsumerData> new_consumers = updateData.newData;
        final TLongIntHashMap tmp_ids = updateData.newConsumerIDs;
        final int eventIndex = data.getEventsDescriptor(RATINGSNAME).index;
        
        for (ConsumerData c: new_consumers)
        {
            if (null == c) // no events were added to this user
                continue;
            
            // are there any new events for this user, otherwise continue
            if (null == c.events[eventIndex])
                continue;
            final int consIndex = tmp_ids.get(c.consumerId);
            
            TIntSet newNegativeSet;
            if (negativeAttributes.containsKey(consIndex))
            	newNegativeSet = new TIntHashSet(negativeAttributes.get(consIndex));
            else
            	newNegativeSet = new TIntHashSet();
            	
            newNegativeAttributes.put(consIndex, newNegativeSet);

            final ConsumerRatingsData ratingsData =  (ConsumerRatingsData) c.events[eventIndex];
            final int [] items = ratingsData.indices;
            final byte [] ratings = ratingsData.ratings;
            for (int i = 0; i < items.length; i++)
            {
                final TIntList attrValues = getAttrValues(items[i]);
                final byte rating = ratings[i];
                
                // store also general counter (without context)
                // for a consumer and for everyone
                for (TIntIterator it = attrValues.iterator(); it.hasNext(); )
                {
                    final int atVal = it.next();
                    
                    if (rating <= NEGATIVE_VALUE)
                    	newNegativeSet.add(atVal);
                    else
                    	newNegativeSet.remove(atVal);
                }
            }
        }

        logger.info("Incremental update of NegativeRatingFilter ended.");
        return new UpdateDelta(newNegativeAttributes);
    }
        
    TIntList getAttrValues(final int productIndex)
    {
        TIntList values = new TIntArrayList();
        final ProductData pd = data.getProductByIndex(productIndex);
        int counter = 0;
        for (int attr : ATTRIBUTES)
        {
            final Attr a = pd.getAttribute(attr);
            if (a == null)
            {
            	counter ++;
                continue;
            }
            final TIntSet avals = a.getValues();
            if (avals == null)
            {
            	counter ++;
                continue;
            }
            for (TIntIterator it = avals.iterator(); it.hasNext(); )
            {
                values.add(it.next()%MAX_ATTR_VALUES + MAX_ATTR_VALUES * counter);
            }
            counter ++;
        }
        return values;
    }        
        
    /**
     * Remove nonrelevant products / add new products. 
     */
    @Override
    public Commitable updateProducts(DataStore.UpdateProductsDelta delta) {
    	return EmptyCommit.INSTANCE;
    }
    
    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int predictionId,
            Map<String, String> tags) {
    	// does nothing; no filter;
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
        logger.error("getBestPair not implemented yet for NegativeRatingFilter.");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumer_index) {
        logger.error("needsProfiling not implemented yet for NegativeRatingFilter.");
        return false;
    }

    @Override
    public Commitable updateConsumer(DataStore.UpdateData updateData) {
        logger.error("updateConsumer is deprecated; should not be called!");
        return null;
    }

	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		out.writeObject(negativeAttributes);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data) throws ClassNotFoundException, IOException {
        logger.info("Update model from file of NegativeRatingFilter started.");
        if (this.data == null) // initialization is needed
        {
            this.data = data;
            negativeAttributes = new TIntObjectHashMap<TIntSet>();            
        }        
        
		TIntObjectMap<TIntSet> newNegativeAttributes = (TIntObjectMap<TIntSet>) in.readObject();
        logger.info("Update model from file of NegativeRatingFilter ended.");
        return new UpdateDelta(newNegativeAttributes);
	}
    

    private class UpdateDelta implements Commitable {
    	TIntObjectMap<TIntSet> newNegativeAttributes;
    
        UpdateDelta(final TIntObjectMap<TIntSet> newNegativeAttributes) {
            this.newNegativeAttributes = newNegativeAttributes;
        }

        @Override
        public void commit() {
        	negativeAttributes.putAll(newNegativeAttributes);
        }        
    }
}
