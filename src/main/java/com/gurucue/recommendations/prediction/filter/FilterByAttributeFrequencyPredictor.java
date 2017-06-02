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
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ProductData;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TDoubleIntMap;
import gnu.trove.map.TIntDoubleMap;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TDoubleIntHashMap;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.Attr;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;


/**
 *  
 * A recommender that counts how many times (n) an attribute occurred within the bought products. 
 * If an attribute occured more than selected number of time,
 * then this product is filtered.   
 */
public class FilterByAttributeFrequencyPredictor extends Predictor {
    private final Logger logger;
    
    // number of attribute values will never exceed this value
    private final int MAX_ATTR_VALUES;
   
    private final int [] ATTRIBUTES;
    private final TLongSet SKIP_CONSUMERS;
    private final int MAX_ITEMS;
    private final int THRESHOLD;
    
    final long consumerBase;
    
    // the name of the event
    private final String BUYSNAME;
    
    // averages of prod
    TLongIntMap attrCounts;

    private DataStore data;
    
    public FilterByAttributeFrequencyPredictor(String name, Settings settings) {
        super(name, settings);
        
        logger = LogManager.getLogger(FilterByAttributeFrequencyPredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        BUYSNAME = settings.getSetting(name + "_BUYS");

        MAX_ATTR_VALUES = settings.getSettingAsInt(name + "_MAX_ATTR_VALUES");
        ATTRIBUTES = settings.getAsIntArray(name + "_ATTRIBUTES");

        MAX_ITEMS = settings.getSettingAsInt(name + "_MAX_ITEMS");
        THRESHOLD = settings.getSettingAsInt(name + "_THRESHOLD");

        long [] skip = settings.getAsLongArray(name + "_SKIP_CONSUMERS");
        SKIP_CONSUMERS = new TLongHashSet(skip);

        consumerBase = ATTRIBUTES.length * MAX_ATTR_VALUES;
    }

    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) 
    {
    	int consumerIndex = -1;
    	for (Iterator<ProductRating> iterator = predictions.iterator(); iterator.hasNext(); )
    	{
            ProductRating pr = iterator.next();
            if (consumerIndex < 0) {
                consumerIndex = pr.getConsumerIndex();
                if (SKIP_CONSUMERS.contains(pr.getConsumerId()))
                    return;
            }

            final int productIndex = pr.getProductIndex();
            final TLongList attrValues = getAttrValues(productIndex);
            
            int attr_count = 0;
            for (TLongIterator it = attrValues.iterator(); it.hasNext(); )
            {
                final long atVal = it.next();
                attr_count += attrCounts.get(consumerIndex * consumerBase + atVal);
            }

            if (attr_count > THRESHOLD)
            {
            	iterator.remove();
            }
    	}
    }

    
    TLongList getAttrValues(final int productIndex)
    {
        TLongList values = new TLongArrayList();
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

    private void updateMap(TLongIntMap counts, final List<ConsumerData> consumers, final TLongIntMap consumerIDs, final int eventIndex)
    {
    	for (ConsumerData c : consumers)
    	{
    		if (c == null)
    			continue;
    		
    		if (c.events[eventIndex] == null)
    			continue;
    		
    		int consIndex = consumerIDs.get(c.consumerId);
            final int [] items = c.events[eventIndex].getProductIndices();
            for (int item : items)
            {
            	final TLongList attrValues = getAttrValues(item);
                for (TLongIterator it = attrValues.iterator(); it.hasNext(); )
                {
                    final long atVal = it.next();
                    final long key = consIndex * consumerBase + atVal;
                    counts.put(key, counts.get(key) + 1);
                }
            }
    	}
    }
    
    /**
     * Increase counter for each bought product in each possible context. 
     */
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData updateData) {
        logger.info("Incremental update of FilterByAttributeFrequencyPredictor started.");
        // update data
        boolean runningForTheFirstTime = false;
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = updateData.dataStore;
            attrCounts = new TLongIntHashMap ();
            runningForTheFirstTime = true;
        }
        final int eventIndex = data.getEventsDescriptor(BUYSNAME).index;

        // new (changed) consumers
        
        TLongIntMap newCounts = new TLongIntHashMap (attrCounts);
        updateMap(newCounts, data.getConsumers(), data.getConsumerIDMap(), eventIndex);
        if (!runningForTheFirstTime)
        {
            final List<ConsumerData> tmp_consumers = updateData.newData;
            final TLongIntHashMap tmp_ids = updateData.newConsumerIDs;
            updateMap(newCounts, tmp_consumers, tmp_ids, eventIndex);
        }
        
        if (newCounts.size() > MAX_ITEMS)
        {
        	// decrease all values by one, if value is 0 or less, remove it from map
        	for (TLongIntIterator it = newCounts.iterator(); it.hasNext(); )
        	{
        		it.advance();
        		final long key = it.key();
        		final int count = it.value();
        		
        		if (count > 1)
        			it.setValue(count-1);
        		else
        			it.remove();
        	}
        }
        
        logger.info("Incremental update of FilterByAttributeFrequencyPredictor ended.");
        return new UpdateAll(newCounts);
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
        logger.error("getBestPair not implemented yet for FilterByAttributeFrequencyPredictor.");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumer_index) {
        logger.error("needsProfiling not implemented yet for FilterByAttributeFrequencyPredictor.");
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
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        logger.info("Loading from file of FilterByAttributeFrequencyPredictor started.");
        // update data
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = data;
            attrCounts = new TLongIntHashMap ();
        }
        TLongIntMap newCounts = (TLongIntMap) in.readObject();
        logger.info("Loading from file of FilterByAttributeFrequencyPredictor ended.");
        return new UpdateAll(newCounts);
	}    


    private class UpdateAll implements Commitable {
        final TLongIntMap newCounts;
    
        UpdateAll(final TLongIntMap newCounts) {
            this.newCounts = newCounts;
        }

        @Override
        public void commit() {
        	attrCounts = newCounts;
        }        
    }
}
