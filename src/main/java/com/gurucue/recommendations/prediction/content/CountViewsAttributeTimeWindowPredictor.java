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
package com.gurucue.recommendations.prediction.content;

import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.EmptyCommit;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.dto.ConsumerMetaEventsData;
import com.gurucue.recommendations.recommender.dto.ContextDiscretizer;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;


/**
 * A recommender that returns the number of times a product was bought per its attribute value.
 * 
 *  It considers only products between the last product read from the data base and TIME_WINDOW.
 * 
 * TODO: still missing handling contexts. 
 */
public class CountViewsAttributeTimeWindowPredictor extends Predictor {
    private final Logger logger;
    
    // the name of the event
    private final String BUYSNAME;
    private final long TIME_WINDOW;
    
    // tag specifying relevance
    private final String TAG_RELEVANCE;
    
    // context handler
    ContextDiscretizer contextHandler;
    
    // counts of attributes
    TIntObjectMap<TIntIntMap> counts;
    
    // attribute index (expects a multival attribute)
    protected final int ATTRIBUTE;
    
    private DataStore data;
    
    public CountViewsAttributeTimeWindowPredictor(String name, Settings settings) {
        super(name, settings);
        
        logger = LogManager.getLogger(CountViewsAttributeTimeWindowPredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        BUYSNAME = settings.getSetting(name + "_BUYS");
        TIME_WINDOW = settings.getSettingAsLong(name + "_TIME_WINDOW");
        
        // initialize context handler
        contextHandler = new ContextDiscretizer(name, settings);
        
        ATTRIBUTE = settings.getSettingAsInt(name + "_ATTRIBUTE_INDEX");  
        TAG_RELEVANCE = settings.getSetting(name + "_TAG_RELEVANCE");
    }

    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) 
    {
        float predValues = 0;
        float predValuesSqr = 0;
        final int N = predictions.size();
        for (ProductRating pr : predictions)
        {
        	if (TAG_RELEVANCE != null && !pr.getTags().containsKey(TAG_RELEVANCE))
            	{
                    addProductRating(pr, 0, "");
            		continue;
            	}

        	// get attribute values
        	final TIntSet attr = pr.getProductData().getValues(ATTRIBUTE);
        	if (attr == null)
        	{
        		logger.warn("An item without an attribute value!");
                addProductRating(pr, 0, "");
                continue;
        	}

            final int productIndex = pr.getProductIndex();
            TIntList contextPositions = contextHandler.getContext(data, productIndex, tags);

            float averageCount = 0;
            int counter = 0;
        	for (TIntIterator it = attr.iterator(); it.hasNext(); )
        	{
        		final int val = it.next();
        		if (!counts.containsKey(val))
        			continue;
                for (TIntIterator itc = contextPositions.iterator(); itc.hasNext();) 
                {
                    averageCount += counts.get(val).get(itc.next());
                    counter += 1;
                }
        	}
            float pred = 0;
            if (counter > 0)
            	pred = averageCount/counter;
            predValues += pred;
            predValuesSqr += pred*pred;
            addProductRating(pr, pred, "");
        }
        final double threshold = predValues / N + Math.sqrt(predValuesSqr/N + Math.pow(predValues/N,2));
        for (ProductRating pr : predictions)
        {
            final double p = pr.getPrediction();
            if (p > threshold) { 
                pr.setExplanation(this.ID, "Number of attribute views for event " + BUYSNAME + " is high:" + p + ";");
                pr.addPrettyExplanation("Most viewed", 1.0f);
            }    
        }
    }

    
    private void countProducts(final int [] items, final long [] dates, TIntObjectMap<TIntIntMap> newCounts, final ConsumerMetaEventsData meta, final long lastAllowedTimestamp)
    {
        for (int i = 0; i < items.length; i++)
        {
            final int item = items[i];
            if (dates[i] < lastAllowedTimestamp)
                continue;
            final TIntSet attr = data.getProductByIndex(item).getValues(ATTRIBUTE);
            if (attr == null)
            {
            	continue;
            }
        	for (TIntIterator it = attr.iterator(); it.hasNext(); )
        	{
        		final int val = it.next();

	            if (!newCounts.containsKey(val))
	            {
	                newCounts.put(val, new TIntIntHashMap());
	            }
	            final TIntIntMap cts = newCounts.get(val);
	            final TIntList contextPositions = contextHandler.getContext(data, item, meta, i);
	            
	            for (TIntIterator itc = contextPositions.iterator(); itc.hasNext(); )
	            {
	                final int pos = itc.next();
	                cts.put(pos, cts.get(pos) + 1);
	            }
        	}
        }
    }
    
    /**
     * Increase counter for each bought product in each possible context. 
     */
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData updateData) {
        logger.info("Incremental update of CountViewsAttributeTimeWindowPredictor started.");
        // update data
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = updateData.dataStore;
        }
        final int eventIndex = data.getEventsDescriptor(BUYSNAME).index;
        final long lastAllowedTimestamp = data.getEventsDescriptor(BUYSNAME).last_read_date - TIME_WINDOW;

        TIntObjectMap<TIntIntMap> newCounts = new TIntObjectHashMap<TIntIntMap>();

        for (ConsumerData c: updateData.newData)
        {
            if (null == c) // no events were added to this user
                continue;
            
            // are there any new events for this user, otherwise continue
            if (null == c.events[eventIndex])
                continue;

            final int [] items = c.events[eventIndex].getProductIndices();
            final ConsumerMetaEventsData meta = c.events[eventIndex].getMeta();
            final long [] dates = meta.getLongValuesArray(data.getEventsDescriptor(BUYSNAME).getMetaIndex("date"));
            countProducts(items, dates, newCounts, meta, lastAllowedTimestamp);
        }
        for (ConsumerData c: data.getConsumers())
        {
            if (null == c) // no events were added to this user
                continue;
            
            // are there any new events for this user, otherwise continue
            if (null == c.events[eventIndex])
                continue;

            final int [] items = c.events[eventIndex].getProductIndices();
            final ConsumerMetaEventsData meta = c.events[eventIndex].getMeta();
            final long [] dates = meta.getLongValuesArray(data.getEventsDescriptor(BUYSNAME).getMetaIndex("date"));
            countProducts(items, dates, newCounts, meta, lastAllowedTimestamp);
        }
        
        logger.info("Incremental update of CountViewsAttributeTimeWindowPredictor ended.");
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
        logger.error("getSimilarProducts for CountViewsAttributeTimeWindowPredictor does not make sense; should not be called!");
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
        logger.error("getBestPair not implemented yet for CountViewsAttributeTimeWindowPredictor.");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumer_index) {
        logger.error("needsProfiling not implemented yet for CountViewsAttributeTimeWindowPredictor.");
        return false;
    }

    @Override
    public Commitable updateConsumer(DataStore.UpdateData updateData) {
        logger.error("updateConsumer is deprecated; should not be called!");
        return null;
    }

	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		out.writeObject(counts);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        // update data
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = data;
        }		
        logger.info("updateModelFromFile of CountViewsAttributeTimeWindowPredictor started.");
        TIntObjectMap<TIntIntMap> newCounts = (TIntObjectMap<TIntIntMap>) in.readObject();
        logger.info("updateModelFromFile of CountViewsAttributeTimeWindowPredictor ended.");
        return new UpdateAll(newCounts);		
	}    
	

    private class UpdateAll implements Commitable {
        final TIntObjectMap<TIntIntMap> newCounts;
    
        UpdateAll(final TIntObjectMap<TIntIntMap> newCounts) {
            this.newCounts = newCounts;
        }

        @Override
        public void commit() {
            counts = newCounts;
        }        
    }       

}
