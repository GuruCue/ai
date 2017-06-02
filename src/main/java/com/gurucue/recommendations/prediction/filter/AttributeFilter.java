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

import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntLongIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntLongHashMap;
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

import com.gurucue.recommendations.misc.Misc;
import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ConsumerMetaEventsData;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;

/**
 * This recommender stores all attributes of bought products in a set. During recommendation it filters out
 * all products that have any of the attributes among bought products. This recommender is mainly used to filter out already bought products. 
 * 
 * For each stored product value the filter stores also the date when this value occured the last time. 
 */
public class AttributeFilter extends Predictor {
    private final Logger logger;
    
    // the name of the event
    private final String [] EVENTSNAMES;
    
    // next predictor (that uses filtered events)
    private final Predictor PREDICTOR;
    

    // attribute id
    private final int ATTRIBUTE;
    
    // time windows of ignored products
    private final long [] TIME_WINDOW;
    
    // max items we can store for a consumer
    private final long MAX_CONSUMER_ITEMS;
    
    // data about attributes from bought products
    TIntObjectMap<TIntSet> attributeData;
    TIntObjectMap<TIntLongMap> attributeDates;
    
    private DataStore data;
    
    public AttributeFilter(String name, Settings settings) {
        super(name, settings);
        
        logger = LogManager.getLogger(AttributeFilter.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        EVENTSNAMES = settings.getAsStringArray(name + "_EVENTS");

        // create predictor
        String predictorName = settings.getSetting(name + "_PREDICTOR");
        PREDICTOR = (Predictor) Misc.createClassObject(settings.getSetting(predictorName+"_CLASS"), predictorName, settings);

        ATTRIBUTE = settings.getSettingAsInt(name + "_ATTRIBUTE");
        TIME_WINDOW = settings.getAsLongArray(name + "_TIME_WINDOW");
        MAX_CONSUMER_ITEMS = settings.getSettingAsLong(name + "_MAX_CONSUMER_ITEMS");
    }

    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) 
    {
        final TIntSet cad = attributeData.get(predictions.get(0).getConsumerIndex());
        if (cad != null)
        {
	        List<ProductRating> newCandidates = new ArrayList<ProductRating> ();
	        for (ProductRating pr : predictions)
	        {
	        	// get attribute values
	        	final TIntSet attr = pr.getProductData().getValues(ATTRIBUTE);
	        	if (attr == null)
	        		continue;
	        	boolean contains = false;
	        	// if cad contains any of the attribute values, continue
	        	for (TIntIterator it = attr.iterator(); it.hasNext(); )
	        	{
	        		final int ati = it.next();
	        		if (cad.contains(ati))
	        		{
	        			contains = true;
	        			break;
	        		}
	        	}
	            // otherwise add to newCandidates
	        	if (!contains)
	        		newCandidates.add(pr);
	        }
	        
	        // now prepare new predictions
	        predictions.clear();
	
	        for (ProductRating pr : newCandidates)
	        	predictions.add(pr);
        }
        
        if (predictions.size() == 0)
        	return;
        
        
        PREDICTOR.getPredictionsTime(predictions, tags);
    }

    /**
     * Increase counter for each bought product in each possible context. 
     */
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData updateData) {
        logger.info("Incremental update of AttributesFilter started.");
        // update data
        if (this.data == null) // initialization is needed
        {
            this.data = updateData.dataStore;
            attributeData = new TIntObjectHashMap<TIntSet> ();
            attributeDates = new TIntObjectHashMap<TIntLongMap>();            
        }

        final List<ConsumerData> new_consumers = updateData.newData;
        
        // create a copy of attribute data
        TIntObjectMap<TIntSet> newAttributeData = new TIntObjectHashMap<TIntSet>();
        for (TIntObjectIterator<TIntSet> it = attributeData.iterator(); it.hasNext(); )
        {
        	it.advance();
        	final int key = it.key();
        	final TIntSet value = it.value();
        	
        	newAttributeData.put(key, new TIntHashSet(value));
        }
        
        // create a copy of dates data
        TIntObjectMap<TIntLongMap> newAttributeDates = new TIntObjectHashMap<TIntLongMap>();
        for (TIntObjectIterator<TIntLongMap> it = attributeDates.iterator(); it.hasNext(); )
        {
        	it.advance();
        	final int key = it.key();
        	final TIntLongMap value = it.value();
        	
        	newAttributeDates.put(key, new TIntLongHashMap(value));
        }
        
        for (int event_counter = 0; event_counter < EVENTSNAMES.length; event_counter ++)
        {
        	final String event = EVENTSNAMES[event_counter]; 
	        final int eventIndex = data.getEventsDescriptor(event).index;
	        final TLongIntHashMap tmp_ids = updateData.newConsumerIDs;
	        final long maximalAllowedDate = data.getEventsDescriptor(event).last_read_date;
	
	        for (ConsumerData c: new_consumers)
	        {

	        	if (c != null)
	        	{
		        	final int consumerIndex = tmp_ids.get(c.consumerId);
	        		if (c.events[eventIndex] != null) {
	        			final ConsumerMetaEventsData meta = c.events[eventIndex].getMeta();
	        			final long [] dates = meta.getLongValuesArray(data.getEventsDescriptor(event).getMetaIndex("date"));	        		
	        			addToConsumer(consumerIndex, c.events[eventIndex].getProductIndices(), dates, newAttributeData, newAttributeDates, TIME_WINDOW[event_counter]);
	        		}
		        	deleteFromConsumer(consumerIndex, newAttributeData, newAttributeDates, maximalAllowedDate);
	        	}
	        }
        }
        logger.info("Incremental update of AttributesFilter ended.");
        Commitable c = PREDICTOR.updateModelIncremental(updateData);
        return new UpdateDelta(newAttributeData, newAttributeDates, c);
    }


    private void addToConsumer(final int consumerIndex, int [] productIndices, long [] productDates, TIntObjectMap<TIntSet> attributeData, TIntObjectMap<TIntLongMap> attributeDates, long time_window) 

    {
    	if (consumerIndex < 0)
    		return;
    	if (!attributeData.containsKey(consumerIndex))
    	{
    		attributeData.put(consumerIndex, new TIntHashSet());
    		attributeDates.put(consumerIndex, new TIntLongHashMap());
    	}
    	if (!attributeDates.containsKey(consumerIndex))
    	{
    		attributeDates.put(consumerIndex, new TIntLongHashMap());
    	}
    	final TIntLongMap consDates = attributeDates.get(consumerIndex);
    	TIntSet cad = attributeData.get(consumerIndex);
    	
    	for (int i = 0; i < productIndices.length; i++)
    	{
    		final int index = productIndices[i];
    		final long date = productDates[i];

    		if (this.data.getProductByIndex(index) == null)
    		{
    			logger.error("Product by index " + index + " is null");
    			continue;
    		}
    		if (this.data.getProductByIndex(index).getValues(ATTRIBUTE) == null)
    		{
    			logger.error("Product by index " + index + " has null video");
    			continue;
    		}
    		

    		final TIntSet vals = this.data.getProductByIndex(index).getValues(ATTRIBUTE);
    		cad.addAll(vals);
    		for (TIntIterator it = vals.iterator(); it.hasNext(); )
    		{
    			final int val = it.next();
    			if (!consDates.containsKey(val) || (date + time_window > consDates.get(val)))
    				consDates.put(val, date + time_window);
    		}
    			
    	}
    }
    
    private void deleteFromConsumer(final int consumerIndex, TIntObjectMap<TIntSet> attributeData, TIntObjectMap<TIntLongMap> attributeDates, long maxAllowedDate)
    {
    	TIntSet cad = attributeData.get(consumerIndex);
    	TIntLongMap consDates = attributeDates.get(consumerIndex);
    	if (consDates == null || cad == null)
    		return;

    	// if cad size is too large, recompute maxAllowedDate to reach MAX_CONSUMER_ITEMS size
    	if (cad.size() > MAX_CONSUMER_ITEMS)
    	{
    		// put all dates in an array
    		long allDates [] = consDates.values();
    		
    		// sort them by date
    		Arrays.sort(allDates);
    		
    		// find date at MAX_CONSUMER_ITEMS index
    		maxAllowedDate = Math.max(maxAllowedDate, allDates[(int) (allDates.length - MAX_CONSUMER_ITEMS)]);
    	}
    	
    	for (TIntLongIterator it = consDates.iterator(); it.hasNext(); )
    	{
    		it.advance();
    		final int key = it.key();
    		final long date = it.value();
    		
    		if (date < maxAllowedDate)
    		{
    			cad.remove(key);
    			it.remove();

    		}
    	}
    }
        
        
    /**
     * Remove nonrelevant products / add new products. 
     */
    @Override
    public Commitable updateProducts(DataStore.UpdateProductsDelta delta) {
        Commitable c = PREDICTOR.updateProducts(delta);
        return new UpdateDelta(attributeData, attributeDates, c);
    }
    
    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int predictionId,
            Map<String, String> tags) {
    	// does nothing; no filter; just call the next predictor
    	PREDICTOR.getSimilarProductsTime(productIndices, predictions, predictionId, tags);
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
        logger.error("getBestPair not implemented yet for SeriesFilter.");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumer_index) {
        logger.error("needsProfiling not implemented yet for SeriesFilter.");
        return false;
    }

    @Override
    public Commitable updateConsumer(DataStore.UpdateData updateData) {
        logger.error("updateConsumer is deprecated; should not be called!");
        return null;
    }

	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		out.writeObject(attributeData);
		out.writeObject(attributeDates);
		PREDICTOR.serialize(out);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data) throws ClassNotFoundException, IOException {
        logger.info("Update model from file of AttributesFilter started.");
        if (this.data == null) // initialization is needed
        {
            this.data = data;
        }        
        
		TIntObjectMap<TIntSet> newAttributeData = (TIntObjectMap<TIntSet>) in.readObject();
        TIntObjectMap<TIntLongMap> newAttributeDates = (TIntObjectMap<TIntLongMap>) in.readObject();
        Commitable c = PREDICTOR.updateModelFromFile(in, data);
        logger.info("Update model from file of AttributesFilter ended.");
        return new UpdateDelta(newAttributeData, newAttributeDates, c);
	}
    

    private class UpdateDelta implements Commitable {
    	TIntObjectMap<TIntSet> newAttributeData;
        TIntObjectMap<TIntLongMap> newAttributeDates;
        Commitable predComm;
    
        UpdateDelta(final TIntObjectMap<TIntSet> newAttributeData, TIntObjectMap<TIntLongMap> newAttributeDates, Commitable predComm) {
            this.newAttributeData = newAttributeData;
            this.newAttributeDates = newAttributeDates;
            this.predComm = predComm;
        }

        @Override
        public void commit() {
        	attributeData = newAttributeData;
        	attributeDates = newAttributeDates;
            predComm.commit();
        }        
    }
}
