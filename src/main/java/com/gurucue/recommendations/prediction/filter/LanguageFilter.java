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
import com.gurucue.recommendations.recommender.dto.TagsManager;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TObjectFloatIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TObjectFloatMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;
import com.gurucue.recommendations.recommender.dto.MultiValAttr;


/**
 * This filter will remove any channels using languages that this consumer is not watching. 
 */

public class LanguageFilter extends Predictor implements Cloneable {
    private static final long serialVersionUID = 1L;

    private final Logger logger;
    private final String LANGUAGE_QUERY;
    private final int CHANNEL_ATT;
    private final String EVENTS_NAME;
    
    // measured statistics
    Map<String, String> langs;
	TObjectIntMap<String> langMap;
	TIntObjectMap<TIntFloatMap> consLangs;
    
    // the data
    private DataStore data;
    
    private long lastUpdate;
    
    public LanguageFilter(String name, Settings settings) {
        super(name, settings);
        logger = LogManager.getLogger(LanguageFilter.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        LANGUAGE_QUERY = settings.getSetting(name + "_LANGUAGE_QUERY");
        EVENTS_NAME = settings.getSetting(name + "_EVENTS_NAME");
        CHANNEL_ATT = settings.getSettingAsInt(name + "_CHANNEL_ATT");
        lastUpdate = 0;
    }
    
    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) {
    	if (predictions.size() == 0)
    		return;
    	
    	// this map stores all language probabilities for this consumer
    	TIntSet langIDs = new TIntHashSet();
    	TIntSet acceptedIDs = new TIntHashSet();
		for (TObjectIntIterator<String> it = langMap.iterator(); it.hasNext(); )
		{
			it.advance();
			if (it.key().equalsIgnoreCase("null") || it.key().equalsIgnoreCase("nullnull"))
				acceptedIDs.add(it.value());
			if (it.key().indexOf("sl") != -1)
				acceptedIDs.add(it.value());
		}
		
    	// consumers frequencies
    	final TIntFloatMap freq = consLangs.get(predictions.get(0).getConsumerIndex());
    	if (freq == null || freq.size() == 0)
    		return;
    	
    	// sum of probs
    	double sumProbs = 0.0;
    			
    	// First add a tag to each prediction (according to its language)
    	for (ProductRating pr : predictions)
    	{
    		final int it = pr.getProductIndex();
			MultiValAttr channel = (MultiValAttr) data.getAttr(it, CHANNEL_ATT);
			if (channel == null)
				continue;
			final int[] vals = channel.getValuesArray();
			if (vals.length == 0)
				continue;
			
			String strChannel = channel.getKey(vals[0]);
			String slangs = langs.get(strChannel);
			final int langID = langMap.get(slangs);
			pr.setTag("languagefilter_"+langID, "");
			// TODO: add this to global computations; it shall work faster
			// TODO: also remove "sl" from code since it is language specific, should be in settings
			langIDs.add(langID);
    	}
    	
    	// compute sumprobs
    	float sumProb = 0;
    	for (TIntIterator it = langIDs.iterator(); it.hasNext(); )
    	{
    		final int key = it.next();
    		sumProbs += (freq.get(key)+3) / (freq.get(-1)+3);
    	}
    	
    	// then add soft contraints to tags
    	final int nrecommend = Integer.parseInt(tags.get(TagsManager.MAX_RECOMMEND_TAG));
    	
    	for (TIntIterator it = langIDs.iterator(); it.hasNext(); )
    	{
    		final int key = it.next();
    		
    		if (acceptedIDs.contains(key))
    			continue;
    		
    		// now set constraint for this value
    		if (!tags.containsKey(TagsManager.SECONDARY_TAG) || tags.get(TagsManager.SECONDARY_TAG).equals(""))    		
    			tags.put(TagsManager.SECONDARY_TAG, "languagefilter_"+key);
    		else
    			tags.put(TagsManager.SECONDARY_TAG, tags.get(TagsManager.SECONDARY_TAG)+";"+"languagefilter_"+key);
    		int nr = (int) Math.round((freq.get(key)+3) / (freq.get(-1)+3) * nrecommend) + 1;
    		if (freq.get(key) == 0)
    			nr = 0;
    		tags.put("MAX_ITEMS_" + "languagefilter_"+key, String.valueOf(nr));    		
    	}
    }    

    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int id_shift, Map<String,String> tags) {
    }
    
    
    /**
     * Increase counter for each bought product in each possible context. 
     */
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData updateData) {
        logger.info("Incremental update of LanguageFilter started.");
        final long currentTime = System.currentTimeMillis()/1000;

        if (this.data == null) // initialization of attributes is needed
        {
            this.data = updateData.dataStore;
        }
        
        final int eventIndex = data.getEventsDescriptor(EVENTS_NAME).index;
        
        Commitable update = EmptyCommit.INSTANCE;
        // time since last update is more than 24hour (86400 seconds), update model
        if (currentTime - lastUpdate > 86400)
        {
        	// do the update;
        	// first read all channels
        	Map<String, String> newLangs = getLangs();
        	TObjectIntMap<String> newLangMap = getLangMap(newLangs);
        	TIntObjectMap<TIntFloatMap> newConsLangs = new TIntObjectHashMap<TIntFloatMap> ();
        	    
        	// then compute all probabilities for all users
        	List<ConsumerData> cdata = data.getConsumers();
        	for (int ic = 0; ic < cdata.size(); ic++)
        	{
        		final ConsumerData cons = cdata.get(ic);
        		if (cons.events == null || cons.events[eventIndex] == null)
        			continue;
        		
        		// store statistics
        		TIntFloatMap freq = new TIntFloatHashMap();
        		
        		final int [] items = cons.events[eventIndex].getProductIndices();
        		for (int it : items)
        		{
        			MultiValAttr channel = (MultiValAttr) data.getAttr(it, CHANNEL_ATT);
        			if (channel == null)
        				continue;
        			int[] vals = channel.getValuesArray();
        			// if it has no values, skip it
        			if (vals.length == 0)
        				continue;
        			String strChannel = channel.getKey(vals[0]);
        			String slangs = newLangs.get(strChannel);
        			final int langID = newLangMap.get(slangs);
        			freq.put(langID, freq.get(langID) + 1);
        			freq.put(-1, freq.get(-1) + 1);
        		}
        		newConsLangs.put(ic, freq);
        	}
        	
        	lastUpdate = currentTime;
            update = new UpdateDelta(newLangs, newLangMap, newConsLangs);
        }	
        logger.info("Incremental update of LanguageFilter ended.");
        return update;
    }
    
    private Map<String, String> getLangs()
    {
    	return data.getReader().getLanguages(LANGUAGE_QUERY);
    }
    
    private TObjectIntMap<String> getLangMap(Map<String, String> lang)
    {
    	TObjectIntMap<String> newLangMap = new TObjectIntHashMap<String> ();
    	int counter = 0;
    	for (Map.Entry<String, String> entry : lang.entrySet()) {
    		newLangMap.put(entry.getValue(), counter++);
    	}
    	return newLangMap;
    }

    @Override
    public ProductPair getBestPair(int consumerIndex) {
        logger.error("Get best pair not implemented for LanguageFilter");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumerIndex) {
        logger.error("Needs profiling not implemented for LanguageFilter");
        return false;
    }

    @Override
    public void updateModelQuick(DataStore data) {
        logger.error("UpdateModelQuick not implemented for LanguageFilter");
    }
    

    @Override
    public Commitable updateProducts(UpdateProductsDelta delta) {
        return EmptyCommit.INSTANCE;
    }
    
	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        logger.info("Update model from file of LanguageFilter started.");
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = data;
        }
    	Map<String, String> newLangs = getLangs();
    	TObjectIntMap<String> newLangMap = getLangMap(newLangs);
    	TIntObjectMap<TIntFloatMap> newConsLangs = new TIntObjectHashMap<TIntFloatMap> ();
        
        logger.info("Update model from file of LanguageFilter ended.");
		return new UpdateDelta(newLangs, newLangMap, newConsLangs);
	}

	@Override
	public void updateModel(DataStore data) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Commitable updateConsumer(UpdateData newData) {
		// TODO Auto-generated method stub
		return null;
	}    
	
    private class UpdateDelta implements Commitable {
    	Map<String, String> newLangs;
    	TObjectIntMap<String> newLangMap;
    	TIntObjectMap<TIntFloatMap> newConsLangs;

        UpdateDelta(Map<String, String> newLangs, TObjectIntMap<String> newLangMap, TIntObjectMap<TIntFloatMap> newConsLangs) {
        	this.newLangs = newLangs;
        	this.newLangMap = newLangMap;
        	this.newConsLangs = newConsLangs;
        }

        @Override
        public void commit() {
        	langs = newLangs;
        	langMap = newLangMap;
        	consLangs = newConsLangs;
        }
    }	
}
