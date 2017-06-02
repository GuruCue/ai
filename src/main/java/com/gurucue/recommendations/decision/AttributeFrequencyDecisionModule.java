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
package com.gurucue.recommendations.decision;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.Attr;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.TIntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;

/**
 * The decision module takes track of all channels that were ever seen the user and counts  
 * 
 */
public class AttributeFrequencyDecisionModule extends DecisionModule implements Serializable {
    private static final long serialVersionUID = 1L;
    
    final int ATTRIBUTE_ID;
    final String EVENTS_NAME;
    
    //AttributeFinder attrFinder;    
    List<TIntIntMap> consumerFreqs; // sets of watched channels for each consumer
    
    public AttributeFrequencyDecisionModule(String name, Settings settings) {
        super(name, settings);
        
        ATTRIBUTE_ID = settings.getSettingAsInt(name + "_ATTRIBUTE_ID");
        EVENTS_NAME = settings.getSetting(name + "_EVENTS_NAME");
    }    
    
    @Override
    public ProductRating[] selectBestCandidates(List<ProductRating> candidates,
                                                int maxCandidates, boolean randomizeResults,
                                                Map<String, String> tags) {
        if (candidates == null || candidates.size() == 0) {
            return null;
        }
        
        // define weights
        TFloatArrayList weights = new TFloatArrayList();  
        for (ProductRating pr: candidates)
        {
            // get attribute values
            Attr a = data.getAttr(pr.getProductIndex(), ATTRIBUTE_ID);
            TIntSet values = a.getValues();
            float mult = 1;
            for (TIntIterator it = values.iterator(); it.hasNext();)
            {
                final int val = it.next();
                mult *= consumerFreqs.get(pr.getConsumerIndex()).get(val) + 1;
            }
            if (values.size() > 0)
            {
                mult = (float) Math.pow(mult, 1.0 / values.size());
            }
            // compute weight
            weights.add((float) pr.getPrediction() *  mult);
        }

        return selectSubset(candidates, weights, maxCandidates, randomizeResults);    
    }

    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData updateData) {
     // update data
        if (this.data == null) // initialization of data is needed
        {
            this.data = updateData.dataStore;
        }

        final int eventIndex = data.getEventsDescriptor(EVENTS_NAME).index;     

        // new (changed) consumers
        final ArrayList<ConsumerData> tmp_consumers = updateData.newData;
        final TLongIntHashMap tmp_ids = updateData.newConsumerIDs;
        // a map of all changes
        HashMap<Integer, TIntIntMap> tmp_freqs = new HashMap<Integer, TIntIntMap> ();
        for (ConsumerData c: tmp_consumers)
        {
            if (null == c) // no events were added to this user
                continue;
            
            // are there any new events for this user, otherwise continue
            if (null == c.events[eventIndex])
                continue;
            
            int cons_index = tmp_ids.get(c.consumerId);

            // Step 1: count all new occurences of attribute
            tmp_freqs.put(cons_index, new TIntIntHashMap());
            TIntIntMap cons_freqs = tmp_freqs.get(cons_index);
            int[] indices = c.events[eventIndex].getProductIndices();
            for (int index : indices)
            {
                Attr a = data.getAttr(index, ATTRIBUTE_ID);
                TIntSet values = a.getValues();
                for (TIntIterator it = values.iterator(); it.hasNext();)
                {
                    final int val = it.next();
                    cons_freqs.put(val, cons_freqs.get(val) + 1);
                }
            }
        }
        return new UpdateDelta(tmp_freqs);
    }
    
	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		out.writeObject(consumerFreqs);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        if (this.data == null) // initialization of data is needed
        {
            this.data = data;
        }		
	    List<TIntIntMap> newConsumerFreqs = (List<TIntIntMap>) in.readObject();
	    return new UpdateAll(newConsumerFreqs);
	}
    

    @Override
    public Commitable updateProducts(UpdateProductsDelta delta) {
        return new DoNothingUpdate(delta.dataStore);
    }
    
    private class UpdateDelta implements Commitable {
        final HashMap<Integer, TIntIntMap> tmp_freqs;
        
        UpdateDelta(final HashMap<Integer, TIntIntMap> tmp_freqs) {
            this.tmp_freqs = tmp_freqs;
        }

        @Override
        public void commit() {
            for (Map.Entry<Integer, TIntIntMap> entry : tmp_freqs.entrySet()) {
                final int key = entry.getKey();
                while (consumerFreqs.size() <= key)
                {
                    consumerFreqs.add(null);
                }
                TIntIntMap freqs = consumerFreqs.get(key);
                if (null == freqs)
                {
                    freqs = new TIntIntHashMap();
                    consumerFreqs.set(key, freqs);
                }
                final TIntIntMap tmp_values = entry.getValue();
                for (TIntIntIterator it = tmp_values.iterator(); it.hasNext(); )
                {
                    it.advance();
                    final int key2 = it.key();
                    final int value = it.value();
                    freqs.put(key, freqs.get(key2)+value);
                }
            }
        }
    }
    
    private class UpdateAll implements Commitable {
        final List<TIntIntMap> newConsumerFreqs;
        
        UpdateAll(final List<TIntIntMap> newConsumerFreqs) {
            this.newConsumerFreqs = newConsumerFreqs;
        }

        @Override
        public void commit() {
        	consumerFreqs = newConsumerFreqs;
        }
    }    
}
