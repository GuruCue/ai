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

import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
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
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.dto.ConsumerMetaEventsData;
import com.gurucue.recommendations.recommender.dto.ContextDiscretizer;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.ProductData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;


/**
 * A recommender that returns the number of times a product was bought.
 */
public class CountViewsPredictor extends Predictor {
    private final Logger logger;
    
    // the name of the event
    private final String BUYSNAME;
    // name of contexts
    String [] CONTEXT_NAMES;
    // indices of contexts in meta data
    int [] CONTEXT_METAS_INDICES;
    // attribute indices of contexts
    int [] CONTEXT_ATTRIBUTES;
    
    // context handler
    ContextDiscretizer contextHandler;
    
    // averages of prod
    List<TIntIntMap> productCounts;
    
    
    private DataStore data;
    
    public CountViewsPredictor(String name, Settings settings) {
        super(name, settings);
        
        logger = LogManager.getLogger(CountViewsPredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        BUYSNAME = settings.getSetting(name + "_BUYS");

        CONTEXT_NAMES = settings.getAsStringArray(name + "_CONTEXT_NAMES");
        CONTEXT_METAS_INDICES = settings.getAsIntArray(name + "CONTEXT_METAS_INDICES");
        CONTEXT_ATTRIBUTES = settings.getAsIntArray(name + "CONTEXT_ATTRIBUTES");
        
        // initialize context handler
        contextHandler = new ContextDiscretizer(name, settings);
    }

    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) 
    {
        float predValues = 0;
        float predValuesSqr = 0;
        final int N = predictions.size();
        for (ProductRating pr : predictions)
        {
            final int productIndex = pr.getProductIndex();
            if (productIndex >= productCounts.size() || productIndex < 0)
            {
                // no records of this product -> return 0
                addProductRating(pr, 0, "");
                continue;
            }
            TIntList contextPositions = contextHandler.getContext(data, productIndex, tags);
            float averageCount = 1;
            int counter = 0;
            final TIntIntMap counts = productCounts.get(productIndex);
            if (counts == null)
            {
                addProductRating(pr, 1, "");
                continue;
            }
            for (TIntIterator it = contextPositions.iterator(); it.hasNext();) 
            {
                averageCount += productCounts.get(productIndex).get(it.next());
                counter += 1;
            }
            final float pred = averageCount/counter;
            predValues += pred;
            predValuesSqr += pred*pred;
            
            addProductRating(pr, pred, "");
        }
        final double threshold = predValues / N + Math.sqrt(predValuesSqr/N + Math.pow(predValues/N,2));
        for (ProductRating pr : predictions)
        {
            final double p = pr.getPrediction();
            if (p > threshold)
                pr.setExplanation(this.ID, "Number of views for event " + BUYSNAME + " is high:" + p + ";");
        }
    }

    /**
     * Increase counter for each bought product in each possible context. 
     */
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData updateData) {
        logger.info("Incremental update of CountViewsPredictor started.");
        // update data
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = updateData.dataStore;
            productCounts = new ArrayList<TIntIntMap> ();
        }
        final int eventIndex = data.getEventsDescriptor(BUYSNAME).index;

        // new (changed) consumers
        final List<ConsumerData> tmp_consumers = updateData.newData;

        Map<Integer, TIntIntMap> newCounts = new HashMap<Integer, TIntIntMap>();

        for (ConsumerData c: tmp_consumers)
        {
            if (null == c) // no events were added to this user
                continue;
            
            // are there any new events for this user, otherwise continue
            if (null == c.events[eventIndex])
                continue;

            final int [] items = c.events[eventIndex].getProductIndices();
            final ConsumerMetaEventsData meta = c.events[eventIndex].getMeta();
            for (int i = 0; i < items.length; i++)
            {
                final int item = items[i];
                if (!newCounts.containsKey(item))
                {
                    newCounts.put(item, new TIntIntHashMap());
                }
                final TIntIntMap productCounts = newCounts.get(item);
                final TIntList contextPositions = contextHandler.getContext(data, item, meta, i);
                
                for (TIntIterator it = contextPositions.iterator(); it.hasNext(); )
                {
                    final int pos = it.next();
                    productCounts.put(pos, productCounts.get(pos) + 1);
                }
            }
        }
        
        
        logger.info("Incremental update of CountViewsPredictor ended.");
        return new UpdateDelta(newCounts);
    }

    /**
     * Remove nonrelevant products / add new products. 
     */
    @Override
    public Commitable updateProducts(DataStore.UpdateProductsDelta delta) {
        // create new product counts 
        List<TIntIntMap> newProductCounts = new ArrayList<TIntIntMap> ();
        final TLongIntHashMap newProductIDs = delta.newProductIDs;
        
        final int counts_len = productCounts.size();
        for (int i = 0; i < counts_len; i++)
        {
            final ProductData pd = data.getProductByIndex(i);
            final int newIndex = newProductIDs.get(pd.productId);
            if (newIndex < 0)
                continue;
            
            while (newProductCounts.size() <= newIndex)
                newProductCounts.add(null);
            
            newProductCounts.set(newIndex, productCounts.get(i));
        }
        return new UpdateProductsDelta(newProductCounts, delta.dataStore);
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
        logger.error("getBestPair not implemented yet for CountViewsPredictor.");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumer_index) {
        logger.error("needsProfiling not implemented yet for CountViewsPredictor.");
        return false;
    }

    @Override
    public Commitable updateConsumer(DataStore.UpdateData updateData) {
        logger.error("updateConsumer is deprecated; should not be called!");
        return null;
    }

	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		out.writeObject(productCounts);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        logger.info("updateModelFromFile of CountViewsPredictor started.");
        // update data
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = data;
            productCounts = new ArrayList<TIntIntMap> ();
        }        
        List<TIntIntMap> newCounts = (List<TIntIntMap>) in.readObject();
        
        logger.info("updateModelFromFile of CountViewsPredictor ended.");
        return new UpdateAll(newCounts);		
	}      

    private class UpdateDelta implements Commitable {
        final Map<Integer, TIntIntMap> newCounts;
    
        UpdateDelta(final Map<Integer, TIntIntMap> newCounts) {
            this.newCounts = newCounts;
        }

        @Override
        public void commit() {
            for (Map.Entry<Integer, TIntIntMap> entry : newCounts.entrySet()) {
                final int key = entry.getKey();
                while (productCounts.size() <= key)
                {
                    productCounts.add(null);
                }
                final TIntIntMap tmp_values = entry.getValue();
                TIntIntMap values = productCounts.get(key);
                if (values == null)
                {
                    values = new TIntIntHashMap();
                    productCounts.set(key, values);
                }
                for (TIntIntIterator it = tmp_values.iterator(); it.hasNext(); )
                {
                    it.advance();
                    final int pos = it.key();
                    final int val = it.value();
                    values.put(pos, values.get(pos)+val);
                }
                
            }
        }        
    }
    
    private class UpdateAll implements Commitable {
        final List<TIntIntMap> newCounts;
    
        UpdateAll(final List<TIntIntMap> newCounts) {
            this.newCounts = newCounts;
        }

        @Override
        public void commit() {
            productCounts = newCounts;
        }        
    }    
    
    
    private class UpdateProductsDelta implements Commitable {
        private final List<TIntIntMap> newProductCounts;
        private final DataStore newData;
    
        UpdateProductsDelta(final List<TIntIntMap> newProductCounts, final DataStore newData) {
            this.newProductCounts = newProductCounts;
            this.newData = newData;
        }

        @Override
        public void commit() {
            data = newData;
            productCounts = newProductCounts;

        }        
    }    

}
