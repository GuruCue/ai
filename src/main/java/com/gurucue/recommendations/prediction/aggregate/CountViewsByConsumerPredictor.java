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
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ConsumerMetaEventsData;
import com.gurucue.recommendations.recommender.dto.ProductData;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.TIntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.ContextDiscretizer;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;


/**
 * A recommender that counts the number of times a product was bought by a consumer. It returns a proportion of bought products in a given context.
 * 
 * Assumption: there are at most MAX_PRODUCTS products and MAX_CONTEXT contexts.
 */
public class CountViewsByConsumerPredictor extends Predictor {
    private final Logger logger;
    
    // number of products will never exceed this value
    private final int MAX_PRODUCTS;
    // number of contexts will never exceed this value
    private final int MAX_CONTEXTS;
    // number of consumers will never ...
    private final long MAX_CONSUMERS;
   
    
    // M used in m-estimate
    private final float M;
    
    // the name of the event
    private final String BUYSNAME;
    
    // context handler
    ContextDiscretizer contextHandler;
    
    // averages of prod
    TLongIntMap productCounts;

    private DataStore data;
    
    public CountViewsByConsumerPredictor(String name, Settings settings) {
        super(name, settings);
        
        logger = LogManager.getLogger(CountViewsByConsumerPredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        BUYSNAME = settings.getSetting(name + "_BUYS");

        MAX_PRODUCTS = settings.getSettingAsInt(name + "_MAX_PRODUCTS");
        MAX_CONTEXTS = settings.getSettingAsInt(name + "_MAX_CONTEXTS");
        MAX_CONSUMERS = settings.getSettingAsLong(name + "_MAX_CONSUMERS");
        
        M = settings.getSettingAsFloat(name + "_M");
        
        // initialize context handler
        contextHandler = new ContextDiscretizer(name, settings);
    }

    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) 
    {
        final float all = productCounts.get(MAX_CONSUMERS * MAX_PRODUCTS * MAX_CONTEXTS + (MAX_PRODUCTS-1) * MAX_CONTEXTS);

        for (ProductRating pr : predictions)
        {
            final int productIndex = pr.getProductIndex();
            final long consumerIndex = pr.getConsumerIndex();
            TIntList contextPositions = contextHandler.getContext(data, productIndex, tags);
            
            // get the average number of times this product was bought
            float averageCount = 0, averageCountAll = 0;
            int counter = 0;
            for (TIntIterator it = contextPositions.iterator(); it.hasNext();) 
            {
                final int context = it.next();
                final long key = consumerIndex * MAX_PRODUCTS * MAX_CONTEXTS + productIndex * MAX_CONTEXTS + context;
                final long keyAll = consumerIndex * MAX_PRODUCTS * MAX_CONTEXTS + (MAX_PRODUCTS-1) * MAX_CONTEXTS + context;
                averageCount += productCounts.get(key);
                averageCountAll += productCounts.get(keyAll);
                counter += 1;
            }
            if (counter == 0)
                counter = 1;
            final float n = averageCount / counter;
            final float N = averageCountAll / counter;
            
            final float allConsumer = productCounts.get(consumerIndex * MAX_PRODUCTS * MAX_CONTEXTS + (MAX_PRODUCTS-1) * MAX_CONTEXTS);
            final float allProductConsumer = productCounts.get(consumerIndex * MAX_PRODUCTS * MAX_CONTEXTS + productIndex * MAX_CONTEXTS);
            final float allProduct = productCounts.get(MAX_CONSUMERS * MAX_PRODUCTS * MAX_CONTEXTS + productIndex * MAX_CONTEXTS);
            final float p0 = (allProductConsumer + M * (allProduct/all)) / (allConsumer + M);
            
            addProductRating(pr, (n + M*p0)/(N + M), "");
        }
    }

    /**
     * Increase counter for each bought product in each possible context. 
     */
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData updateData) {
        logger.info("Incremental update of CountViewsByConsumerPredictor started.");
        // update data
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = updateData.dataStore;
            productCounts = new TLongIntHashMap ();
        }
        final int eventIndex = data.getEventsDescriptor(BUYSNAME).index;

        // new (changed) consumers
        final List<ConsumerData> tmp_consumers = updateData.newData;
        final TLongIntHashMap tmp_ids = updateData.newConsumerIDs;

        TLongIntMap newCounts = new TLongIntHashMap (productCounts);

        for (ConsumerData c: tmp_consumers)
        {
            if (null == c) // no events were added to this user
                continue;
            
            // are there any new events for this user, otherwise continue
            if (null == c.events[eventIndex])
                continue;

            final long consIndex = tmp_ids.get(c.consumerId);
            final int [] items = c.events[eventIndex].getProductIndices();
            final ConsumerMetaEventsData meta = c.events[eventIndex].getMeta();
            for (int i = 0; i < items.length; i++)
            {
                final int item = items[i];
                final TIntList contextPositions = contextHandler.getContext(data, item, meta, i);
                
                for (TIntIterator it = contextPositions.iterator(); it.hasNext(); )
                {
                    final int pos = it.next();
                    final long key = consIndex * MAX_PRODUCTS * MAX_CONTEXTS + item * MAX_CONTEXTS + pos;
                    final long keyAll = consIndex * MAX_PRODUCTS * MAX_CONTEXTS + (MAX_PRODUCTS-1) * MAX_CONTEXTS + pos;
                    newCounts.put(key, newCounts.get(key) + 1);
                    newCounts.put(keyAll, newCounts.get(keyAll) + 1);
                }
                
                // store also general counter (in MAX_CONSUMERS)
                final long key = MAX_CONSUMERS * MAX_PRODUCTS * MAX_CONTEXTS + item * MAX_CONTEXTS;
                final long keyAll = MAX_CONSUMERS * MAX_PRODUCTS * MAX_CONTEXTS + (MAX_PRODUCTS-1) * MAX_CONTEXTS;
                newCounts.put(key, newCounts.get(key) + 1);
                newCounts.put(keyAll, newCounts.get(keyAll) + 1);
                
            }
        }
        
        logger.info("Incremental update of CountViewsByConsumerPredictor ended.");
        return new UpdateDelta(newCounts);
    }

    /**
     * Remove nonrelevant products / add new products. 
     */
    @Override
    public Commitable updateProducts(DataStore.UpdateProductsDelta delta) {
        // create new product counts 
        TLongIntMap newProductCounts = new TLongIntHashMap ();
        final TLongIntHashMap newProductIDs = delta.newProductIDs;
        
        for (TLongIntIterator it = productCounts.iterator(); it.hasNext(); )
        {
            it.advance();
            final long key = it.key();
            final long cons = key / (MAX_PRODUCTS * MAX_CONTEXTS);
            final long rem = key % (MAX_PRODUCTS * MAX_CONTEXTS);
            final long prod = rem / MAX_CONTEXTS;
            final long cont =  rem % MAX_CONTEXTS;
            
            int newIndex;
            if (prod == MAX_PRODUCTS - 1)
            {
                newIndex = MAX_PRODUCTS - 1;   
            }
            else
            {
                final ProductData pd = data.getProductByIndex((int) prod);
                newIndex = newProductIDs.get(pd.productId);
            }
            
            final long newKey = cons * MAX_PRODUCTS * MAX_CONTEXTS + newIndex * MAX_CONTEXTS + cont;
            newProductCounts.put(newKey, it.value());
        }
        return new UpdateProductsDelta(newProductCounts, delta.dataStore);
    }
    
    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int predictionId,
            Map<String, String> tags) {
    }
    
	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		out.writeObject(productCounts);
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        logger.info("updateModelFromFile of CountViewsByConsumerPredictor started.");
        // update data
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = data;
            productCounts = new TLongIntHashMap ();
        }        
        TLongIntMap newCounts = (TLongIntMap) in.readObject();
        logger.info("updateModelFromFile of CountViewsByConsumerPredictor ended.");
        return new UpdateDelta(newCounts);		
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
        logger.error("getBestPair not implemented yet for CountViewsByConsumerPredictor.");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumer_index) {
        logger.error("needsProfiling not implemented yet for CountViewsByConsumerPredictor.");
        return false;
    }

    @Override
    public Commitable updateConsumer(DataStore.UpdateData updateData) {
        logger.error("updateConsumer is deprecated; should not be called!");
        return null;
    }


    private class UpdateDelta implements Commitable {
        final TLongIntMap newCounts;
    
        UpdateDelta(final TLongIntMap newCounts) {
            this.newCounts = newCounts;
        }

        @Override
        public void commit() {
        	productCounts = newCounts;
        }        
    }
    
    
    private class UpdateProductsDelta implements Commitable {
        private final TLongIntMap newProductCounts;
        private final DataStore newData;
    
        UpdateProductsDelta(final TLongIntMap newProductCounts, final DataStore newData) {
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
