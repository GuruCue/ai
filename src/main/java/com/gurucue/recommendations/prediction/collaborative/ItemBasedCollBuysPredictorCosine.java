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
package com.gurucue.recommendations.prediction.collaborative;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.EmptyCommit;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.dto.ConsumerBuysData;
import com.gurucue.recommendations.recommender.dto.ConsumerMetaEventsData;
import com.gurucue.recommendations.recommender.dto.ContextDiscretizer;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.ProductData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;

/**
 * Item-based collaborative kNN for buys data. 
 */
public class ItemBasedCollBuysPredictorCosine extends Predictor {
    private final Logger logger;

    // name of events
    protected final String BUYSNAME;
    
    // similarity threshold for item to be used as similar
    private final float THRESHOLD; // 
    private final int RELEVANT_TIME; // number of days between two products that makes them still revelant for similarity
    private final float SIM_K; // blending factor in computing cosine similarity
    
    // ID of average predictor in product rating
    final int AVERAGE_ID;
    
    // id of timestamp meta
    final int TIMESTAMP_ID;
    
    private DataStore data; // the data source
    
    // similarities and indices
    List<TIntIntHashMap> similarities;
    List<TIntIntHashMap> contextSimilarities;
    TIntList productCounts; 
    
    // context handler
    final ContextDiscretizer contextHandler;

    // max difference in seconds for two products to be relevant
    final long maxDiffSec;
    
    public ItemBasedCollBuysPredictorCosine(String name, Settings settings) throws CloneNotSupportedException {
        super(name, settings);
        logger = LogManager.getLogger(ItemBasedCollBuysPredictorCosine.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        
        THRESHOLD = settings.getSettingAsFloat(name + "_THRESHOLD");
        BUYSNAME = settings.getSetting(name + "_BUYS");
        AVERAGE_ID = settings.getSettingAsInt(name + "_AVERAGE_ID");
        TIMESTAMP_ID = settings.getSettingAsInt(name+"_TIMESTAMP_META");
        RELEVANT_TIME = settings.getSettingAsInt(name+"_RELEVANT_TIME");
        SIM_K = settings.getSettingAsFloat(name+"_SIM_K");
        this.maxDiffSec = RELEVANT_TIME * 24 * 60 * 60;
        
        // initialize context handler
        contextHandler = new ContextDiscretizer(name, settings);        
    }
    

    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) {
        // user data
        ConsumerBuysData cbd = null;
        TIntSet us = null, all=null;
        int consumerIndex = -1;
        final int buysEventIndex = data.getEventsDescriptor(BUYSNAME).index;
        final long currentTimeSec = System.currentTimeMillis()/1000;

        for (ProductRating pr : predictions)
        {
            if (cbd == null || pr.getConsumerIndex() != consumerIndex)
            {
                consumerIndex = pr.getConsumerIndex();
                cbd = (ConsumerBuysData) data.getConsumerByIndex(consumerIndex).events[buysEventIndex];
                // get indices 
                all = null == cbd ? new TIntHashSet() : new TIntHashSet(cbd.indices);
                us = new TIntHashSet();
                if ((null != cbd) && (cbd.indices.length > 0))
                {
                    final long [] dates = cbd.getMeta().getLongValuesArray(TIMESTAMP_ID);
                    for (int i = cbd.indices.length-1; i>=0; i--)
                        if (dates[i] > currentTimeSec - maxDiffSec)
                            us.add(cbd.indices[i]);
                }
            }

            final int prod1_index = pr.getProductIndex();
            final TIntIntHashMap similarity = similarities.get(prod1_index);
            
            // weighted sum of all products that this user bought
            final StringBuilder explanation = new StringBuilder();
            double prediction = 1.0; //;
            for (TIntIterator it = us.iterator(); it.hasNext(); )
            {
                final int prod2_index = it.next();
                final double simpr = similarity.get(prod2_index) / Math.sqrt(productCounts.get(prod1_index) * productCounts.get(prod2_index));
                if (simpr > THRESHOLD)
                {
                    prediction += simpr;
                    explanation.append("based on id=");
                    explanation.append(data.getProducts().get(prod2_index).productId);
                    explanation.append(";");
                }
            }
            prediction *= pr.getPrediction(AVERAGE_ID);

            // add prediction to ProductRating
            addProductRating(pr, prediction, explanation.toString());
        }
    }
    
    private void addPair(final int product_index, final long time1, final TIntList context1, final int product_index2, final long time2, final TIntList context2, Map<Integer, TIntIntHashMap> newSimilarities, Map<Integer, TIntIntHashMap> contextSimilarities)
    {
        // is time difference small enough?
        if (Math.abs(time1-time2) > maxDiffSec)
            return;
        
        // similarities first
        TIntIntHashMap sim1, sim2, contSim1, contSim2;
        sim1 = newSimilarities.get(product_index);
        if (sim1 == null)
        {
            if (similarities.contains(product_index))
                sim1 = similarities.get(product_index);
            else
                sim1 = new TIntIntHashMap();
        }
        sim2 = newSimilarities.get(product_index2);
        if (sim2 == null)
        {
            if (similarities.contains(product_index2))
                sim2 = similarities.get(product_index2);
            else
                sim2 = new TIntIntHashMap();
        }
        
        sim1.put(product_index2, sim1.get(product_index2)+1);
        sim2.put(product_index, sim2.get(product_index)+1);
        
        newSimilarities.put(product_index, sim1);
        newSimilarities.put(product_index2, sim2);
    }
    
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData newData) {

        logger.info("Update item-based collaborative buys kNN started.");
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = newData.dataStore;
            similarities = new ArrayList<TIntIntHashMap>();
            contextSimilarities = new ArrayList<TIntIntHashMap>();
            productCounts = new TIntArrayList(this.data.getProducts().size());
            for (ProductData pr : this.data.getProducts())
            {
                productCounts.add(0);
                similarities.add(new TIntIntHashMap());
                contextSimilarities.add(new TIntIntHashMap());
            }
        }
        final int buysEventIndex = data.getEventsDescriptor(BUYSNAME).index;
        
        TIntArrayList newCounts = new TIntArrayList(productCounts);
        Map<Integer, TIntIntHashMap> newSimilarities = new HashMap<Integer, TIntIntHashMap>();
        Map<Integer, TIntIntHashMap> newContextSimilarities = new HashMap<Integer, TIntIntHashMap>();
        
        int user_index = -1;
        for (ConsumerData cd : newData.newData)
        {
            user_index++;
            if (null == cd)
                continue;
            
            // get events
            final ConsumerBuysData cbd = (ConsumerBuysData) cd.events[buysEventIndex];
            if ((null == cbd) || (cbd.indices.length == 0))
                continue;

            ConsumerBuysData old_cbd = null; 
            ConsumerMetaEventsData old_meta;
            long[] old_times;
            int ilen_old;
            TIntSet consumer_items;
            
            if (data.getConsumers().size() > user_index)
                old_cbd = (ConsumerBuysData) data.getConsumers().get(user_index).events[buysEventIndex];
            if (old_cbd != null)
            {
                old_meta = data.getConsumers().get(user_index).events[buysEventIndex].getMeta();
                old_times  = old_meta.getLongValuesArray(TIMESTAMP_ID);
                ilen_old = old_cbd.indices.length;
                consumer_items = new TIntHashSet(old_cbd.indices);
            }
            else
            {
                old_meta = null;
                old_times = null;
                ilen_old = 0;
                consumer_items = new TIntHashSet();
            }
            
            // get meta data (for time stamps)
            final ConsumerMetaEventsData meta = cd.events[buysEventIndex].getMeta();
            final long[] times = meta.getLongValuesArray(TIMESTAMP_ID);
            final int ilen = cbd.indices.length;
            
            // create a set of user items (to determine if the product is new) 
            for (int i=0; i<ilen; i++)
            {
                final int product_index = cbd.indices[i];
                //final TIntList contextPositions = contextHandler.getContext(data, product_index, meta, i);
                
                if (consumer_items.contains(product_index))
                    continue;
                
                newCounts.set(product_index, newCounts.get(product_index)+1);
                consumer_items.add(product_index);
                
                // add all pairs of new product
                for (int j=i+1; j<ilen; j++)
                {
                    final int product_index2 = cbd.indices[j];
                    if (product_index == product_index2)
                        continue;
                    addPair(product_index, times[i], null, product_index2, times[j], null, newSimilarities, newContextSimilarities);
                }
                
                // now compare this product with the old consumer items
                for (int j=0; j<ilen_old; j++)
                {
                    final int product_index2 = old_cbd.indices[j];
                    if (product_index == product_index2)
                        continue;
                    addPair(product_index, times[i], null, product_index2, times[j], null, newSimilarities, newContextSimilarities);
                }
            }
        }
        logger.info("Update item-based collaborative buys kNN ended.");
        return new UpdateDelta(newCounts, newSimilarities, newContextSimilarities);
    }
    
    
    
    @Override
    public Commitable updateConsumer(final DataStore.UpdateData updateData) {
        return EmptyCommit.INSTANCE;
    }
    
    @Override
    public void updateModel(DataStore data) {
        logger.warn("Update model does nothing for the Item-based collaborative predictor cosine.");
    }

    @Override
    public void updateModelQuick(DataStore data) {
        logger.error("Quick update not implemented for ItemBasedCollBuysPredictorCosine.");
    }
    
    @Override
    public ProductPair getBestPair(int consumerIndex) {
        logger.error("Get best pair not implemented for ItemBasedCollBuysPredictorCosine.");
        return null;
    }

    
	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		out.writeObject(similarities);
		out.writeObject(contextSimilarities);
		out.writeObject(productCounts);
	}


	@SuppressWarnings("unchecked")
	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        if (this.data == null) // initialization is needed
        {
            this.data = data;
        }		
        // similarities and indices
        List<TIntIntHashMap> newSimilarities = (List<TIntIntHashMap>) in.readObject();
        List<TIntIntHashMap> newContextSimilarities = (List<TIntIntHashMap>) in.readObject();
        TIntList productCounts = (TIntList) in.readObject(); 
		return new UpdateAll(productCounts, newSimilarities, newContextSimilarities);
	}       
        

    @Override
    public void getSimilarProducts(TIntSet productIndices, List<ProductRating> predictions, int id_shift, Map<String,String> tags) {
    }


    @Override
    public boolean needsProfiling(int consumerIndex) {
        logger.error("Needs profiling not implemented for ItemBasedCollBuysPredictorCosine.");
        return false;
    }




    @Override
    public Commitable updateProducts(DataStore.UpdateProductsDelta delta) {
        // new similarities
        List<TIntIntHashMap> newSimilarities = new ArrayList<TIntIntHashMap> ();
        List<TIntIntHashMap> newContextSimilarities = new ArrayList<TIntIntHashMap> ();
        TIntList newProductCounts = new TIntArrayList();         
        
        final TLongIntHashMap newProductIDs = delta.newProductIDs;
        
        final int counts_len = productCounts.size();
        for (int i = 0; i < counts_len; i++)
        {
            final ProductData pd = data.getProductByIndex(i);
            final int newIndex = newProductIDs.get(pd.productId);
            if (newIndex < 0)
                continue;
            
            while (newProductCounts.size() <= newIndex)
            {
                newProductCounts.add(0);
                newSimilarities.add(new TIntIntHashMap());
                newContextSimilarities.add(new TIntIntHashMap());
            }
            
            newProductCounts.set(newIndex, productCounts.get(i));
            
            TIntIntMap sim1 = newSimilarities.get(newIndex);
            TIntIntMap sim2 = newContextSimilarities.get(newIndex);
            
            for (TIntIntIterator it = similarities.get(i).iterator(); it.hasNext();)
            {
                it.advance();
                final int key = it.key();
                final int value = it.value();
                final int newIndex2 = newProductIDs.get(data.getProductByIndex(key).productId);
                sim1.put(newIndex2, value);
            }
            for (TIntIntIterator it = newContextSimilarities.get(i).iterator(); it.hasNext();)
            {
                it.advance();
                final int key = it.key();
                final int value = it.value();
                final int newIndex2 = newProductIDs.get(data.getProductByIndex(key).productId);
                sim2.put(newIndex2, value);
            }
        }
        return new UpdateProductsDelta(newProductCounts, newSimilarities, newContextSimilarities);
    }
    
    
    private class UpdateDelta implements Commitable {
        final Map<Integer, TIntIntHashMap> newSimilarities;
        final Map<Integer, TIntIntHashMap> newContextSimilarities;
        final TIntList newProductCounts;         
        
        UpdateDelta(final TIntList newProductCounts, final Map<Integer, TIntIntHashMap> newSimilarities, final Map<Integer, TIntIntHashMap> newContextSimilarities) {
            this.newSimilarities = newSimilarities;
            this.newContextSimilarities = newContextSimilarities;
            this.newProductCounts = newProductCounts;
        }

        @Override
        public void commit() {
            productCounts = newProductCounts;
            for (Map.Entry<Integer, TIntIntHashMap> entry : newSimilarities.entrySet())
            {
                final int key = entry.getKey();
                while (similarities.size() <= key)
                    similarities.add(new TIntIntHashMap());
                similarities.set(key, entry.getValue());
            }
            for (Map.Entry<Integer, TIntIntHashMap> entry : newContextSimilarities.entrySet())
            {
                final int key = entry.getKey();
                while (contextSimilarities.size() <= key)
                    contextSimilarities.add(new TIntIntHashMap());
                contextSimilarities.set(key, entry.getValue());
            }
        }
    }    
    
    private class UpdateAll implements Commitable {
        final List<TIntIntHashMap> newSimilarities;
        final List<TIntIntHashMap> newContextSimilarities;
        final TIntList newProductCounts;
        
        UpdateAll(final TIntList newProductCounts, final List<TIntIntHashMap> newSimilarities, final List<TIntIntHashMap> newContextSimilarities) {
            this.newSimilarities = newSimilarities;
            this.newContextSimilarities = newContextSimilarities;
            this.newProductCounts = newProductCounts;
        }

        @Override
        public void commit() {
            productCounts = newProductCounts;
            similarities = newSimilarities;
            contextSimilarities = newContextSimilarities;
        }
    }    
    
    
    private class UpdateProductsDelta implements Commitable {
        final List<TIntIntHashMap> newSimilarities;
        final List<TIntIntHashMap> newContextSimilarities;
        final TIntList newProductCounts;
        
        
        UpdateProductsDelta(final TIntList newProductCounts, final List<TIntIntHashMap> newSimilarities, final List<TIntIntHashMap> newContextSimilarities) {
            this.newSimilarities = newSimilarities;
            this.newContextSimilarities = newContextSimilarities;
            this.newProductCounts = newProductCounts;
        }

        @Override
        public void commit() {
            productCounts = newProductCounts;
            similarities = newSimilarities;
            contextSimilarities = newContextSimilarities;
        }
    }    
}
