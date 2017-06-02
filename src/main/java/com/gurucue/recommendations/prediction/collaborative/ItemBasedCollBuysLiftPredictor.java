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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.EmptyCommit;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ProductData;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntLongIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.ConsumerBuysData;
import com.gurucue.recommendations.recommender.dto.ConsumerMetaEventsData;
import com.gurucue.recommendations.recommender.dto.ContextDiscretizer;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;

/**
 * Item-based collaborative kNN for buys data. 
 */
public class ItemBasedCollBuysLiftPredictor extends Predictor {
    private final Logger logger;

    // name of events
    protected final String BUYSNAME;
    
    // similarity threshold for item to be used as similar
    private final float THRESHOLD; // 
    private final int RELEVANT_TIME; // number of days between two products that makes them still revelant for similarity
    private final float M; // blending factor in computing cosine similarity
    private int MAX_PRODUCTS; // maximal number of similar items for another item 
    
    // ID of average predictor in product rating
    final int AVERAGE_ID;
    
    // id of timestamp meta
    final int TIMESTAMP_ID;
    
    private DataStore data; // the data source
    
    // similarities and indices
    List<TIntIntMap> similarities;
    List<TIntIntMap> contextSimilarities;
    TIntLongMap productsTimes;
    TIntList productCounts;
    
    // context handler
    ContextDiscretizer contextHandler;

    // max difference in seconds for two products to be relevant
    long maxDiffSec;
    
    public ItemBasedCollBuysLiftPredictor(String name, Settings settings) throws CloneNotSupportedException {
        super(name, settings);
        logger = LogManager.getLogger(ItemBasedCollBuysLiftPredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        
        THRESHOLD = settings.getSettingAsFloat(name + "_THRESHOLD");
        BUYSNAME = settings.getSetting(name + "_BUYS");
        AVERAGE_ID = settings.getSettingAsInt(name + "_AVERAGE_ID");
        TIMESTAMP_ID = settings.getSettingAsInt(name+"_TIMESTAMP_META");
        RELEVANT_TIME = settings.getSettingAsInt(name+"_RELEVANT_TIME");
        MAX_PRODUCTS = settings.getSettingAsInt(name+"_MAX_PRODUCTS"); 
        
        M = settings.getSettingAsFloat(name+"_M");
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
                final ConsumerData cons = data.getConsumerByIndex(consumerIndex);
                if (cons != null)
                    cbd = (ConsumerBuysData) (cons.events[buysEventIndex]);
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
            final int prod_count = productCounts.get(prod1_index);
            if (prod_count == 0)
            {
                addProductRating(pr, pr.getPrediction(AVERAGE_ID), "");
                continue;
            }
            
            final TIntIntMap similarity = similarities.get(prod1_index);
            if (similarity == null)
            {
                addProductRating(pr, pr.getPrediction(AVERAGE_ID), "");
                continue;
            }
            
            // weighted sum of all products that this user bought  
            String explanation = "";
            double prediction = 0.0;
            
            // highest sim
            double highest_sim = 0;
            for (TIntIterator it = us.iterator(); it.hasNext(); )
            {
                final int prod2_index = it.next();
                if (similarity.get(prod2_index) < 5)
                    continue;
                final double sim = similarity.get(prod2_index) / (productCounts.get(prod2_index) * Math.sqrt(productCounts.get(prod1_index)));
                if (sim > highest_sim)
                {
                    explanation = "customers who watched also watched:(ID=" + data.getProducts().get(prod2_index).productId + ", title=" + data.getProducts().get(prod2_index).getStrAttrValue(0, 0) + ");";
                    //explanation = "based on id=" + data.getProducts().get(prod2_index).productId + "(" + sim + "," + similarity.get(prod2_index) + "," + productCounts.get(prod2_index) + ", " + Math.sqrt(productCounts.get(prod1_index)) +  ")" + ";";
                    highest_sim = sim; 
                }
                prediction += sim;
            }
            addProductRating(pr, prediction, explanation);
        }
        
    }
    
    private void addPair(final int product_index, final long time1, final TIntList context1, final int product_index2, final long time2, final TIntList context2, Map<Integer, TIntIntMap> newSimilarities, Map<Integer, TIntIntMap> contextSimilarities, long currentTime)
    {
        // is time difference small enough?
        if (Math.abs(time1-time2) > maxDiffSec)
            return;
        
        // similarities first
        TIntIntMap sim1, sim2, contSim1, contSim2;
        sim1 = newSimilarities.get(product_index);
        if (sim1 == null)
            sim1 = new TIntIntHashMap();
        sim2 = newSimilarities.get(product_index2);
        if (sim2 == null)
            sim2 = new TIntIntHashMap();
        
        sim1.put(product_index2, sim1.get(product_index2)+1);
        sim2.put(product_index, sim2.get(product_index)+1);
        
        newSimilarities.put(product_index, sim1);
        newSimilarities.put(product_index2, sim2);
    }
    
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData newData) {

        logger.info("Update ItemBasedCollBuysLiftPredictor kNN started.");
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = newData.dataStore; // dataStore object; this should not change since DataStore is a permanent object
            similarities = new ArrayList<TIntIntMap>();
            contextSimilarities = new ArrayList<TIntIntMap>();
            productsTimes = new TIntLongHashMap(); 
            productCounts = new TIntArrayList(this.data.getProducts().size());
            for (ProductData pr : this.data.getProducts())
            {
                productCounts.add(0);
                similarities.add(null); //new TIntIntHashMap());
                contextSimilarities.add(null); //new TIntIntHashMap());
            }
        }
        final int buysEventIndex = data.getEventsDescriptor(BUYSNAME).index;
        final long currentTime = System.currentTimeMillis() / 1000;
        
        TIntArrayList newCounts = new TIntArrayList(productCounts);
        Map<Integer, TIntIntMap> newSimilarities = new HashMap<Integer, TIntIntMap>();
        Map<Integer, TIntIntMap> newContextSimilarities = new HashMap<Integer, TIntIntMap>();
        
        // we first need to update the list of active products
        for (ConsumerData cd : newData.newData)
        {
            if (null == cd)
                continue;
            final ConsumerBuysData cbd = (ConsumerBuysData) cd.events[buysEventIndex];
            if ((null == cbd) || (cbd.indices.length == 0))
                continue;
            final ConsumerMetaEventsData meta = cd.events[buysEventIndex].getMeta();
            final long[] times = meta.getLongValuesArray(TIMESTAMP_ID);

            final int ilen = cbd.indices.length;
            
            // create a set of user items (to determine if the product is new) 
            for (int i=0; i<ilen; i++)
            {
                final int product_index = cbd.indices[i];
                productsTimes.put(product_index, Math.max(times[i],productsTimes.get(product_index)));
            }
        }
        
        // now, select a set of products that are current 
        TIntSet currentProducts = new TIntHashSet();
        long [] vals = productsTimes.values();
        Arrays.sort(vals);
        final long threshold = vals[Math.max(0, productsTimes.size()-MAX_PRODUCTS)];
        for (TIntLongIterator it = productsTimes.iterator(); it.hasNext(); )
        {
            it.advance();
            if (it.value() >= threshold)
                currentProducts.add(it.key());
        }
        
        // first remove everthing from newCounts that is not in currentCounts
        for (int i = newCounts.size()-1; i >= 0; i--)
        {
            if (!currentProducts.contains(i))
                newCounts.set(i, 0);
        }

        // now add data about the pairs containing only current products
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
                old_times = old_meta.getLongValuesArray(TIMESTAMP_ID);
            }
            else
            {
                old_meta = null;
                old_times = null;
                ilen_old = 0;
                consumer_items = new TIntHashSet();
                old_times = null;
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
                
                if (!currentProducts.contains(product_index))
                    continue;

                newCounts.set(product_index, newCounts.get(product_index)+1);
                consumer_items.add(product_index);
                
                // add all pairs of new product
                for (int j=i+1; j<ilen; j++)
                {
                    final int product_index2 = cbd.indices[j];
                    if (product_index == product_index2)
                        continue;
                    if (!currentProducts.contains(product_index2))
                        continue;
                    addPair(product_index, times[i], null, product_index2, times[j], null, newSimilarities, newContextSimilarities, currentTime);
                }
                
                // now compare this product with the old consumer items
                for (int j=0; j<ilen_old; j++)
                {
                    final int product_index2 = old_cbd.indices[j];
                    if (product_index == product_index2)
                        continue;
                    if (!currentProducts.contains(product_index2))
                        continue;
                    addPair(product_index, times[i], null, product_index2, old_times[j], null, newSimilarities, newContextSimilarities, currentTime);
                }
            }
        }
        // go through similarities
        int key = 0;
        for (TIntIntMap sim : similarities)
        {
            if (sim == null || sim.size () == 0)
                continue;
            // if newSimilarities contains this, sum it up, or if it contains a key that is not in currentProducts
            if (newSimilarities.containsKey(key) || !currentProducts.containsAll(sim.keys()))
            {
                TIntIntMap newSim = newSimilarities.get(key);
                if (newSim == null)
                {
                    newSim = new TIntIntHashMap();
                    newSimilarities.put(key, newSim);
                }
                if (currentProducts.contains(key))
                    for (TIntIntIterator it = sim.iterator(); it.hasNext(); )
                    {
                        it.advance();
                        final int key2 = it.key();
                        if (currentProducts.contains(key2))
                            newSim.put(key, newSim.get(it.key()) + it.value());
                    }
            }
            key += 1;
        }

        logger.info("Update ItemBasedCollBuysLiftPredictor kNN ended.");
        return new UpdateDelta(newCounts, newSimilarities, newContextSimilarities);
    }
    
    
    
    @Override
    public Commitable updateConsumer(final DataStore.UpdateData updateData) {
        return EmptyCommit.INSTANCE;
    }
    
    @Override
    public void updateModel(DataStore data) {
        logger.warn("Update model does nothing for the ItemBasedCollBuysLiftPredictor predictor.");
    }

    @Override
    public void updateModelQuick(DataStore data) {
        logger.error("Quick update not implemented for ItemBasedCollBuysLiftPredictor.");
    }
    
    @Override
    public ProductPair getBestPair(int consumerIndex) {
        logger.error("Get best pair not implemented for ItemBasedCollBuysLiftPredictor.");
        return null;
    }


    @Override
    public void getSimilarProducts(TIntSet productIndices, List<ProductRating> predictions, int id_shift, Map<String,String> tags) {
        for (ProductRating pr : predictions)
        {
            final int prod1_index = pr.getProductIndex();
            final int prod_count = productCounts.get(prod1_index);
            if (prod_count == 0)
            {
                addProductRating(pr, 0.0, "");
                continue;
            }
            
            final TIntIntMap similarity = similarities.get(prod1_index);
            if (similarity == null)
            {
                addProductRating(pr, 0.0, "");
                continue;
            }
            
            // weighted sum of all products that this user bought  
            String explanation = "";
            double prediction = 0.0;
            
            // store highest similarity
            double highest_sim = 0;
            int highest_index = 0;
            for (TIntIterator it = productIndices.iterator(); it.hasNext(); )
            {
                final int prod2_index = it.next();
                final double sim = similarity.get(prod2_index) / (productCounts.get(prod2_index) * Math.sqrt(productCounts.get(prod1_index)));
                if (sim > highest_sim)
                {
                    highest_sim = sim;
                    highest_index = prod2_index;
                }
                prediction += sim;
            }
            if (highest_sim > 0)
            {
                explanation = "based on id=" + data.getProducts().get(highest_index).productId + "(" + highest_sim + "," + similarity.get(highest_index) + "," + productCounts.get(highest_index) + ", " + Math.sqrt(productCounts.get(prod1_index)) +  ")" + ";";
            }                    
            addProductRating(pr, prediction, explanation);
        }
    }

    @Override
    public boolean needsProfiling(int consumerIndex) {
        logger.error("Needs profiling not implemented for ItemBasedCollBuysLiftPredictor.");
        return false;
    }

    @Override
    public Commitable updateProducts(DataStore.UpdateProductsDelta delta) {
        // new similarities
        List<TIntIntMap> newSimilarities = new ArrayList<TIntIntMap> ();
        List<TIntIntMap> newContextSimilarities = new ArrayList<TIntIntMap> ();
        TIntList newProductCounts = new TIntArrayList();         
        
        final List<ProductData> newProducts = delta.newProducts;
        final TLongIntHashMap newProductIDs = delta.newProductIDs;
        TIntLongMap newProductsTimes = new TIntLongHashMap();
        
        for (ProductData pd : newProducts)
        {
            newProductCounts.add(0);
            newSimilarities.add(null);
            newContextSimilarities.add(null);
        }
        
        final int counts_len = productCounts.size();
        for (int i = 0; i < counts_len; i++)
        {
            final ProductData pd = data.getProductByIndex(i);
            final int newIndex = newProductIDs.get(pd.productId);
            if (newIndex < 0)
                continue;
            
            newProductCounts.set(newIndex, productCounts.get(i));
            newProductsTimes.put(newIndex, productsTimes.get(i));
            
            if (similarities.get(i) == null)
                continue;
            
            newSimilarities.set(newIndex, new TIntIntHashMap());
            TIntIntMap sim1 = newSimilarities.get(newIndex);
            
            for (TIntIntIterator it = sim1.iterator(); it.hasNext();)
            {
                it.advance();
                final int key = it.key();
                final int value = it.value();
                final int newIndex2 = newProductIDs.get(data.getProductByIndex(key).productId);
                sim1.put(newIndex2, value);
            }
            
            if (contextSimilarities.get(i) == null)
                continue;
            newContextSimilarities.set(newIndex, new TIntIntHashMap());
            TIntIntMap sim2 = newContextSimilarities.get(newIndex);
            for (TIntIntIterator it = newContextSimilarities.get(i).iterator(); it.hasNext();)
            {
                it.advance();
                final int key = it.key();
                final int value = it.value();
                final int newIndex2 = newProductIDs.get(data.getProductByIndex(key).productId);
                sim2.put(newIndex2, value);
            }
        }
        
        productsTimes = newProductsTimes;
        return new UpdateProductsDelta(newProductCounts, newSimilarities, newContextSimilarities);
    }
    
	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		out.writeObject(similarities);
		out.writeObject(contextSimilarities);
		out.writeObject(productsTimes);
		out.writeObject(productCounts);
	}


	@SuppressWarnings("unchecked")
	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = data; // dataStore object; this should not change since DataStore is a permanent object
        }		
	    List<TIntIntMap> newSimilarities = (List<TIntIntMap>) in.readObject();
	    List<TIntIntMap> newContextSimilarities = (List<TIntIntMap>) in.readObject();		
		TIntLongMap productsTimes = (TIntLongMap) in.readObject();
		TIntList newProductCounts = (TIntList) in.readObject();
		return new UpdateAll(newProductCounts, newSimilarities, newContextSimilarities, productsTimes);
	}       
        
    
    private class UpdateDelta implements Commitable {
        final Map<Integer, TIntIntMap> newSimilarities;
        final Map<Integer, TIntIntMap> newContextSimilarities;
        final TIntList newProductCounts;         
        
        UpdateDelta(final TIntList newProductCounts, final Map<Integer, TIntIntMap> newSimilarities, final Map<Integer, TIntIntMap> newContextSimilarities) {
            this.newSimilarities = newSimilarities;
            this.newContextSimilarities = newContextSimilarities;
            this.newProductCounts = newProductCounts;
        }

        @Override
        public void commit() {
            productCounts = newProductCounts;
            for (Map.Entry<Integer, TIntIntMap> entry : newSimilarities.entrySet())
            {
                final int key = entry.getKey();
                while (similarities.size() <= key)
                    similarities.add(null);
                
                TIntIntMap newValues = entry.getValue();
                if (newValues == null)
                    continue;
                
                similarities.set(key, newValues);    
            }
            
            for (Map.Entry<Integer, TIntIntMap> entry : newContextSimilarities.entrySet())
            {
                final int key = entry.getKey();
                while (contextSimilarities.size() <= key)
                    contextSimilarities.add(null);
                
                TIntIntMap newValues = entry.getValue();
                if (newValues == null)
                    continue;
                
                similarities.set(key, newValues);    
            }
        }
    }    
    
    private class UpdateAll implements Commitable {
        final List<TIntIntMap> newSimilarities;
        final List<TIntIntMap> newContextSimilarities;
        final TIntList newProductCounts;  
        final TIntLongMap newProductsTimes;
        
        UpdateAll(final TIntList newProductCounts, final List<TIntIntMap> newSimilarities, final List<TIntIntMap> newContextSimilarities, final TIntLongMap newProductsTimes) {
            this.newSimilarities = newSimilarities;
            this.newContextSimilarities = newContextSimilarities;
            this.newProductCounts = newProductCounts;
            this.newProductsTimes = newProductsTimes;
        }

        @Override
        public void commit() {
            productCounts = newProductCounts;
            similarities = newSimilarities;
            contextSimilarities = newContextSimilarities;
            productsTimes = newProductsTimes;
        }
    }      
    
    private class UpdateProductsDelta implements Commitable {
        final List<TIntIntMap> newSimilarities;
        final List<TIntIntMap> newContextSimilarities;
        final TIntList newProductCounts;
        
        
        UpdateProductsDelta(final TIntList newProductCounts, final List<TIntIntMap> newSimilarities, final List<TIntIntMap> newContextSimilarities) {
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
