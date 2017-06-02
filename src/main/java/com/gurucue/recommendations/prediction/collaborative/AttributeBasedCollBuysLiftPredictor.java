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
import java.util.List;
import java.util.Map;

import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.EmptyCommit;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.Attr;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ProductData;
import com.gurucue.recommendations.recommender.dto.TagsManager;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntLongIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.TIntList;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TIntHashSet;
import gnu.trove.set.hash.TLongHashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.ConsumerBuysData;
import com.gurucue.recommendations.recommender.dto.ConsumerMetaEventsData;
import com.gurucue.recommendations.recommender.dto.ContextDiscretizer;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;

/**
 * Item-based collaborative kNN for attributes (products are similar if a specific attribute is similar). If several attributes are provided, average similarity is returned.  
 * 
 */
public class AttributeBasedCollBuysLiftPredictor extends Predictor {
	final static int MAX_SIMILAR_SIZE=20;
	final static int MAX_EXPLANATION_SIZE=3;
    
    private final Logger logger;

    // name of events
    protected final String BUYSNAME;
    
    // similarity threshold for item to be used as similar
    private final int RELEVANT_TIME, RECENT_TIME; // number of days between two products that makes them still revelant for similarity
    private final int [] ATTRIBUTE_IDS;
    private final int [] EXPLAINABLE_ATTRIBUTE_IDS;
    private final int [] ATTRIBUTE_CONSTRAINT; // attributes used to constrain explanations (at least one of those values need to be the same)
    private final int MAX_ATTR_VALUE;
    private final int LIMIT_VALUES;
    private final int TIME_LIMIT; // maximum allowed time of an item not being bought (in seconds)
    private final float M; // blending factor
    private final float STABILIZE; // blending factor
    private final int RECENT_PRODUCTS; // how many recent products are used?
    
    // ID of average predictor in product rating
    final int AVERAGE_ID;
    
    // id of timestamp meta
    final int TIMESTAMP_ID;
    
    private DataStore data; // the data source
    
    // similarities and indices
    TIntObjectMap<TIntLongMap> similarities;  
    TIntLongMap attributeCounts;
    TIntLongMap attributeTimes;
    
    // number of all considered attributes
    long allElements;
    
    // pool of TIntIntMaps to avoid constant garbage collection
    TIntObjectMap<TIntLongMap> newSimilarities;  
    TIntLongMap newCounts;
    
    // context handler
    ContextDiscretizer contextHandler;
    
    
    
    public AttributeBasedCollBuysLiftPredictor(String name, Settings settings) throws CloneNotSupportedException {
    	
        super(name, settings);
        logger = LogManager.getLogger(AttributeBasedCollBuysLiftPredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        
        BUYSNAME = settings.getSetting(name + "_BUYS");
        AVERAGE_ID = settings.getSettingAsInt(name + "_AVERAGE_ID");
        TIMESTAMP_ID = settings.getSettingAsInt(name+"_TIMESTAMP_META");
        RELEVANT_TIME = settings.getSettingAsInt(name+"_RELEVANT_TIME");
        Integer recent = settings.getSettingAsInt(name+"_RECENT_TIME");
        if (recent == null)
        	RECENT_TIME = 0;
        else
        	RECENT_TIME = recent;
        ATTRIBUTE_IDS = settings.getAsIntArray(name + "_ATTRIBUTE_IDS");
        EXPLAINABLE_ATTRIBUTE_IDS = settings.getAsIntArray(name + "_EXPLAINABLE_ATTRIBUTE_IDS");
        MAX_ATTR_VALUE = settings.getSettingAsInt(name + "_MAX_ATTR_VALUE");
        LIMIT_VALUES = settings.getSettingAsInt(name + "_LIMIT_VALUES");
        TIME_LIMIT = settings.getSettingAsInt(name + "_TIME_LIMIT");
        M = settings.getSettingAsFloat(name + "_M");
        STABILIZE = settings.getSettingAsFloat(name + "_STABILIZE");
        ATTRIBUTE_CONSTRAINT = settings.getAsIntArray(name + "_ATTRIBUTE_CONSTRAINT");
        RECENT_PRODUCTS = settings.getSettingAsInt(name+"_RECENT_PRODUCTS");
        
        // initialize context handler
        contextHandler = new ContextDiscretizer(name, settings);        
    }
    

    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) {
    	if (predictions.size() == 0)
    		return;    	
    	
        ConsumerBuysData cbd = null;
        TIntIntMap keySet = new TIntIntHashMap(); 
        int consumerIndex = -1;
        final int buysEventIndex = data.getEventsDescriptor(BUYSNAME).index;
        final long currentTimeSec = TagsManager.getCurrentTimeSeconds(tags);

        TDoubleList sims = new TDoubleArrayList();
        TDoubleList counts = new TDoubleArrayList();
        TIntList bestKeys = new TIntArrayList();
        
        for (ProductRating pr : predictions)
        {
            if (pr.getConsumerIndex() != consumerIndex)
            {
                keySet = new TIntIntHashMap();
                consumerIndex = pr.getConsumerIndex();
                final ConsumerData cons = data.getConsumerByIndex(consumerIndex);
                if (cons != null)
                    cbd = (ConsumerBuysData) (cons.events[buysEventIndex]);
                // get indices 
                if ((null != cbd) && (cbd.indices.length > 0))
                {
                    final long [] dates = cbd.getMeta().getLongValuesArray(TIMESTAMP_ID);
                    int last_index = Math.max(0, cbd.indices.length-1-RECENT_PRODUCTS);
                    for (int i = cbd.indices.length-1; i>=last_index; i--)
                        if (dates[i] > currentTimeSec - RELEVANT_TIME && dates[i] < currentTimeSec)
                        {
                            final TIntSet values = getAttributeValues(cbd.indices[i]);
                            for (TIntIterator it = values.iterator(); it.hasNext(); )
                            {
                                final int key = it.next();
                                if (dates[i] > currentTimeSec - RECENT_TIME)
                                	keySet.put(key, keySet.get(key)+2); // count as 2 if it is within recent time interval
                                else
                                	keySet.put(key, keySet.get(key)+1);
                            }
                        }
                }
            }

            final ProductData productData = pr.getProductData();
            final int prod1_index = pr.getProductIndex();
            TIntSet val1 = getAttributeValues(prod1_index);
            if (val1.size() == 0)
            {
                addProductRating(pr, 0, "");
                continue;
            }

            sims.clear();
            counts.clear();
            bestKeys.clear();
            // weighted sum of all products that this user bought
            for (TIntIntIterator it = keySet.iterator(); it.hasNext(); )
            {
                it.advance();
                final int count = it.value();
                final int key = it.key();
                final long keyCounts = attributeCounts.get(key);
                
                double sim = 0;
                final int counter = val1.size();
                for (TIntIterator itval1 = val1.iterator(); itval1.hasNext(); )
                {
                    final int v1 = itval1.next();
                    final TIntLongMap similarity = similarities.get(v1);
                    if (similarity == null)
                    	continue;
                    final long jointdist = similarity.get(key);
                    if (jointdist > 50)
                    {
                    	final double generalprob = ((double) attributeCounts.get(v1)) / allElements; // + STABILIZE;
                    	final double logsim = Math.log(((jointdist + M * generalprob) / (keyCounts + M) + STABILIZE) / (generalprob + STABILIZE));
                   		sim += logsim; 
                    }
                }
                if (counter > 0)
                {
                    sims.add(Math.sqrt(count)*sim/counter);
                    counts.add(Math.sqrt(count));
                    bestKeys.add(key);
                }
            }
            
            double prediction = 0;
            double weight = MAX_SIMILAR_SIZE;
            String explanation = "";
            // find fifth best item in sims
            TDoubleList tmpSims = new TDoubleArrayList(sims);
            tmpSims.sort();
            final int sims_size = sims.size();
            int npos = 0;
            int explained = 0;
            if (sims_size > 0)
            {
                double threshold = tmpSims.get(Math.max(0, sims_size - MAX_SIMILAR_SIZE));
                // add only MAX_SIMILAR_SIZE best items in sims
                final StringBuilder explanationBuilder = new StringBuilder();
                TLongSet mentionedKeys = new TLongHashSet();
                for (int i = 0; i < sims_size; i++)
                {
                    if (sims.get(i) >= threshold)
                    {
                        // add explanation
                        final long atVal = bestKeys.get(i);
                        if (mentionedKeys.contains(atVal))
                            continue;
                        mentionedKeys.add(atVal);
                        if (explained < MAX_EXPLANATION_SIZE && sims.get(i)/counts.get(i) > 0.7)
                        {
                        	// check if explanation can be made
                        	final int attr = (int) (atVal / MAX_ATTR_VALUE);
                        	final int val = (int) (atVal % MAX_ATTR_VALUE);

                        	final String str_val = productData.getStrAttrValue(ATTRIBUTE_IDS[attr], val);
                        	if (EXPLAINABLE_ATTRIBUTE_IDS[attr] == 1)
                        	{
                        		// get 
                        		StringBuilder pretty = new StringBuilder();
                        		String [] tmps = str_val.split(":");
                        		if (tmps != null && tmps.length == 2)
                        		{
                        			ProductData pd_temp = data.getProductById(Long.valueOf(tmps[1]));
                        			if (pd_temp != null && pd_temp.getAttribute(0) != null && pd_temp.getAttribute(0).hasValue())
                        			// found a product! now check its attributes
                        			{
                        				if (similar(pd_temp, productData))
                        					pretty.append(pd_temp.getAttribute(0).getStrValue(0));
                        			}
                        			else
                        				pretty.append(str_val);
                        			
                        		}
                        		else
                        			pretty.append(str_val);
                        		if (pretty.length() > 0)
                        		{
                            		pretty.insert(0, "Users who watched ");
                            		pretty.append(" also watched this item");
                            		pr.addPrettyExplanation(pretty.toString(), 70.0f);
                            		explained++;
                        		}
                        	}

                        	explanationBuilder.append("similar:");
                        	explanationBuilder.append(str_val);
                        	if (str_val == null || str_val.equalsIgnoreCase("null"))
                        	{
                        		logger.warn("Problem with attribute value, attr = " + attr + ", val = " + val + ", " + str_val + ", key = " + atVal);
                        	}
                        	
                        	explanationBuilder.append("=");
                        	explanationBuilder.append(sims.get(i));
                        	explanationBuilder.append(";");
                        }
                        prediction += sims.get(i);
                        weight += counts.get(i);
                    	npos += 1;
                    }
                }
                explanationBuilder.append(");");
                explanation = explanationBuilder.toString();
            }
            if (npos == 0 && prediction <= 0)
            	prediction = 0;
            else
            	prediction = Math.exp(prediction/weight);
            if (AVERAGE_ID < 0)
            	addProductRating(pr, prediction, explanation);
            else
            	addProductRating(pr, pr.getPrediction(AVERAGE_ID) * prediction, explanation);
        }
    }
    
    @Override
    public void getSimilarProducts(TIntSet productIndices, List<ProductRating> predictions, int id_shift, Map<String,String> tags) {
        for (ProductRating pr : predictions)
        {
            final int prod1_index = pr.getProductIndex();
            TIntSet val1 = getAttributeValues(prod1_index);
            if (val1.size() == 0)
            {
                addProductRating(pr, 0, "");
            }
            
            // weighted sum of all products 
            String explanation = "";
            double prediction = 1.0; 
            for (TIntIterator it = productIndices.iterator(); it.hasNext(); )
            {
                final int prod2_index = it.next();
                TIntSet val2 = getAttributeValues(prod2_index);
                // for each item in val1 find the most similar item in val2
                for (TIntIterator itval1 = val1.iterator(); itval1.hasNext(); )
                {
                    final int v1 = itval1.next();
                    final double generalprob = ((double) attributeCounts.get(v1)) / allElements; // + 0.001;
                    final TIntLongMap similarity = similarities.get(v1);
                    double maxSim = 1.0;
                    if (similarity != null)
                        for (TIntIterator itval2 = val2.iterator(); itval2.hasNext(); )
                        {
                            final int v2 = itval2.next();
                            final double sim_v1v2 = ((similarity.get(v2)+M * generalprob) / (attributeCounts.get(v2) + M) + STABILIZE) / 
                            					    (generalprob + STABILIZE);
                            maxSim *= sim_v1v2;
                        }
                    if (val2.size() > 0)
                    	prediction *= Math.pow(maxSim, 1.0/val2.size());
                    explanation += String.valueOf(maxSim) + ";" + String.valueOf(val2.size()) + ";";
                }
            }
            addProductRating(pr, prediction, "");
        }
    }
    
    private boolean similar(ProductData p1, ProductData p2)
    {
    	// returns true if at least one of the attributes is the same;
    	if (ATTRIBUTE_CONSTRAINT.length == 0)
    		return true;
    
    	int compare = 0;
    	for (int att : ATTRIBUTE_CONSTRAINT)
    	{
    		if (p1.getAttribute(att) == null || p2.getAttribute(att) == null)
    			continue;
    		TIntSet vals1 = p1.getAttribute(att).getValues();
    		TIntSet vals2 = p2.getAttribute(att).getValues();
    		if (vals1 == null || vals2 == null)
    		vals1.retainAll(vals2);
    		compare ++;
    		if (vals1.size() > 0)
    			return true;
    	}
    	// if all attributes were null, let it be
    	if (compare == 0)
    		return true;
    	
    	return false;
    }
    
    private void addPair(final int val_index1, final int val_index2, TIntObjectMap<TIntLongMap> newSimilarities)
    {
        // similarities first
    	TIntLongMap sim1, sim2;
        sim1 = newSimilarities.get(val_index1);
        if (sim1 == null)
        {
        	sim1 = new TIntLongHashMap();
            if (similarities.containsKey(val_index1))
            	sim1.putAll(similarities.get(val_index1));
        }
        sim2 = newSimilarities.get(val_index2);
        if (sim2 == null)
        {
        	sim2 = new TIntLongHashMap();
            if (similarities.containsKey(val_index2))
            	sim2.putAll(similarities.get(val_index2));
        }
        
        sim1.put(val_index2, sim1.get(val_index2)+1);
        sim2.put(val_index1, sim2.get(val_index1)+1);
        
        newSimilarities.put(val_index1, sim1);
        newSimilarities.put(val_index2, sim2);
    }
    
    TIntSet getAttributeValues(int index)
    {
        TIntSet attrValues = new TIntHashSet();
        for (int ati = 0; ati < ATTRIBUTE_IDS.length; ati ++)
        {
            Attr attribute = this.data.getAttr(index, ATTRIBUTE_IDS[ati]);
            if (attribute != null && attribute.hasValue())
            {
                final TIntSet vals = attribute.getValues();
                for (TIntIterator it = vals.iterator(); it.hasNext(); )
                {
                	final int val = it.next();
                    final int key = ati * MAX_ATTR_VALUE + (val % MAX_ATTR_VALUE);
                    if (val > MAX_ATTR_VALUE)
                    	logger.warn("Attribute index " + val + " is larger than MAX_ATTR_VALUE.");
                    attrValues.add(key);
                }
                if (attrValues.size() > 0) // only the most relevant attribute is used
                	break;
            }
        }
        return attrValues;
    }
    
    TIntSet getAttributeValues(int [] indices)
    {
        TIntSet attrValues = new TIntHashSet();
        for (int item : indices)
        {
            attrValues.addAll(getAttributeValues(item));
        }
        return attrValues;
    }
    
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData newData) {

        logger.info("Update attribute-based collaborative buys kNN started.");
        if (this.data == null) // initialization is needed
        {
            this.data = newData.dataStore;
            similarities = new TIntObjectHashMap<TIntLongMap>();
            attributeCounts = new TIntLongHashMap();
            attributeTimes = new TIntLongHashMap();
            allElements = 0;
        }
        final int buysEventIndex = data.getEventsDescriptor(BUYSNAME).index;
        final long lastDateAllowed = data.getEventsDescriptor(BUYSNAME).last_read_date - TIME_LIMIT;
        
        int new_events = 0;
        // get a number of new events
        for (ConsumerData cd : newData.newData)
        {
            if (null == cd)
                continue;
            
            // get events
            final ConsumerBuysData cbd = (ConsumerBuysData) cd.events[buysEventIndex];
            if ((null == cbd) || (cbd.indices.length == 0))
                continue;

            new_events += cbd.indices.length;
            break;
        }
        if (new_events == 0) // changes are not necessary
        	return EmptyCommit.INSTANCE;
        
        newSimilarities = new TIntObjectHashMap<TIntLongMap>();
        newCounts = new TIntLongHashMap();        	
        newCounts.putAll(attributeCounts);
        for (TIntObjectIterator<TIntLongMap> it = similarities.iterator(); it.hasNext(); )
        {
            it.advance();
            final int key = it.key();
            newSimilarities.put(key, new TIntLongHashMap());
            newSimilarities.get(key).putAll(it.value());
        }
        long newAll = allElements;
        
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
            
            if (data.getConsumers().size() > user_index)
                old_cbd = (ConsumerBuysData) data.getConsumers().get(user_index).events[buysEventIndex];
            if (old_cbd != null)
            {
                old_meta = data.getConsumers().get(user_index).events[buysEventIndex].getMeta();
                old_times  = old_meta.getLongValuesArray(TIMESTAMP_ID);
                ilen_old = old_cbd.indices.length;
            }
            else
            {
                old_meta = null;
                old_times = null;
                ilen_old = 0;
            }
            
            // get meta data (for time stamps)
            final ConsumerMetaEventsData meta = cd.events[buysEventIndex].getMeta();
            final long[] times = meta.getLongValuesArray(TIMESTAMP_ID);
            final int ilen = cbd.indices.length;
            
            // create a set of user items (to determine if the product is new) 
            for (int i=0; i<ilen; i++)
            {
                final int product_index = cbd.indices[i];
                final TIntSet vali = getAttributeValues(product_index);

                // update times
                for (TIntIterator it = vali.iterator(); it.hasNext(); )
                {
                	final int ati = it.next();
                	attributeTimes.put(ati,  Math.max(times[i],attributeTimes.get(ati)));
                }
                
                
                // loop over old products
                for (int j = 0; j < ilen_old; j++)
                {
                    // if times are too far apart, should not count
                    if (Math.abs(times[i] - old_times[j]) > RELEVANT_TIME)
                        continue;
                    
                    final TIntSet valj = getAttributeValues(old_cbd.indices[j]);
                    for (TIntIterator jt = valj.iterator(); jt.hasNext(); )
                    {
                        final int atj = jt.next();
                        if (!attributeTimes.containsKey(atj))
                        	continue;
                        for (TIntIterator it = vali.iterator(); it.hasNext(); )
                        {
                            final int ati = it.next();
                            addPair(ati, atj, newSimilarities);
                            newCounts.put(ati, newCounts.get(ati)+1);
                            newCounts.put(atj, newCounts.get(atj)+1);
                            newAll += 1;
                        }
                    }
                }
                
                // loop over new products ; skip already checked comparisons
                for (int j = i+1; j < ilen; j++)
                {
                    // if times are too far apart, should not count
                    if (Math.abs(times[i] - times[j]) > RELEVANT_TIME)
                        continue;
                    
                    final TIntSet valj = getAttributeValues(cbd.indices[j]);
                    for (TIntIterator it = vali.iterator(); it.hasNext(); )
                    {
                        final int ati = it.next();
                        for (TIntIterator jt = valj.iterator(); jt.hasNext(); )
                        {
                            final int atj = jt.next();
                            addPair(ati, atj, newSimilarities);
                            newCounts.put(ati, newCounts.get(ati)+1);
                            newCounts.put(atj, newCounts.get(atj)+1);
                            newAll += 1;
                        }
                    }
                }
            }
        }
        logger.info("Basic processing finished");

        // remove items that are too old
        for (TIntLongIterator it = attributeTimes.iterator(); it.hasNext();)
    	{
    		it.advance();
    		if (it.value() < lastDateAllowed)
    			it.remove();
    	}        
        
        // define a set of attributes to be removed from the similarity map
        if (attributeTimes.size() > LIMIT_VALUES)
        {
        	// create a list of time
        	TLongList timesSort = new TLongArrayList();
        	for (TIntLongIterator it = attributeTimes.iterator(); it.hasNext();)
        	{
        		it.advance();
        		timesSort.add(it.value());
        	}
        	timesSort.sort();
        	long threshold = timesSort.get(timesSort.size()-LIMIT_VALUES);
        	for (TIntLongIterator it = attributeTimes.iterator(); it.hasNext();)
        	{
        		it.advance();
        		if (it.value() < threshold)
        			it.remove();
        	}
        }

        // remove old items' similarities
        for (TIntObjectIterator<TIntLongMap> it = newSimilarities.iterator(); it.hasNext(); )
        {
            it.advance();
            final int key = it.key();
            final TIntLongMap value = it.value();
            if (!(attributeTimes.containsKey(key)))
            {
            	// decrease attribute counts
                for (TIntLongIterator it2 = value.iterator(); it2.hasNext(); )
                {
                	it2.advance();
                	final int key2 = it2.key();
                	final long counts = it2.value();
                	if (attributeTimes.containsKey(key2))
                	{
                        newCounts.put(key2, newCounts.get(key2)-counts);
                        if (newCounts.get(key2) < 0)
                        {
                        	logger.error("Negative counts !!!! : " + newCounts.get(key2) + ", " + counts);
                        	attributeTimes.remove(key2);
                        	newCounts.put(key2, 0);
                        }
                	}
                }
            	newAll -= newCounts.get(key);
                newCounts.put(key, 0);
            	it.remove();
            }
        }

        // remove items that are not in attributeTimes
        // counts do not need to be updated (that was already processed)
        for (TIntObjectIterator<TIntLongMap> it = newSimilarities.iterator(); it.hasNext(); )
        {
            it.advance();
            TIntLongMap value = it.value();
            for (TIntLongIterator it2 = value.iterator(); it2.hasNext(); )
            {
            	it2.advance();
            	final int key2 = it2.key();
                if (!(attributeTimes.containsKey(key2)))
                {
                	it2.remove();
                }
            }
        }        
        logger.info("Update attribute-based collaborative buys kNN ended.");
        return new UpdateDelta(newCounts, newSimilarities, newAll);
    }
    
    
    
    @Override
    public Commitable updateConsumer(final DataStore.UpdateData updateData) {
        return EmptyCommit.INSTANCE;
    }
    
    @Override
    public void updateModel(DataStore data) {
        logger.warn("Update model does nothing for the Attribute-based collaborative predictor.");
    }

    @Override
    public void updateModelQuick(DataStore data) {
        logger.error("Quick update not implemented for AttributeBasedCollBuysLiftPredictor.");
    }
    
    @Override
    public ProductPair getBestPair(int consumerIndex) {
        logger.error("Get best pair not implemented for AttributeBasedCollBuysLiftPredictor.");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumerIndex) {
        logger.error("Needs profiling not implemented for AttributeBasedCollBuysLiftPredictor.");
        return false;
    }

    @Override
    public Commitable updateProducts(DataStore.UpdateProductsDelta delta) {
        return EmptyCommit.INSTANCE;
    }
    
	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		out.writeObject(similarities);
		out.writeObject(attributeCounts);
		out.writeObject(attributeTimes);
		out.writeLong(allElements);
	}


	@SuppressWarnings("unchecked")
	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        if (this.data == null) // initialization is needed
        {
            this.data = data;
            similarities = new TIntObjectHashMap<TIntLongMap>();
            attributeCounts = new TIntLongHashMap();
            attributeTimes = new TIntLongHashMap();
            allElements = 0;
        }		
	    // similarities and indices
		TIntObjectMap<TIntLongMap> newSimilarities = (TIntObjectMap<TIntLongMap>) in.readObject();
		TIntLongMap newAttributeCounts = (TIntLongMap) in.readObject();
		attributeTimes = (TIntLongMap) in.readObject();
		long newAllElements = in.readLong();
		return new UpdateDelta(newAttributeCounts, newSimilarities, newAllElements);
	}       
    
    
    private class UpdateDelta implements Commitable {
        final TIntObjectMap<TIntLongMap> newSimilaritiesUpdate;
        final TIntLongMap newAttributeCounts;         
        final long newAllElements;
        
        UpdateDelta(final TIntLongMap newAttributeCounts, final TIntObjectMap<TIntLongMap> newSimilarities, final long newAllElements) {
            this.newSimilaritiesUpdate = newSimilarities;
            this.newAttributeCounts = newAttributeCounts;
            this.newAllElements = newAllElements;
        }

        @Override
        public void commit() {
            allElements = newAllElements;
            final TIntLongMap tmp1 = attributeCounts;
            attributeCounts = newAttributeCounts;
            newCounts = tmp1;
            
            final TIntObjectMap<TIntLongMap> tmp2 = similarities;
            similarities = newSimilaritiesUpdate;
            newSimilarities = tmp2;
        }
    }
}
