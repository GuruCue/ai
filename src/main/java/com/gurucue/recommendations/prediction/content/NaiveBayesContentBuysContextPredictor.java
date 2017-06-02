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
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.list.TFloatList;
import gnu.trove.list.TIntList;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.linked.TLongLinkedList;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TIntHashSet;
import gnu.trove.set.hash.TLongHashSet;

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
import com.gurucue.recommendations.recommender.dto.Attr;
import com.gurucue.recommendations.recommender.dto.ConsumerMetaEventsData;
import com.gurucue.recommendations.recommender.dto.ContextDiscretizer;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.ProductData;
import com.gurucue.recommendations.recommender.dto.TagsManager;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;


/**
 * A recommender that counts the number of times a product was bought by a consumer. It returns a proportion of bought products in a given context.
 * 
 * Assumption: there are at most MAX_PRODUCTS products and MAX_CONTEXT contexts.
 */
public class NaiveBayesContentBuysContextPredictor extends Predictor {
    private final Logger logger;
    private final int ALL_ATT_USER = -9;
    
    // number of attributes will never exceed this value
    private final int MAX_ATTR_VALUES;
    // number of contexts will never exceed this value
    private final int MAX_CONTEXTS;
   
    private final int [] ATTRIBUTES;
    private final int [] EXPLAINABLE_ATTRIBUTES;
    private final float [] ATTRIBUTES_WEIGHTS;
    
    // should naive bayes add constraints to tags?
    private final boolean ADD_CONSTRAINTS;
    // attributes to be used for constraints
    private final TIntSet CONSTRAINT_ATTRIBUTES;
    // number of attributes in combination
    private final int COMBINATION_SIZE;
    // number of attributes per user
    private final int MAX_ITEMS_USER;
    // index of timestamp meta
    private final int TIMESTAMP_META;
    
    
    final long consumerBase;
    final long attrBase;
    final long allAttributes;
    
    // M used in m-estimate
    private final float M;
    
    // the name of the event
    private final String BUYSNAME;
    
    // context handler
    final ContextDiscretizer contextHandler;
    
    // counts of attributes
    TIntObjectMap<TLongIntMap> attrCounts;
    
    // counts for updating
    TIntObjectMap<TLongIntMap> newAttrCounts;
    
    // update consumer dates - for deleting
    TIntLongMap lastUpdate; 

    // last 25% of consumer items
    TIntObjectMap<TLongList> consumerItems25;
    
    long allViews;

    private DataStore data;
    
    public NaiveBayesContentBuysContextPredictor(String name, Settings settings) {
        super(name, settings);
        
        logger = LogManager.getLogger(NaiveBayesContentBuysContextPredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        BUYSNAME = settings.getSetting(name + "_BUYS");

        MAX_ATTR_VALUES = settings.getSettingAsInt(name + "_MAX_ATTR_VALUES");
        MAX_CONTEXTS = settings.getSettingAsInt(name + "_MAX_CONTEXTS");
        ATTRIBUTES = settings.getAsIntArray(name + "_ATTRIBUTES");
        EXPLAINABLE_ATTRIBUTES = settings.getAsIntArray(name + "_EXPLAINABLE_ATTRIBUTES");
        ATTRIBUTES_WEIGHTS  = settings.getAsFloatArray(name + "_ATTRIBUTES_WEIGHTS");
        COMBINATION_SIZE = settings.getSettingAsInt(name + "_COMBINATION_SIZE");
        MAX_ITEMS_USER = settings.getSettingAsInt(name + "_MAX_ITEMS_USER");
        TIMESTAMP_META = settings.getSettingAsInt(name + "_TIMESTAMP_META");
        
        Boolean ac = settings.getSettingAsBoolean(name + "_ADD_CONSTRAINTS");
        if (ac == null)
        {
        	ADD_CONSTRAINTS = false;
        	CONSTRAINT_ATTRIBUTES = new TIntHashSet();
        }
        else
        {
        	ADD_CONSTRAINTS = ac;
        	CONSTRAINT_ATTRIBUTES = new TIntHashSet(settings.getAsIntArray(name + "_CONSTRAINT_ATTRIBUTES"));
        }
        
        M = settings.getSettingAsFloat(name + "_M");
        
        consumerBase = (ATTRIBUTES.length + 1) * MAX_ATTR_VALUES * MAX_CONTEXTS;
        allAttributes = ATTRIBUTES.length * MAX_ATTR_VALUES;
        attrBase = MAX_CONTEXTS;

        
        
        // initialize context handler
        contextHandler = new ContextDiscretizer(name, settings);
    }

    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) 
    {
    	if (predictions.size() == 0)
    		return;
    	
        float allViewsConsumer = 0;
        int consumerIndex = -1;
        TLongIntMap userCounts = null;
        TLongIntMap predictionCounter = new TLongIntHashMap();
        
        for (ProductRating pr : predictions)
        {
            if (consumerIndex < 0)
            {
                consumerIndex = pr.getConsumerIndex();
                if (attrCounts.containsKey(consumerIndex) && attrCounts.get(consumerIndex) != null)
                	allViewsConsumer = attrCounts.get(consumerIndex).get(-1);
                else
                	allViewsConsumer = 0;
                userCounts = attrCounts.get(consumerIndex);
            }
            final int productIndex = pr.getProductIndex();
            final ProductData productData = pr.getProductData();
            TIntList contextPositions = contextHandler.getContext(data, productIndex, tags);

            final TLongList attrValues = getAttrValues(productIndex);
            // naive bayes is also adding constraint, need to store all attribute values in the recommendations
            if (ADD_CONSTRAINTS)
            {
                //final TLongList attrValuesCons = getConstrainAttrValues(productIndex);
                // add values to prediction counter
                for (TLongIterator it = attrValues.iterator(); it.hasNext(); )
                {
                	final long val = it.next();
                	if (CONSTRAINT_ATTRIBUTES.contains(getAttr(val)))
                	{
	                	predictionCounter.put(val, predictionCounter.get(val) + 1);
	                	// set this tag to product pr
	                	pr.setTag("naive_bayes_"+val, "");
                	}
                }
            }

            if (attrValues.size() == 0 || userCounts == null)
            {
                addProductRating(pr, 0, "");
                continue;
            }
            double prediction = 1.0;

            TLongSet explainValues = new TLongHashSet();
            // all views alltogether
            // all views of this consumer
            
            StringBuilder sb = new StringBuilder();
            float weights = 0.0f;
            for (TLongIterator it = attrValues.iterator(); it.hasNext(); )
            {
                final long atVal = it.next();
                
                // get all views of this attribute
                final float allViewsAtt = attrCounts.get(ALL_ATT_USER).get(atVal);
                // compute prior probability of this att
                final float p0 = (allViewsAtt+0.000001f) / (allViews+2);
                
                // get all views of this attribute for this user
                final float consumerViewsAtt = userCounts.get(atVal);
                // compute prior probability of buying this product for consumer
                final float p0_user = (consumerViewsAtt + M * p0) / (allViewsConsumer + M);
                
                // compute probabilities per context
                float factor = 1;
                for (TIntIterator itc = contextPositions.iterator(); itc.hasNext();) 
                {
                    final int context = itc.next();
                    factor *= (userCounts.get(atVal+context) + M * p0_user) / (userCounts.get(-1-context) + M);
                    factor /= p0;
                }
                final float weight = ATTRIBUTES_WEIGHTS[getAttrLocalIndex(atVal)]/contextPositions.size();
                weights += weight;
                factor = (float) Math.min(1.0+consumerViewsAtt/2, Math.pow(factor, weight));
                factor = (float) Math.max(0.9, factor);
                prediction *= factor;
                if (factor > 1.99)
                    explainValues.add(atVal);
            }

            prediction = Math.pow(prediction, 1.0/weights);
            // prepare explanation
            if (explainValues.size() > 0)
            {
            	TIntList explInds = new TIntArrayList();
            	TIntList explVals = new TIntArrayList();
                sb.append("you like nb:(");
                for (TLongIterator it = explainValues.iterator(); it.hasNext(); )
                {
                    final long atVal = it.next();
                    fillAttribute(atVal, explInds, explVals);
                }
                TIntIterator it1 = explInds.iterator();
                TIntIterator it2 = explVals.iterator();
                for (; it1.hasNext(); )
                {
                    sb.append(productData.getStrAttrValue(ATTRIBUTES[it1.next()], it2.next()) + ",");
                }
                sb.append(");");
                // pretty explanation
                it1 = explInds.iterator();
                it2 = explVals.iterator();
                for (; it1.hasNext(); )
                {
                    final int attr = it1.next();
                    final int val = it2.next();
                    if (EXPLAINABLE_ATTRIBUTES[attr] == 1)
                    {
                    	StringBuilder pretty = new StringBuilder();
                    	pretty.append("you like: ");
                    	final String val_tmp = productData.getStrAttrValue(ATTRIBUTES[attr], val);
                    	if (val_tmp.startsWith("series"))
                    		pretty.append("this series");
                    	else
                    		pretty.append(productData.getStrAttrValue(ATTRIBUTES[attr], val));
                    	pr.addPrettyExplanation(pretty.toString(), 90.0f);
                    }
                }
            }
            prediction = Math.max(prediction-1, 0); // if prediction equals 1, on average you dont prefer this item
            
            addProductRating(pr, prediction, sb.toString());
        }
        
        if (ADD_CONSTRAINTS && userCounts != null)
        {
        	final int nitems = predictions.size();
        	final int nrecommend = Integer.parseInt(tags.get(TagsManager.MAX_RECOMMEND_TAG));
        	
    		if (!tags.containsKey(TagsManager.SECONDARY_TAG) || tags.get(TagsManager.SECONDARY_TAG).equals(""))
    			tags.put(TagsManager.SECONDARY_TAG, "naive_bayes_novalue");
    		else
    			tags.put(TagsManager.SECONDARY_TAG, tags.get(TagsManager.SECONDARY_TAG)+";"+"naive_bayes_novalue");
    		tags.put("MAX_ITEMS_" + "naive_bayes_novalue", "1");
    		
    		// compute probabilities of each attribute
            double max_pred_views = 0;
        	for (TLongIntIterator it = predictionCounter.iterator(); it.hasNext(); )
        	{
        		it.advance();
        		final long key = it.key();
                max_pred_views = Math.max(userCounts.get(key), max_pred_views);
            }

        	for (TLongIntIterator it = predictionCounter.iterator(); it.hasNext(); )
        	{
        		it.advance();
        		final long key = it.key();
        		final int val = it.value();
        		
        		// compute minimal value of this key
        		final int minVal = Math.max(0, nrecommend-(nitems - val)/2);
        		
        		// compute user based probability of this key
        		final double consumer_views_att = userCounts.get(key);        		
        		final double p0_user = consumer_views_att / max_pred_views / 2; //(allViewsConsumer + M);
        		
        		// now set constraint for this value
    			tags.put(TagsManager.SECONDARY_TAG, tags.get(TagsManager.SECONDARY_TAG)+";"+"naive_bayes_"+key);
        		tags.put("MAX_ITEMS_" + "naive_bayes_"+key, String.valueOf(Math.max(minVal, Math.round(p0_user*nrecommend+1))));
        	}
        }
    }
    
    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int predictionId,
            Map<String, String> tags) {
        TLongIntMap currentAttrCount = new TLongIntHashMap();
        long allCurrentViews = 0;
        // prepare views given productIndices
        for (TIntIterator it = productIndices.iterator(); it.hasNext(); )
        {
            final int item = it.next();
            final TLongList attrValues = getAttrValues(item);
            
            for (TLongIterator ita = attrValues.iterator(); ita.hasNext(); )
            {
                final long atVal = ita.next();
                currentAttrCount.put(atVal, currentAttrCount.get(atVal) + 1);
            }
            allCurrentViews++;
        }
        for (ProductRating pr : predictions)
        {
            final int productIndex = pr.getProductIndex();
            final TLongList attrValues = getAttrValues(productIndex);

            if (attrValues.size() == 0)
            {
                addProductRating(pr, 0, "");
                continue;
            }
            double prediction = 1.0;

            String explanation = "";
            // all views alltogether
            // all views of this consumer
            
            for (TLongIterator it = attrValues.iterator(); it.hasNext(); )
            {
                final long atVal = it.next();
                
                // get all views of this attribute
                final float allViewsAtt = attrCounts.get(ALL_ATT_USER).get(atVal);
                // compute prior probability of this att
                final float p0 = (allViewsAtt + 0.01f) / (allViews+2);
                
                // get all views of this attribute for this user
                final float consumerViewsAtt = currentAttrCount.get(atVal);
                // compute prior probability of buying this product for consumer
                final float p0_user = (consumerViewsAtt + M * p0) / (allCurrentViews + M);

                float factor = (float) Math.min(3.0, p0_user/p0);
                final float weight = 1.0f; //ATTRIBUTES_WEIGHTS[(int) (atVal / MAX_ATTR_VALUES)];
                factor = (float) Math.pow(factor, weight);
                
                prediction *= factor;
                if (factor > 1.3)
                {
                    explanation += atVal + "(" + allViewsAtt + "," + allViews + "," + consumerViewsAtt + "," + allCurrentViews + "," + factor + ")" + ";";
                }
            }
            if (prediction < 0)
            {
            	logger.error("Prediction value in get similar is less than 0, shall not pass! Run test, find error! ");
                for (TLongIterator it = attrValues.iterator(); it.hasNext(); )
                {
                    final long atVal = it.next();
                    
                    // get all views of this attribute
                    final float allViewsAtt = attrCounts.get(ALL_ATT_USER).get(atVal);
                    // get all views of this attribute for this user
                    final float consumerViewsAtt = currentAttrCount.get(atVal);
                    // compute prior probability of buying this product for consumer
                    final float p0 = (allViewsAtt + 0.01f) / (allViews+2);
                    final float p0_user = (consumerViewsAtt + M * p0) / (allCurrentViews + M);
                    float factor = (float) Math.min(3.0, p0_user/p0);
                    logger.error(String.valueOf(allViewsAtt));
                    logger.error(String.valueOf(allViews));
                    logger.error(String.valueOf(allCurrentViews));
                    logger.error(String.valueOf(consumerViewsAtt));
                    logger.error(String.valueOf(factor));
                    
                    prediction *= factor;
                    if (factor > 1.3)
                    {
                        explanation += atVal + "(" + allViewsAtt + "," + allViews + "," + consumerViewsAtt + "," + allCurrentViews + "," + factor + ")" + ";";
                    }
                }

            	prediction = 0;
            }
            if (Double.isNaN(prediction))
            {
            	logger.error("Prediction value in get similar is NaN, shall not pass! Run test, find error! ");
            	prediction = 0;
            }
            prediction = Math.pow(prediction, 1.0/ATTRIBUTES.length);
            prediction = Math.max(prediction-1, 0); // if prediction equals 1, on average you dont prefer this item
            
            addProductRating(pr, prediction, explanation);
        }
        
    }
    
    /**
     * Computes one long value representing all values of attributes. 
     * Assuming that attributes are sorted (indices and values). 
     * 
     * @param attrIndices
     * @param attrValues
     * @return
     */
    private long getCombinedAttributeValue(TIntList attrIndices, TIntList attrValues)
    {
    	long result = 0;
    	
    	TIntIterator itInd = attrIndices.iterator();
    	TIntIterator itVal = attrValues.iterator();
    	for (int i = 0; itInd.hasNext(); i++)
    	{
    		final int index = itInd.next();
    		final int value = itVal.next();
    		
    		result += (index * MAX_ATTR_VALUES + value) * Math.pow(allAttributes, i);
    	}
    	
    	return result * attrBase;
    }
    
    /**
     * Inverse to getCombinedAttributeValue. From a combined value, create a list of separate attribute values.
     * 
     * @param key
     * @param indices
     * @param values
     */
    void fillAttribute(final long key, TIntList indices, TIntList values)
    {
    	// remove context
    	long bareKey = key / attrBase;
    	
    	while (bareKey > 0)
    	{
        	long partKey = bareKey % allAttributes;
        	
        	final long index = partKey / MAX_ATTR_VALUES;
        	final long value = partKey % MAX_ATTR_VALUES;
        	
        	indices.add((int) index);
        	values.add((int) value);
        	
        	bareKey /= allAttributes;
    	}
    }
    
    // returns index of attribute that this key represents
    // if key represents several attributes, then -1 is returned
    int getAttr(final long key)
    {
    	return ATTRIBUTES[getAttrLocalIndex(key)];
    }

    int getAttrLocalIndex(final long key)
    {
    	// remove context
    	final long bareKey = key / attrBase;
    	
    	// get first attribute
    	final long partKey = bareKey % allAttributes;
    	if (partKey != bareKey)
    		return -1;
    	
    	return (int) (partKey / MAX_ATTR_VALUES);
    }
    

    /**
     * Gets attribute values for a product. If specified, then also combinations of attributes are generated. 
     * 
     * @param productIndex
     * @return
     */
    TLongList getAttrValues(final int productIndex)
    {
        final ProductData pd = data.getProductByIndex(productIndex);
        // values and indices of subsets
        List<TIntList> indices = new ArrayList<TIntList> ();
        indices.add(new TIntArrayList());
        List<TIntList> values = new ArrayList<TIntList> ();
        values.add(new TIntArrayList());
        for (int i = 0; i < COMBINATION_SIZE; i++)
        {
        	final int size = indices.size();
        	for (int oldset = 0; oldset < size; oldset++)
        	{
        		final TIntList oldIndices = indices.get(oldset);
        		final int oldSize = oldIndices.size();
        		if (oldSize != i) // do not add to shorter versions
        			continue;
        		final TIntList oldValues = values.get(oldset);
        		
        		int attr_index = -1;
                for (int attr : ATTRIBUTES)
                {
                	attr_index++;
                	if (oldSize > 0 && oldIndices.get(oldSize-1) > attr_index) // indices should be increasing
                		continue;
                    final Attr a = pd.getAttribute(attr);
                    if (a == null)
                    	continue;
                    final TIntSet avals = a.getValues();
                    if (avals == null)
                    	continue;
                    for (TIntIterator it = avals.iterator(); it.hasNext(); )
                    {
                    	final int val = it.next();
                    	// also values should be increasing
                    	if (oldSize > 0 && oldIndices.get(oldSize-1) == attr_index && oldValues.get(oldSize-1) >= val)
                    		continue;
                    	
                    	TIntList newIndices = new TIntArrayList(oldIndices);
                    	TIntList newValues = new TIntArrayList(oldValues);
                    	newIndices.add(attr_index);
                    	newValues.add(val);
                    	indices.add(newIndices);
                    	values.add(newValues);
                    }
                    	
                }
        	}
        }
        // compute combined values
        TLongList combined_values = new TLongArrayList();
        final int size = indices.size();
    	for (int oldset = 0; oldset < size; oldset++)
    	{
    		final TIntList oldIndices = indices.get(oldset);
    		final TIntList oldValues = values.get(oldset);
    		if (oldIndices.size() == 0)
    			continue;
    		
    		combined_values.add(getCombinedAttributeValue(oldIndices, oldValues));
    	}
        return combined_values;
    }

    /**
     * Increase counter for each bought product in each possible context. 
     */
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData updateData) {
        logger.info("Incremental update of NaiveBayesContentBuysContextPredictor started.");
        // update data
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = updateData.dataStore;
            attrCounts = new TIntObjectHashMap<TLongIntMap> ();
            lastUpdate = new TIntLongHashMap(); 
            consumerItems25 = new TIntObjectHashMap<TLongList> ();            
            allViews = 0;
        }
        final int eventIndex = data.getEventsDescriptor(BUYSNAME).index;
        final long currentUpdate = data.getEventsDescriptor(BUYSNAME).last_read_date;
        
        // new (changed) consumers
        final List<ConsumerData> tmp_consumers = updateData.newData;
        final TLongIntHashMap tmp_ids = updateData.newConsumerIDs;
        
        if (newAttrCounts == null)
        	newAttrCounts = new TIntObjectHashMap<TLongIntMap> ();
        
        // copy all views
        long newViews = allViews;
        if (newViews < 0)
        {
            logger.warn("New views was less than 0 before update");
        }

        // copy ALL_ATT_USER
        if (attrCounts.containsKey(ALL_ATT_USER))
        	newAttrCounts.put(ALL_ATT_USER, new TLongIntHashMap(attrCounts.get(ALL_ATT_USER)));
        else
        	newAttrCounts.put(ALL_ATT_USER, new TLongIntHashMap());
        
        // delete users that have not had an event in certain time (2 months)
        // TODO: this should be moved to settings
        for (TIntObjectIterator<TLongIntMap> it = attrCounts.iterator(); it.hasNext(); )
        {
        	it.advance();
        	final int key = it.key();
        	final long lastUserUpdate = lastUpdate.get(key);
        	// 2 months = 60 * 24 * 60 * 60 = 5184000
        	if (lastUserUpdate > 0 && currentUpdate - lastUserUpdate > 5184000)
        	{
        		final TLongIntMap oldCounts = newAttrCounts.get(key);
        		if (oldCounts == null)
        			continue;
        			
        		newAttrCounts.put(key, null);
        		// remove from ALL_ATT_USER and newViews;
                for (TLongIntIterator it2 = oldCounts.iterator(); it2.hasNext(); )
                {
                	it2.advance();
                	final int counts = it2.value();
                	final long atVal = it2.key();
            		if (atVal%attrBase != 0)
            			continue;                	
                	
                    newAttrCounts.get(ALL_ATT_USER).put(atVal, newAttrCounts.get(ALL_ATT_USER).get(atVal) - counts);
                    newViews -= counts;
                }        		
        	}
        }

        for (ConsumerData c: tmp_consumers)
        {
            if (null == c) // no events were added to this user
                continue;
            
            // are there any new events for this user, otherwise continue
            if (null == c.events[eventIndex])
                continue;
            final int consIndex = tmp_ids.get(c.consumerId);
            final int [] items = c.events[eventIndex].getProductIndices();
            final ConsumerMetaEventsData meta = c.events[eventIndex].getMeta();
            
            if (attrCounts.containsKey(consIndex) && attrCounts.get(consIndex) != null)
            	newAttrCounts.put(consIndex, new TLongIntHashMap(attrCounts.get(consIndex)));
            else
            {
            	newAttrCounts.put(consIndex, new TLongIntHashMap());
            	lastUpdate.put(consIndex,  currentUpdate);
            	consumerItems25.put(consIndex,  new TLongArrayList()); 
            }
            TLongIntMap userCounts = newAttrCounts.get(consIndex);
            TLongList currentConsumerItems = consumerItems25.get(consIndex);
            for (int i = 0; i < items.length; i++)
            {
                final int item = items[i];
                final TLongList attrValues = getAttrValues(items[i]);
                final TIntList contextPositions = contextHandler.getContext(data, item, meta, i);
                
                for (TIntIterator it2 = contextPositions.iterator(); it2.hasNext(); )
                {
                    final long cont = it2.next();
                    for (TLongIterator it = attrValues.iterator(); it.hasNext(); )
                    {
                        final long atVal = it.next() + cont;
                        userCounts.put(atVal, userCounts.get(atVal) + 1);
                        userCounts.put(-1-cont, userCounts.get(-1-cont) + 1);
                    }
                    // store also all views for each context (key = -1-cont)
                }
                
                // store also general counter (without context)
                // for everyone
                // & update dates for attributes
                for (TLongIterator it = attrValues.iterator(); it.hasNext(); )
                {
                    final long atVal = it.next();
                    newAttrCounts.get(ALL_ATT_USER).put(atVal, newAttrCounts.get(ALL_ATT_USER).get(atVal) + 1);
                    newViews += 1;
                    // update current consumer items
                    currentConsumerItems.remove(atVal);
                    currentConsumerItems.add(atVal);
                    // if current items contain more than 25% of allowed items, remove one
                    if (currentConsumerItems.size() > 0.25 * MAX_ITEMS_USER)
                    	currentConsumerItems.removeAt(0);
                }
                
                // if user has too many items, remove
            }
            
            if (userCounts.size() > MAX_ITEMS_USER)
            {
            	// WE need to delete some items. For each user store last 25% of MAX_ITEMS_USER items. Those should not be deleted.  
            	while (userCounts.size() > MAX_ITEMS_USER)
            	{
            		// decrease each key by the number of contexts; if value becomes 0, remove it from the counts
            		int changed = 0;
            		for (TLongIntIterator it = userCounts.iterator(); it.hasNext(); )
            		{
            			it.advance();
            			final long key = it.key();
                		if (key < 0 || key%attrBase > 0 || userCounts.get(key) <= 0)
                			continue;
                		if (currentConsumerItems.contains(key))
                			continue;
                
                		changed ++;

                		// remove from contexts
                        for (int ic = 0; ic < MAX_CONTEXTS; ic++)
                        {
                        	if (userCounts.containsKey(key + ic) && userCounts.get(key+ic) > 0)
                        		userCounts.put(key+ic, userCounts.get(key+ic) - 1);
                        	if (userCounts.containsKey(-1-ic) && userCounts.get(-1-ic) > 0)
                        		userCounts.put(-1-ic, userCounts.get(-1-ic) - 1);
                        }
                        
                        newViews -= 1;
                        newAttrCounts.get(ALL_ATT_USER).put(key, newAttrCounts.get(ALL_ATT_USER).get(key) - 1);
            		}
            		for (TLongIntIterator it = userCounts.iterator(); it.hasNext(); )
            		{
            			it.advance();
            			if (it.value() <= 0)
            				it.remove();
            		}
            		if (changed == 0)
            			break;
            	}

                // test whether counts are correct
                int cnt = 0;
                for (TLongIntIterator it = userCounts.iterator(); it.hasNext();)
                {
                    it.advance();
                    final long key = it.key();
                    if (key < 0 || key%attrBase > 0 || userCounts.get(key) <= 0)
                        continue;

                    cnt += userCounts.get(key);
                }
                if (cnt != userCounts.get(-1))
                    logger.error("Wrong values for user " + c.consumerId + ", cnt = " + cnt + ", all = " + userCounts.get(-1));
            	
            }
                		
        }
        
        if (newViews < 0)
        {
            logger.warn("New views was less than 0 after update");
        }
        logger.info("Incremental update of NaiveBayesContentBuysContextPredictor ended.");
        return new UpdateAll(newAttrCounts, newViews);
    }

    /**
     * Remove nonrelevant products / add new products. 
     */
    @Override
    public Commitable updateProducts(DataStore.UpdateProductsDelta delta) {
        return EmptyCommit.INSTANCE;
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
        logger.error("getBestPair not implemented yet for NaiveBayesContentBuysContextPredictor.");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumer_index) {
        logger.error("needsProfiling not implemented yet for NaiveBayesContentBuysContextPredictor.");
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
		out.writeObject(lastUpdate);
		out.writeObject(consumerItems25);
		out.writeLong(allViews);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        logger.info("Update model from file of NaiveBayesContentBuysContextPredictor started.");
        // update data
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = data;
            attrCounts = new TIntObjectHashMap<TLongIntMap> ();
            allViews = 0;
        }
        newAttrCounts = (TIntObjectMap<TLongIntMap>) in.readObject(); 
		lastUpdate = (TIntLongMap) in.readObject(); 
        consumerItems25 = (TIntObjectMap<TLongList>) in.readObject(); 
		long newAllViews = in.readLong();
        logger.info("Update model from file of NaiveBayesContentBuysContextPredictor ended.");
        return new UpdateAll(newAttrCounts, newAllViews);
	}    


    private class UpdateDelta implements Commitable {
    	TIntObjectMap<TLongIntMap> newCounts;
    	long newAllViews;
    
        UpdateDelta(TIntObjectMap<TLongIntMap> newCounts, long newAllViews) {
            this.newCounts = newCounts;
            this.newAllViews = newAllViews;
        }

        @Override
        public void commit() {
        	allViews = newAllViews;
            for (TIntObjectIterator<TLongIntMap> it = newCounts.iterator(); it.hasNext(); )
            {
                it.advance();
                final int key = it.key();
                attrCounts.put(key, newCounts.get(key));
            }
        }        
    }
    
    private class UpdateAll implements Commitable {
    	TIntObjectMap<TLongIntMap> newCounts;
    	long newAllViews;
    
        UpdateAll(TIntObjectMap<TLongIntMap> newCounts, long newAllViews) {
            this.newCounts = newCounts;
            this.newAllViews = newAllViews;
        }

        @Override
        public void commit() {
        	attrCounts = newCounts;
        	allViews = newAllViews;
        }        
    }    
}
