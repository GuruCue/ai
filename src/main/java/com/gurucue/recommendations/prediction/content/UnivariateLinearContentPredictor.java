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

import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.EmptyCommit;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.Attr;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ProductData;
import gnu.trove.iterator.TIntFloatIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongLongIterator;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.ConsumerMetaEventsData;
import com.gurucue.recommendations.recommender.dto.ConsumerRatingsData;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;

/**
 * This recommender estimates average ratings for each consumer and for each attribute in the ATTRIBUTES setting.
 * Then, during the prediction phase the max and the min average values of product's attributes are taken, added together and 
 * returned. This approach is - in a certain way - a modification of naive bayesian classifier for regression problems, 
 * with a distinct difference: only two most influential factors are taken, mainly due to a high dependency between attributes.
 */
public class UnivariateLinearContentPredictor extends Predictor {
	private final int ALL_ATT_USER = -9;
    private final Logger logger;
    
    // number of attribute values should never exceed this value
    private final int MAX_ATTR_VALUES;
    
    // attributes used in learning
    private final int [] ATTRIBUTES;
    private final int [] EXPLAINABLE_ATTRIBUTES;
    
    // blending factor used in estimation of average
    private final float BLENDING;
    
    // the name of the event
    private final String RATINGSNAME;
    
    // max attribute values per user
    private final int MAX_ITEMS_USER;
    
    // id of timestamp meta
    final int TIMESTAMP_ID;
    
    // under which value, the item should not be recommended anymore? default: 0
    final int THRESHOLD;

    
    // averages of prod
    private TIntObjectMap<TLongFloatMap> attrAverages; // averages of attributes for each user
    private TIntObjectMap<TLongLongMap> attrCounts; // number of times an attribute occured by a user
    private TIntObjectMap<TLongLongMap> attrDates; // date when attribute was last seen by a user
    private TIntFloatMap userAverages; // general user averages
    private TIntLongMap userCounts; // user counts 
    
    // the following objects are used to compute an update of the model. 
    TIntObjectMap<TLongFloatMap> newAttrAverages;
    TIntObjectMap<TLongLongMap> newAttrCounts;
    TIntFloatMap newUserAverages;
    TIntLongMap newUserCounts;    
    
    private DataStore data;	
    
    // global average over all attributes
    private float globalAverage;
    // global count over all attributes 
    private long globalCount;

	public UnivariateLinearContentPredictor(String name, Settings settings) {
        super(name, settings);
        
        logger = LogManager.getLogger(UnivariateLinearContentPredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        RATINGSNAME = settings.getSetting(name + "_RATINGS");

        MAX_ATTR_VALUES = settings.getSettingAsInt(name + "_MAX_ATTR_VALUES");
        ATTRIBUTES = settings.getAsIntArray(name + "_ATTRIBUTES");
        EXPLAINABLE_ATTRIBUTES = settings.getAsIntArray(name + "_EXPLAINABLE_ATTRIBUTES");
        
        BLENDING = settings.getSettingAsFloat(name + "_BLENDING");
        //consumerBase = (ATTRIBUTES.length + 1) * MAX_ATTR_VALUES;
        
        MAX_ITEMS_USER = settings.getSettingAsInt(name + "_MAX_ITEMS_USER"); 
        TIMESTAMP_ID = settings.getSettingAsInt(name+"_TIMESTAMP_META");   
        
        Integer threshold = settings.getSettingAsInt(name + "_THRESHOLD");
        if (threshold == null)
        	THRESHOLD = 0;
        else
        	THRESHOLD = threshold;
	}

	@Override
	public void getPredictions(List<ProductRating> predictions,
			Map<String, String> tags) 
	{
        int consumerIndex = -1;
        float consumerAverage = 0;
        float globalAvg = 0;
        for (ProductRating pr : predictions)
        {
            if (consumerIndex < 0)
            {
                consumerIndex = pr.getConsumerIndex();
                globalAvg = globalAverage / globalCount;
                consumerAverage = (userAverages.get(consumerIndex) + globalAvg)/ (userCounts.get(consumerIndex) + 1);
            }
            final int productIndex = pr.getProductIndex();
            final ProductData productData = pr.getProductData();
            
            final TLongList attrValues = getAttrValues(productIndex);

            int maxCounts = 0;
            double prediction = 0; //consumerAverage;
            double weights = 0.0;
            StringBuilder sb = new StringBuilder();
            int rated_n = 0;
            final int values_n = attrValues.size(); 
            for (TLongIterator it = attrValues.iterator(); it.hasNext(); )
            {
                final long atVal = it.next();

                float avg=0, cnt=0;
                if (attrCounts.containsKey(consumerIndex) && attrCounts.get(consumerIndex) != null)
                {
                	avg = attrAverages.get(consumerIndex).get(atVal);
                	cnt = attrCounts.get(consumerIndex).get(atVal);
                }
                
                if (cnt > 0)
                	rated_n += 1;
                else
                	continue;
                
                // get average of this attribute
                final float genAttrAverage = (attrAverages.get(ALL_ATT_USER).get(atVal) + globalAvg) / (attrCounts.get(ALL_ATT_USER).get(atVal) + 1);
                final float attrAverage_user = (avg + BLENDING * consumerAverage) / (cnt + BLENDING);
                final float attrAverage_attr = (avg + BLENDING * genAttrAverage) / (cnt + BLENDING);
                final int attr = (int) (atVal / MAX_ATTR_VALUES);
                final int val = (int) (atVal % MAX_ATTR_VALUES);

                maxCounts = (int) Math.max(maxCounts, cnt);
                final double w = Math.log( ((float)globalCount) / (attrCounts.get(ALL_ATT_USER).get(atVal) + 1.0)) * Math.log(cnt + 1.1);
                prediction += w * (attrAverage_user - consumerAverage);
                weights += w;

                if (attrAverage_user > consumerAverage && attrAverage_attr > genAttrAverage)
                {
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
                    sb.append(productData.getStrAttrValue(ATTRIBUTES[attr], val));
                    sb.append(" has avg rating ");
                    sb.append(attrAverage_user);
                    sb.append(",");
                    sb.append(attrAverage_attr);
                    sb.append("(avg:");
                    sb.append(consumerAverage);
                    sb.append(", genavg: ");
                    sb.append(genAttrAverage);
                    sb.append(", cnt: ");
                    sb.append(cnt);
                    sb.append(");");
                }
            }

            if (attrValues.size() == 0 || weights < 1e-6)
            	prediction = 0;
            else
            	prediction = consumerAverage + prediction / weights;
            if (prediction < THRESHOLD)
            	prediction = 0;
            if (prediction > 0)
            	pr.setPrediction(prediction, (rated_n + 1.0f) / (values_n + 1.0f), this.ID, sb.toString());
            else
            	pr.setPrediction(0, (rated_n + 1.0f) / (values_n + 1.0f), this.ID, "");
        }
    }	


	@Override
	public Commitable updateModelIncremental(UpdateIncrementalData updateData) {
        logger.info("Incremental update of UnivariateLinearContentPredictor started.");
        // update data
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = updateData.dataStore;
            attrAverages = new TIntObjectHashMap<TLongFloatMap> ();
            userAverages = new TIntFloatHashMap ();
            attrCounts = new TIntObjectHashMap<TLongLongMap> ();
            userCounts = new TIntLongHashMap ();
            attrDates = new TIntObjectHashMap<TLongLongMap> ();
        }
        
        // add user with ID -1 (contains all values)
        if (!attrAverages.containsKey(ALL_ATT_USER))
        {
        	attrAverages.put(ALL_ATT_USER, new TLongFloatHashMap());
    		attrCounts.put(ALL_ATT_USER, new TLongLongHashMap());
        }
        newAttrAverages = new TIntObjectHashMap<TLongFloatMap> ();
        newAttrCounts = new TIntObjectHashMap<TLongLongMap> ();
      	newAttrAverages.put(ALL_ATT_USER, new TLongFloatHashMap());
      	newAttrCounts.put(ALL_ATT_USER, new TLongLongHashMap());
        
        newAttrAverages.get(ALL_ATT_USER).putAll(attrAverages.get(ALL_ATT_USER));
        newAttrCounts.get(ALL_ATT_USER).putAll(attrCounts.get(ALL_ATT_USER));
        
        final int eventIndex = data.getEventsDescriptor(RATINGSNAME).index;

        // new (changed) consumers
        final List<ConsumerData> tmp_consumers = updateData.newData;
        final TLongIntHashMap tmp_ids = updateData.newConsumerIDs;

        newUserAverages = new TIntFloatHashMap ();
        newUserCounts = new TIntLongHashMap ();        
        newUserAverages.putAll(userAverages);
        newUserCounts.putAll(userCounts);
        
        float newGlobalAverage = globalAverage;
        long newGlobalCount = globalCount;

        for (ConsumerData c: tmp_consumers)
        {
            if (null == c) // no events were added to this user
                continue;
            
            // are there any new events for this user, otherwise continue
            if (null == c.events[eventIndex])
                continue;
            final int consIndex = tmp_ids.get(c.consumerId);
            // create a new object to store dates
            if (!attrDates.containsKey(consIndex))
            	attrDates.put(consIndex, new TLongLongHashMap());
            if (!newAttrAverages.containsKey(consIndex))
                newAttrAverages.put(consIndex, new TLongFloatHashMap());
            if (attrAverages.get(consIndex) != null)
            	newAttrAverages.get(consIndex).putAll(attrAverages.get(consIndex));
            if (!newAttrCounts.containsKey(consIndex))
                newAttrCounts.put(consIndex, new TLongLongHashMap());
            if (attrCounts.get(consIndex) != null)
            	newAttrCounts.get(consIndex).putAll(attrCounts.get(consIndex));
            final TLongFloatMap averages = newAttrAverages.get(consIndex);
        	final TLongLongMap dates = attrDates.get(consIndex);
        	final TLongLongMap counts = newAttrCounts.get(consIndex);
           
            
            final ConsumerRatingsData ratingsData =  (ConsumerRatingsData) c.events[eventIndex];
            final ConsumerMetaEventsData meta = ratingsData.getMeta();
            final int [] items = ratingsData.indices;
            final byte [] ratings = ratingsData.ratings;
            final long[] times = meta.getLongValuesArray(TIMESTAMP_ID);
            
            for (int i = 0; i < items.length; i++)
            {
                final TLongList attrValues = getAttrValues(items[i]);

                // store also general counter (without context)
                // for a consumer and for everyone
                for (TLongIterator it = attrValues.iterator(); it.hasNext(); )
                {
                    final long atVal = it.next();
                    // update user data
                    counts.put(atVal, counts.get(atVal) + 1);
                    averages.put(atVal, averages.get(atVal) + ratings[i]);
                    // update general user data
                    newAttrCounts.get(ALL_ATT_USER).put(atVal, newAttrCounts.get(ALL_ATT_USER).get(atVal) + 1);
                    newAttrAverages.get(ALL_ATT_USER).put(atVal, newAttrAverages.get(ALL_ATT_USER).get(atVal) + ratings[i]);
                    
                    dates.put(atVal, Math.max(attrDates.get(consIndex).get(atVal), times[i]));

                    newUserCounts.put(consIndex, newUserCounts.get(consIndex) + 1);
                    newUserAverages.put(consIndex, newUserAverages.get(consIndex) + ratings[i]);
                    
                    newGlobalAverage += ratings[i];
                    newGlobalCount += 1;
                }
            }
            
            // do we need to delete any values?
        	if (averages.size() > MAX_ITEMS_USER)
        	{
            	// create a list of times
            	TLongList timesSort = new TLongArrayList();
            	for (TLongLongIterator it2 = dates.iterator(); it2.hasNext();)
            	{
            		it2.advance();
            		timesSort.add(it2.value());
            	}
            	// sort by values - to get the oldest
            	timesSort.sort();
            	long threshold = timesSort.get(timesSort.size()-MAX_ITEMS_USER);
                TLongSet todelete = new TLongHashSet();
            	for (TLongLongIterator it2 = dates.iterator(); it2.hasNext();)
            	{
            		it2.advance();
            		if (it2.value() < threshold)
            		{
            			todelete.add(it2.key()); // these keys should be removed
            		}
            	}
            	// iterate through todelete and compute interestingness of the item
            	double maxInteresting = 0;
            	for (TLongIterator it2 = todelete.iterator(); it2.hasNext(); )
            	{
            		final long key = it2.next();
            		final long cnt = counts.get(key);
            		final double interesting = (double) cnt * cnt / (newAttrCounts.get(ALL_ATT_USER).get(key) + 1.0);
            		if (interesting > maxInteresting)
            			maxInteresting = interesting;
            	}
            	
            	// you are allowed to keep 50% of MAX_ITEMS_USER size
            	if (todelete.size() < MAX_ITEMS_USER / 2)
            		todelete.clear();
            	else
            	{
            		double keyThreshold = maxInteresting * (1.0 - (MAX_ITEMS_USER / 2.0) / todelete.size());
            		// remove from delete
                	for (TLongIterator it2 = todelete.iterator(); it2.hasNext(); )
                	{
                		final long key = it2.next();
                		final long cnt = counts.get(key);
                		final double interesting = (double) cnt * cnt / (newAttrCounts.get(ALL_ATT_USER).get(key) + 1.0);

                		if (interesting > keyThreshold) // do not remove this item!
                			it2.remove();
                	}
            	}
            	
            	// remove todelete from averages and counts
            	for (TLongIterator it2 = todelete.iterator(); it2.hasNext(); )
                {
            		final long key = it2.next();
            		final long cnt = counts.get(key);
                	final float a = averages.get(key);
                	
                	// delete from attrCounts
                	counts.remove(key);
                	averages.remove(key);
                	dates.remove(key);
                	// subtract
                	newUserAverages.put(consIndex, newUserAverages.get(consIndex) - a);
                	newUserCounts.put(consIndex, newUserCounts.get(consIndex) - cnt);
                    newAttrAverages.get(ALL_ATT_USER).put(key, newAttrAverages.get(ALL_ATT_USER).get(key) - a);
                    newAttrCounts.get(ALL_ATT_USER).put(key, newAttrCounts.get(ALL_ATT_USER).get(key) - cnt);
                	newGlobalAverage -= a;
                	newGlobalCount -= cnt;
                	if (newGlobalCount < 0)
                		logger.error("New global count negative!!!");
                	if (newAttrCounts.get(ALL_ATT_USER).get(key) < 0)
                		logger.error("ALL_ATT_USER count negative, key = " + key + ", new count = " + newAttrCounts.get(ALL_ATT_USER).get(key) + ", cnt = " + cnt + "!!!");
                	if (counts.get(key) < 0)
                		logger.error("counts count negative!!!");
                }        	
        	}
        }
        

        logger.info("Incremental update of UnivariateLinearContentPredictor ended.");
        return new UpdateDelta(newAttrAverages, newAttrCounts, newUserAverages, newUserCounts, newGlobalAverage, newGlobalCount);	
    }
	
    TLongList getAttrValues(final int productIndex)
    {
        TLongList values = new TLongArrayList();
        final ProductData pd = data.getProductByIndex(productIndex);
        int counter = 0;
        for (int attr : ATTRIBUTES)
        {
            final Attr a = pd.getAttribute(attr);
            if (a == null)
            {
            	counter ++;
                continue;
            }
            final TIntSet avals = a.getValues();
            if (avals == null)
            {
            	counter ++;
                continue;
            }
            for (TIntIterator it = avals.iterator(); it.hasNext(); )
            {
            	final int val = it.next();
                values.add(val%MAX_ATTR_VALUES + MAX_ATTR_VALUES * counter);
                if (val > MAX_ATTR_VALUES)
                	logger.warn("Attribute value index " + val + " is larger than MAX_ATTR_VALUES");
            }
            counter ++;
        }
        return values;
    }

	@Override
	public Commitable updateProducts(UpdateProductsDelta delta) {
		// not needed since attribute ids are not used in the model.
		return EmptyCommit.INSTANCE;
	}

	@Override
	public void getSimilarProducts(TIntSet productIndices,
			List<ProductRating> predictions, int prediction_id,
			Map<String, String> tags) {
		// My opinion: there is no reasonable implementation of getSimilarProducts for this recommender
		// so it is OK to just pass ...
	}

	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		out.writeObject(attrAverages);
		out.writeObject(attrCounts);
		out.writeObject(attrDates);
		out.writeObject(userAverages);
		out.writeObject(userCounts);
		out.writeFloat(globalAverage);
		out.writeLong(globalCount);
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        logger.info("Update model from file of UnivariateLinearContentPredictor started.");
        // update data
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = data;
            attrAverages = new TIntObjectHashMap<TLongFloatMap> ();
            userAverages = new TIntFloatHashMap ();
            attrCounts = new TIntObjectHashMap<TLongLongMap> (); 
            userCounts = new TIntLongHashMap ();
            
            newAttrAverages = new TIntObjectHashMap<TLongFloatMap> ();
            newAttrCounts = new TIntObjectHashMap<TLongLongMap> ();
            newUserAverages = new TIntFloatHashMap ();
            newUserCounts = new TIntLongHashMap ();            
        }
        TIntObjectMap<TLongFloatMap> TnewAttrAverages = (TIntObjectMap<TLongFloatMap>) in.readObject();
        TIntObjectMap<TLongLongMap> TnewAttrCounts = (TIntObjectMap<TLongLongMap>) in.readObject();
        TIntObjectMap<TLongLongMap> TnewAttrDates = (TIntObjectMap<TLongLongMap>) in.readObject();
        TIntFloatMap TnewUserAverages = (TIntFloatMap) in.readObject();
        TIntLongMap TnewUserCounts = (TIntLongMap) in.readObject();
        float TnewGlobalAverage = in.readFloat();
        long TnewGlobalCount = in.readLong();
        logger.info("Update model from file of UnivariateLinearContentPredictor ended.");
        return new UpdateAll(TnewAttrAverages, TnewAttrCounts, TnewUserAverages, TnewUserCounts, TnewGlobalAverage, TnewGlobalCount, TnewAttrDates);
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
        logger.error("getBestPair not implemented yet for UnivariateLinearContentPredictor.");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumer_index) {
        logger.error("needsProfiling not implemented yet for UnivariateLinearContentPredictor.");
        return false;
    }

    @Override
    public Commitable updateConsumer(DataStore.UpdateData updateData) {
        logger.error("updateConsumer is deprecated; should not be called!");
        return null;
    }
    
    private class UpdateAll implements Commitable {
        private TIntObjectMap<TLongFloatMap> newAttrAverages;
        private TIntObjectMap<TLongLongMap> newAttrCounts;
        private TIntFloatMap newUserAverages;
        private TIntLongMap newUserCounts;
        private TIntObjectMap<TLongLongMap> newAttrDates;
        private float newGlobalAverage;
        private long newGlobalCount;
    
        UpdateAll(final TIntObjectMap<TLongFloatMap> newAttrAverages, final TIntObjectMap<TLongLongMap> newAttrCounts, final TIntFloatMap newUserAverages, final TIntLongMap newUserCounts, final float newGlobalAverage, final long newGlobalCount, final TIntObjectMap<TLongLongMap> newAttrDates) {
        	this.newAttrAverages = newAttrAverages;
        	this.newAttrCounts = newAttrCounts;
        	this.newUserAverages = newUserAverages;
        	this.newUserCounts = newUserCounts;
        	this.newGlobalAverage = newGlobalAverage;
        	this.newGlobalCount = newGlobalCount;
        	this.newAttrDates = newAttrDates;
        }

        @Override
        public void commit() {
        	globalAverage = newGlobalAverage;
        	globalCount = newGlobalCount;
        	attrAverages = newAttrAverages;
        	attrCounts = newAttrCounts;
        	userAverages = newUserAverages;
        	userCounts = newUserCounts;
        	attrDates = newAttrDates;
        }        
    }       

    private class UpdateDelta implements Commitable {
        private TIntObjectMap<TLongFloatMap> newAttrAverages;
        private TIntObjectMap<TLongLongMap> newAttrCounts;
        private TIntFloatMap newUserAverages;
        private TIntLongMap newUserCounts;
        private float newGlobalAverage;
        private long newGlobalCount;
        
    
        UpdateDelta(final TIntObjectMap<TLongFloatMap> newAttrAverages, final TIntObjectMap<TLongLongMap> newAttrCounts, final TIntFloatMap newUserAverages, final TIntLongMap newUserCounts, final float newGlobalAverage, final long newGlobalCount) {
        	this.newAttrAverages = newAttrAverages;
        	this.newAttrCounts = newAttrCounts;
        	this.newUserAverages = newUserAverages;
        	this.newUserCounts = newUserCounts;
        	this.newGlobalAverage = newGlobalAverage;
        	this.newGlobalCount = newGlobalCount;
        }

        @Override
        public void commit() {
        	long start = System.currentTimeMillis();
        	// first update all values
        	globalAverage = newGlobalAverage;
        	globalCount = newGlobalCount;
        	for (TIntObjectIterator<TLongFloatMap> it = newAttrAverages.iterator(); it.hasNext(); )
        	{
        		it.advance();
        		final int consIndex = it.key();
        		// swap consumer averages
        		TLongFloatMap tmp1 = attrAverages.get(consIndex); 
        		attrAverages.put(consIndex, it.value());
        		newAttrAverages.put(consIndex, tmp1);
        		// swap consumer counts
        		TLongLongMap tmp2 = attrCounts.get(consIndex);
        		attrCounts.put(consIndex, newAttrCounts.get(consIndex));
        		newAttrCounts.put(consIndex, tmp2);
        		// compute
        		userAverages.put(consIndex, newUserAverages.get(consIndex));
        		userCounts.put(consIndex, newUserCounts.get(consIndex));
        	}
            logger.info("Commit of UnivariateLinearContentPredictor took" + ((System.currentTimeMillis()-start)/1000) + " seconds.");
        }        
    }       
    
}
