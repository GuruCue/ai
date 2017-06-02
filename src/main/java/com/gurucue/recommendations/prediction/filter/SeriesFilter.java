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

import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ProductData;
import com.gurucue.recommendations.recommender.dto.TagsManager;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TIntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.misc.Misc;
import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.ConsumerMetaEventsData;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;

class LastSeriesInfo
{
    long series_id;
    int season, episode;
    long time_watched;
    ProductRating pr;
    
    LastSeriesInfo(long series_id, int season, int episode, long time_watched)
    {
        this.series_id = series_id;
        this.season = season;
        this.episode = episode;
        this.time_watched = time_watched;
    }
    
    LastSeriesInfo(long series_id, int season, int episode, long time_watched, ProductRating pr)
    {
    	this(series_id, season, episode, time_watched);
        this.pr = pr;
    }    
    
    public void update(long series_id, int season, int episode, long time_watched)
    {
        if (time_watched >= 0 && time_watched <= this.time_watched)
            return;
        
        this.series_id = series_id;
        if (season > 0)
        	this.season = season;
        if (episode > 0)
        	this.episode = episode;
        this.time_watched = time_watched;
    }

    public void update(long series_id, int season, int episode, long time_watched, ProductRating pr)
    {
    	this.update(series_id, season, episode, time_watched);
        this.pr = pr;
    }

    public void serialize(ObjectOutputStream out) throws IOException
    {
    	out.writeLong(series_id);
    	out.writeInt(season);
    	out.writeInt(episode);
    	out.writeLong(time_watched);
    }
    
    public static LastSeriesInfo deserialize(ObjectInputStream in) throws IOException
    {
    	final long series_id = in.readLong();
    	final int season = in.readInt();
    	final int episode = in.readInt();
    	final long time_watched = in.readLong();
    	return new LastSeriesInfo(series_id, season, episode, time_watched);
    }
    
    public LastSeriesInfo clone()
    {
        return new LastSeriesInfo(series_id, season, episode, time_watched);
    }
}

/**
 * A recommender that selects only relevant series (only one episode).
 * It gives recommendation 1 if the series was watched in the last predefined period of time.  
 */
public class SeriesFilter extends Predictor {
    private final Logger logger;
    
    // the name of the event
    private final String EVENTSNAME;
    
    // next predictor (that uses filtered events)
    private final Predictor PREDICTOR;
    
    // series, season and episode attribute indices
    private final int SERIES_ATTRIBUTE, SEASON_ATTRIBUTE, EPISODE_ATTRIBUTE;
    
    // time for considering a series being relevant
    private final long RELEVANT_TIME;
    
    // should we add also nonseries?
    private final boolean ADD_NONSERIES;
    
    // data about watched episodes (key = user id, value = episodes watched)
    
    TIntObjectMap<TLongObjectMap<LastSeriesInfo>> seriesData;
    
    private DataStore data;
    
    public SeriesFilter(String name, Settings settings) {
        super(name, settings);
        
        logger = LogManager.getLogger(SeriesFilter.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        EVENTSNAME = settings.getSetting(name + "_EVENT");

        // create predictor
        String predictorName = settings.getSetting(name + "_PREDICTOR");
        PREDICTOR = (Predictor) Misc.createClassObject(settings.getSetting(predictorName+"_CLASS"), predictorName, settings);

        SERIES_ATTRIBUTE = settings.getSettingAsInt(name + "_SERIES_ATTRIBUTE");
        SEASON_ATTRIBUTE = settings.getSettingAsInt(name + "_SEASON_ATTRIBUTE");
        EPISODE_ATTRIBUTE = settings.getSettingAsInt(name + "_EPISODE_ATTRIBUTE");
        
        RELEVANT_TIME = settings.getSettingAsLong(name + "_RELEVANT_TIME");
        
        ADD_NONSERIES = settings.getSettingAsBoolean(name + "_ADD_NONSERIES");
    }

    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) 
    {
    	if (predictions.size() == 0)
    		return;
    	
        final TLongObjectMap<LastSeriesInfo> userSeries = seriesData.get(predictions.get(0).getConsumerIndex());        
        
        // create candidates
        TLongObjectMap<LastSeriesInfo> candidates = new TLongObjectHashMap<LastSeriesInfo> ();
        List<ProductRating> nonSeries = new ArrayList<ProductRating> ();
        for (ProductRating pr : predictions)
        {
            final ProductData pd = pr.getProductData();
            final Long series = pd.getLongAttrValue(SERIES_ATTRIBUTE);
            final Long season = pd.getLongAttrValue(SEASON_ATTRIBUTE);
            final Long episode = pd.getLongAttrValue(EPISODE_ATTRIBUTE);
            if (series == null)
            {
                nonSeries.add(pr);
                continue;
            }
            
            
            int iseason, iepisode;
            if (season == null)
                iseason = Integer.MAX_VALUE;
            else
                iseason = season.intValue();
            if (episode == null)
                iepisode = Integer.MAX_VALUE;
            else
                iepisode = episode.intValue();
            LastSeriesInfo user = null;
            if (userSeries != null)
                user = userSeries.get(series);
            
            if (user!=null && (iseason < user.season || iseason == user.season && iepisode <= user.episode))
                continue;
            
            // update candidates
            if (!candidates.containsKey(series))
            {
                candidates.put(series, new LastSeriesInfo(series, iseason, iepisode, -1, pr));
            }
            else if (userSeries == null || !userSeries.containsKey(series))
            {
                final LastSeriesInfo old = candidates.get(series);
                if (old.season > iseason || old.season == iseason && old.episode > iepisode)
                {
                    old.update(series, iseason, iepisode, -1, pr);
                }
            }
            else
            {
                final LastSeriesInfo old = candidates.get(series);
                // nearest season
                if ((old.season < user.season || old.season == user.season && old.episode < user.episode) && 
                        (iseason > old.season || iseason == old.season && iepisode > old.episode))
                {
                    old.update(series, iseason, iepisode, -1, pr);
                }
                else if ((old.season > user.season || old.season == user.season && old.episode > user.episode) && 
                        (iseason < old.season || iseason == old.season && iepisode < old.episode) &&
                        (iseason > user.season || iseason == user.season && iepisode > user.episode))
                {
                    old.update(series, iseason, iepisode, -1, pr);
                }
            }
        }
        
        // now prepare new predictions
        predictions.clear();
        final long relMinTime = TagsManager.getCurrentTimeSeconds(tags) - RELEVANT_TIME;
        for (TLongObjectIterator<LastSeriesInfo> it = candidates.iterator(); it.hasNext();)
        {
            it.advance();
            final long key = it.key();
            final LastSeriesInfo val = it.value();
            final ProductRating pr = val.pr;
            
            predictions.add(pr);
            if (userSeries != null && userSeries.containsKey(key))
            {
            	final long watched = userSeries.get(key).time_watched;
            	final long season = userSeries.get(key).season;
            	final long episode = userSeries.get(key).episode;
            	

            	if (watched > relMinTime && season == val.season && episode == val.episode-1)
            	{
        	        pr.setPrediction(5, 1f, this.ID, "Currently watched series;");
                    pr.addPrettyExplanation("Currently watched series", 100.0f);
                    pr.setTag("block_decrease", "");
            	}
            	else if (watched > relMinTime && season == val.season)
            	{
        	        pr.setPrediction(4, 0.8f, this.ID, "Currently watched series;");
                    pr.addPrettyExplanation("Currently watched series", 100.0f);
                    pr.setTag("block_decrease", "");
            	}
            	else if (season < val.season && val.episode == 1)
            	{
        	        pr.setPrediction(3, 0.5f, this.ID, "New season of previously watched series;");
                    pr.addPrettyExplanation("New season of previously watched series", 100.0f);
                    pr.setTag("block_decrease", "");
            	}
            	else if (watched > relMinTime && season < val.season)
            	{
                    addProductRating(pr, 4.0, "Currently watched series;");
                    pr.addPrettyExplanation("Currently watched series", 100.0f);
                    pr.setTag("block_decrease", "");
            	}
            	else if (watched > relMinTime)
            	{
                    addProductRating(pr, 1.0, "Currently watched series;");
                    pr.addPrettyExplanation("Currently watched series", 100.0f);
                    pr.setTag("block_decrease", "");
            	}
            	else
            	{
                    addProductRating(pr, 0.0, "");
            	}
            }
            else if (userSeries != null && userSeries.size() > 3 && val.episode == 1 && val.season == 1)
            {
        	    pr.setPrediction(1, 0.2f, this.ID, "New series;");
            }
            else
            {
                addProductRating(pr, 0.0, "");
            }
        }
        
        if (ADD_NONSERIES)
        {
            for (ProductRating ns : nonSeries)
                predictions.add(ns);
        }
        PREDICTOR.getPredictionsTime(predictions, tags);
    }

    /**
     * Increase counter for each bought product in each possible context. 
     */
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData updateData) {
        logger.info("Incremental update of SeriesFilter started.");
        // update data
        if (this.data == null) // initialization is needed
        {
            this.data = updateData.dataStore;
            seriesData = new TIntObjectHashMap<TLongObjectMap<LastSeriesInfo>> ();
        }
        
        final int eventIndex = data.getEventsDescriptor(EVENTSNAME).index;

        
        TIntObjectMap<TLongObjectMap<LastSeriesInfo>> newSeriesData = new TIntObjectHashMap<TLongObjectMap<LastSeriesInfo>> ();
        final List<ConsumerData> tmp_consumers = updateData.newData;
        final TLongIntHashMap tmp_ids = updateData.newConsumerIDs;
        
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
            final long [] dates = meta.getLongValuesArray(0);
            
            TLongObjectMap<LastSeriesInfo> seriesSet;
            if (!newSeriesData.containsKey(consIndex))
            {
                seriesSet = new TLongObjectHashMap<LastSeriesInfo> ();
                newSeriesData.put(consIndex, seriesSet);
                // add all from original series set
                final TLongObjectMap<LastSeriesInfo> originalSeriesSet = seriesData.get(consIndex);
                if (originalSeriesSet != null)
                {
                    for (TLongObjectIterator<LastSeriesInfo> it = originalSeriesSet.iterator(); it.hasNext();)
                    {
                        it.advance();
                        seriesSet.put(it.key(), it.value().clone());
                    }
                }
            }
            else
            {
                seriesSet = newSeriesData.get(consIndex);
            }

            for (int i = 0; i < items.length; i++)
            {
                final int item = items[i];
                // get series, season, episode, time
                final ProductData pd = data.getProductByIndex(item);
                final Long series = pd.getLongAttrValue(SERIES_ATTRIBUTE);
                final Long season = pd.getLongAttrValue(SEASON_ATTRIBUTE);
                final Long episode = pd.getLongAttrValue(EPISODE_ATTRIBUTE);
                
                if (series == null)
                {
                    continue;
                }
                int iseason, iepisode;
                if (season == null)
                	iseason = 0;
                else
                	iseason = season.intValue();
                if (episode == null)
                	iepisode = 0;
                else
                	iepisode = episode.intValue();
                final long time = dates[i];
                if (!seriesSet.containsKey(series))
                {
                    seriesSet.put(series, new LastSeriesInfo(series, iseason, iepisode, time));
                }
                else
                {
                    seriesSet.get(series).update(series, iseason, iepisode, time);
                }
            }
            
        }
        
        logger.info("Incremental update of SeriesFilter ended.");
        Commitable c = PREDICTOR.updateModelIncremental(updateData);
        return new UpdateDelta(newSeriesData, c);
    }

    /**
     * Remove nonrelevant products / add new products. 
     */
    @Override
    public Commitable updateProducts(DataStore.UpdateProductsDelta delta) {
        Commitable c = PREDICTOR.updateProducts(delta);
        return new UpdateDelta(null, c);
    }
    
    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int predictionId,
            Map<String, String> tags) {
    	// for each series, select its last part (index should not be in productIndices)
        List<ProductRating> nonSeries = new ArrayList<ProductRating> ();
        TLongObjectMap<LastSeriesInfo> candidates = new TLongObjectHashMap<LastSeriesInfo> ();
        for (ProductRating pr : predictions)
        {
            final ProductData pd = pr.getProductData();
            final Long series = pd.getLongAttrValue(SERIES_ATTRIBUTE);
            final Long season = pd.getLongAttrValue(SEASON_ATTRIBUTE);
            final Long episode = pd.getLongAttrValue(EPISODE_ATTRIBUTE);
            if (series == null)
            {
                nonSeries.add(pr);
                continue;
            }
    	
            int iseason, iepisode;
            if (season == null)
                iseason = Integer.MAX_VALUE;
            else
                iseason = season.intValue();
            if (episode == null)
                iepisode = Integer.MAX_VALUE;
            else
                iepisode = episode.intValue();
    	
            // update candidates
            if (!candidates.containsKey(series))
            {
                candidates.put(series, new LastSeriesInfo(series, iseason, iepisode, -1, pr));
            }
            else 
            {
                final LastSeriesInfo old = candidates.get(series);
                if (old.season < iseason || old.season == iseason && old.episode < iepisode)
                {
                    old.update(series, iseason, iepisode, -1, pr);
                }
            }
        }

        // now prepare new predictions
        predictions.clear();
        for (TLongObjectIterator<LastSeriesInfo> it = candidates.iterator(); it.hasNext();)
        {
            it.advance();
            final LastSeriesInfo val = it.value();
            final ProductRating pr = val.pr;
            
            predictions.add(pr);
        }
        
        if (ADD_NONSERIES)
        {
            for (ProductRating ns : nonSeries)
                predictions.add(ns);
        }
    	
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
		// serialize number of keys
		out.writeInt(seriesData.size());
		for (TIntObjectIterator<TLongObjectMap<LastSeriesInfo>> it = seriesData.iterator(); it.hasNext(); )
		{
			it.advance();
			final int key = it.key();
			// write key
			out.writeInt(key);
			// serialize number of internal keys
			final TLongObjectMap<LastSeriesInfo> value = (TLongObjectMap<LastSeriesInfo>) it.value();
			out.writeInt(value.size());
			for (TLongObjectIterator<LastSeriesInfo> it2 = value.iterator(); it2.hasNext(); )
			{
				it2.advance();
				final long key2 = it2.key();
				final LastSeriesInfo sinfo = (LastSeriesInfo) it2.value();
				// write key2
				out.writeLong(key2);
				// write series info
				sinfo.serialize(out);
			}
		}
		PREDICTOR.serialize(out);
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        logger.info("Update model from file of SeriesFilter started.");
        if (this.data == null) // initialization is needed
        {
            this.data = data;
            seriesData = new TIntObjectHashMap<TLongObjectMap<LastSeriesInfo>> ();
        }        
		TIntObjectMap<TLongObjectMap<LastSeriesInfo>> newSeriesData = new TIntObjectHashMap<TLongObjectMap<LastSeriesInfo>> ();
		final int length = in.readInt();
		for (int i = 0; i < length; i++)
		{
			// deserialize key and add an object to newSeriesData
			final int key = in.readInt();
			TLongObjectMap<LastSeriesInfo> value = new TLongObjectHashMap<LastSeriesInfo> ();
			newSeriesData.put(key, value);
			
			// deserialize number of elements for this key
			final int length2 = in.readInt();
			for (int i2 = 0; i2 < length2; i2++)
			{
				// deserialize key2
				final long key2 = in.readLong();
				// deserialize series info
				final LastSeriesInfo sinfo = LastSeriesInfo.deserialize(in);
				// add it to the map
				value.put(key2, sinfo);
			}
		}
        
		Commitable c = PREDICTOR.updateModelFromFile(in, data);
        logger.info("Update model from file of SeriesFilter ended.");
        return new UpdateDelta(newSeriesData, c);
	}    

    private class UpdateDelta implements Commitable {
        TIntObjectMap<TLongObjectMap<LastSeriesInfo>> newSeriesData;
        Commitable predComm;
    
        UpdateDelta(final TIntObjectMap<TLongObjectMap<LastSeriesInfo>> newSeriesData, Commitable predComm) {
            this.newSeriesData = newSeriesData;
            this.predComm = predComm;
        }

        @Override
        public void commit() {
        	if (newSeriesData != null)
	            for (TIntObjectIterator<TLongObjectMap<LastSeriesInfo>> it = newSeriesData.iterator(); it.hasNext();)
	            {
	                it.advance();
	                seriesData.put(it.key(), it.value());
	            }
            predComm.commit();
        }        
    }
}
