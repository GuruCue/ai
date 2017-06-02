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

import com.gurucue.recommendations.misc.Misc;
import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.Attr;
import gnu.trove.set.TIntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;

/**
 * A recommender that selects only relevant series (only one episode).
 * It gives recommendation 1 if the series was watched in the last predefined period of time.  
 */
public class ShortProgrammesFilter extends Predictor {
    private final Logger logger;
    
    final private int START_ATTRIBUTE; // index of attribute containing begin times
    final private int END_ATTRIBUTE; // index of attribute containing begin times
    final private int MIN_LENGTH; // minimal programme length in seconds
    final private int MAX_LENGTH = 60*60*3; // max programme length in seconds
    final private int TITLE_ATTR = 0;
    
    // next predictor (that uses filtered events)
    private final Predictor PREDICTOR;
    
    public ShortProgrammesFilter(String name, Settings settings) {
        super(name, settings);
        
        logger = LogManager.getLogger(ShortProgrammesFilter.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        // create predictor
        String predictorName = settings.getSetting(name + "_PREDICTOR");
        PREDICTOR = (Predictor) Misc.createClassObject(settings.getSetting(predictorName+"_CLASS"), predictorName, settings);

        START_ATTRIBUTE = settings.getSettingAsInt(name + "_START_ATTRIBUTE");
        END_ATTRIBUTE = settings.getSettingAsInt(name + "_END_ATTRIBUTE");
        MIN_LENGTH = settings.getSettingAsInt(name + "_MIN_LENGTH");
    }

    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) 
    {
        // prepare N+1 array list; the first one is to determine genres over the lists
        List<ProductRating> acc = new ArrayList<ProductRating>();
        for (ProductRating pr: predictions)
        {
            
            final Long start = pr.getProductData().getLongAttrValue(START_ATTRIBUTE);
            final Long end = pr.getProductData().getLongAttrValue(END_ATTRIBUTE);

            if (start == null || end == null)
            {
                continue;
            }

            if (end - start < MIN_LENGTH)
                continue;
            
            if (end - start > MAX_LENGTH)
                continue;
            
            final Attr title = pr.getProductData().getAttribute(TITLE_ATTR);
            if (title.toString().contains("rodaj"))
                continue;
            
            acc.add(pr);
        }
        predictions.clear();
        for (ProductRating pr : acc)
            predictions.add(pr);

        PREDICTOR.getPredictionsTime(predictions, tags);
    }

    /**
     * Increase counter for each bought product in each possible context. 
     */
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData updateData) {
        Commitable c = PREDICTOR.updateModelIncremental(updateData);
        return new UpdateCommit(c);        
    }

    /**
     * Remove nonrelevant products / add new products. 
     */
    @Override
    public Commitable updateProducts(DataStore.UpdateProductsDelta delta) {
        Commitable c = PREDICTOR.updateProducts(delta);
        return new UpdateCommit(c);        
    }
    

	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		PREDICTOR.serialize(out);
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        Commitable c = PREDICTOR.updateModelFromFile(in, data);
        return new UpdateCommit(c);        
	}    
    
    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int predictionId,
            Map<String, String> tags) {
        logger.error("getSimilarProducts for ShortProgrammesFilter does not make sense; should not be called!");
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
        logger.error("getBestPair not implemented yet for ShortProgrammesFilter.");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumer_index) {
        logger.error("needsProfiling not implemented yet for ShortProgrammesFilter.");
        return false;
    }

    @Override
    public Commitable updateConsumer(DataStore.UpdateData updateData) {
        logger.error("updateConsumer is deprecated; should not be called!");
        return null;
    }
    
    private class UpdateCommit implements Commitable {
        Commitable predComm;
    
        UpdateCommit(final Commitable predComm) {
            this.predComm = predComm;
        }

        @Override
        public void commit() {
            predComm.commit();
        }        
    }
}
