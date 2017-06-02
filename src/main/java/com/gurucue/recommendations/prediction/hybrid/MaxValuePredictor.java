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
package com.gurucue.recommendations.prediction.hybrid;

import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.EmptyCommit;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import gnu.trove.set.TIntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;


/**
 * This predictor simply computes the maximum of all predictors and use it as the final prediction
 */

public class MaxValuePredictor extends Predictor implements Cloneable {

    private final Logger logger;
    
    // predictors that will be used together 
    protected int [] PREDICTOR_IDS;
    
    
    public MaxValuePredictor(String name, Settings settings) {
        super(name, settings);
        logger = LogManager.getLogger(MaxValuePredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        
        // create predictors
        PREDICTOR_IDS = settings.getAsIntArray(name + "_PREDICTOR_IDS");
    }
    
    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) {
        // set predictions
        for (ProductRating p : predictions)
        {
            double maxVal = 0;
            for (int i = 0; i < PREDICTOR_IDS.length; i++)
            {
                final int predid = PREDICTOR_IDS[i];
                final double val = p.getPrediction(predid);
                if (val > maxVal)
                    maxVal = val;
            }
            addProductRating(p, maxVal, "");
        }
    }    

    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int id_shift, Map<String,String> tags) {
        logger.error("getSimilarProducts not implemented for MaxValuePredictor");
    }
    
    @Override
    public Commitable updateConsumer(DataStore.UpdateData updateData) {
        logger.error("updateConsumer not implemented for MaxValuePredictor");
        return null;
    }

    @Override
    public void updateModel(DataStore data) {
        logger.error("updateModel not implemented for MaxValuePredictor");
    }

    @Override
    public ProductPair getBestPair(int consumerIndex) {
        logger.error("Get best pair not implemented for MaxValuePredictor");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumerIndex) {
        logger.error("Needs profiling not implemented for MaxValuePredictor");
        return false;
    }

    @Override
    public void updateModelQuick(DataStore data) {
        logger.error("UpdateModelQuick not implemented for MaxValuePredictor");
    }
 
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData newData) {
        return EmptyCommit.INSTANCE;
    }

    @Override
    public Commitable updateProducts(UpdateProductsDelta delta) {
        return EmptyCommit.INSTANCE;
    }

	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        return EmptyCommit.INSTANCE;
	}

}
