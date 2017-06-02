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
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.TIntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;


/**
 * This predictor computes ranks of all predictors; new prediction is sum all ranks
 */

public class HybridSumPredictor extends Predictor implements Cloneable {

    private final Logger logger;
    
    // predictors that will be used together 
    protected int [] PREDICTOR_IDS;
    // weigths that each rank gets
    protected float [] PREDICTOR_WEIGHTS;
    
    
    public HybridSumPredictor(String name, Settings settings) {
        super(name, settings);
        logger = LogManager.getLogger(HybridSumPredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        
        // create predictors
        PREDICTOR_IDS = settings.getAsIntArray(name + "_PREDICTOR_IDS");
        PREDICTOR_WEIGHTS = settings.getAsFloatArray(name + "_PREDICTOR_WEIGHTS");
    }
    
    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) {
        final int nPredictions = predictions.size();
        TLongFloatMap ranks = new TLongFloatHashMap();
        for (int i = 0; i < PREDICTOR_IDS.length; i++)
        {
            final int predid = PREDICTOR_IDS[i];
            final float predweight = PREDICTOR_WEIGHTS[i];
            Collections.sort(predictions, new ProductRating.ProductDataComparator(predid));
            int rank = nPredictions + 1;
            for (ProductRating p : predictions)
            {
                ranks.put(p.getProductId(), ranks.get(p.getProductId()) + rank * predweight);
                rank--;
            }
        }
        // set predictions
        for (ProductRating p : predictions)
        {
            addProductRating(p, ranks.get(p.getProductId()), "");
        }
    }    

    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int id_shift, Map<String,String> tags) {
        logger.error("getSimilarProducts not implemented for CombineByRanks");
    }
    
    @Override
    public Commitable updateConsumer(DataStore.UpdateData updateData) {
        logger.error("updateConsumer not implemented for CombineByRanks");
        return null;
    }

    @Override
    public void updateModel(DataStore data) {
        logger.error("updateModel not implemented for CombineByRanks");
    }

    @Override
    public ProductPair getBestPair(int consumerIndex) {
        logger.error("Get best pair not implemented for CombineByRanks");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumerIndex) {
        logger.error("Needs profiling not implemented for CombineByRanks");
        return false;
    }

    @Override
    public void updateModelQuick(DataStore data) {
        logger.error("UpdateModelQuick not implemented for CombineByRanks");
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
