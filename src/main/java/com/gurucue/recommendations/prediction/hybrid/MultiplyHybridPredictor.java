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
 * This class implements a hybrid predictor where predictions of all sub-predictors are simply multiplied. 
 */
public class MultiplyHybridPredictor extends Predictor {
    private final Logger logger;
    
    // for average probability estimation
    final int [] PREDICTOR_IDS;
    final float [] PREDICTION_MULTIPLIERS;
    final float [] FIND_SIMILAR_MULTIPLIERS;
    
    public MultiplyHybridPredictor(String name, Settings settings) throws CloneNotSupportedException {
        super(name, settings);

        logger = LogManager.getLogger(MultiplyHybridPredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");

        PREDICTOR_IDS = settings.getAsIntArray(name + "_PREDICTOR_IDS");
        PREDICTION_MULTIPLIERS = settings.getAsFloatArray(name + "_PREDICTION_MULTIPLIERS");
        FIND_SIMILAR_MULTIPLIERS = settings.getAsFloatArray(name + "_FIND_SIMILAR_MULTIPLIERS");
    }
    
    @Override
    public void updateModel(DataStore data) {
    }

    @Override
    public Commitable updateConsumer(final DataStore.UpdateData updateData) {
        return EmptyCommit.INSTANCE;
    }
    
    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) {
        // user probabilities
        for (ProductRating pr : predictions)
        {
            float newPrediction = 1;
            for (int i=0; i<PREDICTOR_IDS.length; i++)
            {
                if (PREDICTION_MULTIPLIERS[i] < 0 && pr.getPrediction(PREDICTOR_IDS[i]) < 1e-6)
                    newPrediction *= Math.pow(1e-6, PREDICTION_MULTIPLIERS[i]);
                else
                    newPrediction *= Math.pow(pr.getPrediction(PREDICTOR_IDS[i]), PREDICTION_MULTIPLIERS[i]);
            }
            // add prediction to ProductRating
            addProductRating(pr, newPrediction, "");
        }
    }

    @Override
    public void getSimilarProducts(TIntSet productIndices, List<ProductRating> items, int id_shift, Map<String,String> tags) {
        // user probabilities
        for (ProductRating pr : items)
        {
            float newPrediction = 1;
            for (int i=0; i<PREDICTOR_IDS.length; i++)
            {
                newPrediction *= Math.pow(pr.getPrediction(id_shift + PREDICTOR_IDS[i]), FIND_SIMILAR_MULTIPLIERS[i]);
            }
            // add prediction to ProductRating
            addProductRating(pr, newPrediction, "", id_shift);
        }
    }

    @Override
    public ProductPair getBestPair(int consumerIndex) {
        return null;
    }

    @Override
    public boolean needsProfiling(int consumerIndex) {
        return false;
    }

    @Override
    public void updateModelQuick(DataStore data) {
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException {
        MultiplyHybridPredictor clone = (MultiplyHybridPredictor)super.clone();
        return clone;
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
