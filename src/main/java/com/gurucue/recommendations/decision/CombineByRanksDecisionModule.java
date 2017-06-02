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
package com.gurucue.recommendations.decision;

import com.gurucue.recommendations.misc.Statistics;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.EmptyCommit;
import com.gurucue.recommendations.recommender.Settings;
import gnu.trove.list.array.TFloatArrayList;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;

/**
 * This decision module combines predictions of several predictors. It selects items that have the highest weighted average ranks of all predictors. 
 */

public class CombineByRanksDecisionModule extends DecisionModule {
    final int [] PREDICTOR_IDS;
    final float [] PREDICTOR_WEIGHTS;
    
    public CombineByRanksDecisionModule(String name, Settings settings) {
        super(name, settings);
        
        PREDICTOR_IDS = settings.getAsIntArray(name + "_PREDICTOR_IDS");
        PREDICTOR_WEIGHTS = settings.getAsFloatArray(name + "_PREDICTOR_WEIGHTS");
    }

    public ProductRating[] selectBestCandidates(List<ProductRating> candidates, int maxCandidates, boolean randomizeResults, Map<String,String> tags)
    {
        if (candidates == null || candidates.size() == 0) {
            return null;
        }
        
        // initialize rankSum
        TFloatArrayList rankSum = new TFloatArrayList();
        for (ProductRating pr: candidates)
            rankSum.add(0.0f);
        
        // create ranks of all predicted items for each predictor and add them to rankSum
        for (int i=0; i<PREDICTOR_IDS.length; i++)
        {
            TFloatArrayList predictions = new TFloatArrayList();  
            for (ProductRating pr: candidates)
            {
                predictions.add((float) pr.getPrediction(PREDICTOR_IDS[i]));
            }
            TFloatArrayList ranks = Statistics.rankData(predictions);
            for (int j=0; j<ranks.size(); j++)
                rankSum.set(j, rankSum.get(j) + PREDICTOR_WEIGHTS[i] * ranks.get(j));
        }
        
        
        // weights = rankSum
        return selectSubset(candidates, rankSum, maxCandidates, randomizeResults);
    }

    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData data) {
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

