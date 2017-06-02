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

import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.EmptyCommit;
import com.gurucue.recommendations.recommender.Settings;
import gnu.trove.list.array.TFloatArrayList;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;

/**
 * This decision module combines predictions of several predictors.
 * It selects a certain percentage of best items from one predictor and then from another, etc.
 */

public class CombinePredictorsDecisionModule extends DecisionModule {
    final int [] PREDICTOR_IDS;
    final int [] PREDICTOR_WEIGHTS;
    final int TITLE_ATTRIBUTE;
    
    public CombinePredictorsDecisionModule(String name, Settings settings) {
        super(name, settings);
        
        PREDICTOR_IDS = settings.getAsIntArray(name + "_PREDICTOR_IDS");
        PREDICTOR_WEIGHTS = settings.getAsIntArray(name + "_PREDICTOR_WEIGHTS");
        TITLE_ATTRIBUTE = settings.getSettingAsInt(name + "_TITLE_ATTRIBUTE");

    }

    public ProductRating[] selectBestCandidates(List<ProductRating> candidates, int maxCandidates, boolean randomizeResults, Map<String,String> tags)
    {
        if (candidates == null || candidates.size() == 0) {
            return null;
        }
        
        // all candidates are acceptable here!
        
        // define weights for each predictor
        List<List<ProductRating>> sortedResults = new ArrayList<List<ProductRating>> ();
        for (int i=0; i<PREDICTOR_IDS.length; i++)
        {
            TFloatArrayList weights = new TFloatArrayList();
            List<ProductRating> newCandidates = new ArrayList<ProductRating> ();
            //long start = System.currentTimeMillis();
            for (ProductRating pr: candidates)
            {
                final float weight = (float) pr.getPrediction(PREDICTOR_IDS[i]);
                if (weight > 0)
                {
                    weights.add((float) pr.getPrediction(PREDICTOR_IDS[i]));
                    newCandidates.add(pr);
                }
            }
            sortedResults.add(Arrays.asList((selectSubset(newCandidates, weights, newCandidates.size(), randomizeResults))));
        }
        
        // indices of different results (to keep track what is in results already)
        int [] pred_index = new int [PREDICTOR_IDS.length];
        for (int i=0; i<PREDICTOR_IDS.length; i++)
            pred_index[i] = 0;
        
        // combine predictions
        ProductAdder adder = new ProductAdder(tags, TITLE_ATTRIBUTE, 7);
        
        outer:
        while (true)
        {
            for (int i=0; i<PREDICTOR_IDS.length; i++)
            {
                for (int j=0; j<PREDICTOR_WEIGHTS[i]; j++)
                {
                    final List<ProductRating> sortedResult = sortedResults.get(i);
                    // if all products were added already, go on. 
                    if (sortedResult.size() <= pred_index[i]) continue;
                    final ProductRating selected = sortedResult.get(pred_index[i]);
                    pred_index[i] ++;
                    if (selected.getPrediction(PREDICTOR_IDS[i]) <= 0.0) continue;                    
                    selected.setPrediction(selected.getPrediction(PREDICTOR_IDS[i]), this.ID, "");
                    boolean added = adder.add(selected);
                    if (!added)
                    {
                    	j--;
                    }
                    
                    if (adder.size() >= maxCandidates)
                        break outer;
                }
            }
            // if all pred_indices equal sorteResults.size() then stop
            boolean stop = true;
            for (int i=0; i<PREDICTOR_IDS.length; i++)
                if (sortedResults.get(i).size() > pred_index[i]) 
                    stop = false;                    
            if (stop)
                break;
        }
        if (adder.size() < maxCandidates)
        {
            for (ProductRating pr : sortedResults.get(0))
            {
                adder.addForced(pr);
                if (adder.size() >= maxCandidates)
                    break;
            }
        }        
        return adder.getRecommended();
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

