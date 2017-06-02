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
import java.util.List;
import java.util.Map;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;

/**
 * This decision module returns best N products according to computed predictions.
 */

public class GetBestNProducts extends DecisionModule {
    final private float MIN_PREDICTION;
    
    public GetBestNProducts(String name, Settings settings) {
        super(name, settings);
        MIN_PREDICTION = settings.getSettingAsFloat(name + "_MIN_PREDICTION");
    }

    public ProductRating[] selectBestCandidates(List<ProductRating> candidates, int maxCandidates, boolean randomizeResults, Map<String,String> tags)
    {
        if (candidates == null || candidates.size() == 0) {
            return null;
        }
        
        // take only acceptable candidates
        ArrayList<ProductRating> acc = new ArrayList<ProductRating>();
        for (ProductRating pr: candidates)
        {
            if (recommendProduct(pr))
                acc.add(pr);
        }
        
        // define weights
        TFloatArrayList weights = new TFloatArrayList();  
        for (ProductRating pr: acc)
        {
            weights.add((float) pr.getPrediction() - MIN_PREDICTION);
        }

        return selectSubset(acc, weights, maxCandidates, randomizeResults);
    }
    
    /**
     * Defines acceptable or recommendable products.
     * @param cand candidate product (given as a ProductRating)
     * @return
     */
    private boolean recommendProduct(ProductRating cand)
    {
        if (cand.getPrediction() >= MIN_PREDICTION)
            return true;
        return false;
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

