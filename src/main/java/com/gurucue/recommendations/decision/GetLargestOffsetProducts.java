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
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import com.gurucue.recommendations.prediction.ProductRating;
import gnu.trove.list.array.TFloatArrayList;
import java.util.List;
import java.util.Map;

/**
 * This decision module selects products with
 * highest offset between average movie rating and 
 * estimated rating for this user. Assumption: if the 
 * difference is high, then the user probably likes this 
 * type of movies even though others do not.
 */
public class GetLargestOffsetProducts extends DecisionModule {
    final int AVERAGE_PREDICTOR_ID;
    
    public GetLargestOffsetProducts(String name, Settings settings) {
        super(name, settings);
        AVERAGE_PREDICTOR_ID = settings.getSettingAsInt(name + "_AVERAGE_PREDICTOR_ID");
    }

    @Override
    public ProductRating[] selectBestCandidates(List<ProductRating> candidates, int maxCandidates, boolean randomizeResults, Map<String,String> tags)
    {
        if (candidates == null || candidates.size() == 0) {
            return null;
        }

        // select only candidates that have estimated rating higher than candidate's average
        // take only acceptable candidates
        ArrayList<ProductRating> acc = new ArrayList<ProductRating>();
        for (ProductRating pr: candidates)
        {
            if (pr.getPrediction() >= pr.getPrediction(AVERAGE_PREDICTOR_ID))
                acc.add(pr);
        }

        // define weights
        TFloatArrayList weights = new TFloatArrayList();
        for (ProductRating pr: acc)
        {
            weights.add((float) (pr.getPrediction() - pr.getPrediction(AVERAGE_PREDICTOR_ID)));
        }
        
        return selectSubset(acc, weights, maxCandidates, randomizeResults);
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


