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
import com.gurucue.recommendations.recommender.dto.ConsumerRatingsData;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;

/**
 * This decision module returns best N movies according to predicted ratings. 
 */

public class GetBestRatingProducts extends DecisionModule {
    private float acceptableRating;
//    private RatingsData ratings;
    private int ratingsEventIndex;
    
    public GetBestRatingProducts(String name, Settings settings) {
        super(name, settings);
        acceptableRating = settings.getSettingAsFloat(name + "_ACCEPTABLE_RATINGS");
    }
    

    public ProductRating[] selectBestCandidates(List<ProductRating> candidates, int maxCandidates, boolean randomizeResults, Map<String,String> tags)
    {
        if (candidates == null || candidates.size() == 0) {
            return null;
        }
        
        // get user's average
        final ConsumerRatingsData crd = (ConsumerRatingsData) candidates.get(0).getConsumerData().events[ratingsEventIndex];
        final float average = crd.sum / (crd.getN() + 0.1f);
        
        // take only acceptable candidates
        ArrayList<ProductRating> acc = new ArrayList<ProductRating>();
        for (ProductRating pr: candidates)
        {
            if (recommendProduct(pr, average))
                acc.add(pr);
        }
        
        // define weights
        TFloatArrayList weights = new TFloatArrayList();  
        final float boundary = Math.min(average, acceptableRating);
        for (ProductRating pr: acc)
            weights.add((float) pr.getPrediction() - boundary);

        return selectSubset(acc, weights, maxCandidates, randomizeResults);
    }
    
    /**
     * Defines acceptable or recommendable products.
     * @param cand candidate product (given as a ProductRating)
     * @param userAverage (this user's average rating)
     * @return
     */
    private boolean recommendProduct(ProductRating cand, float userAverage)
    {
        if (cand.getPrediction() >= acceptableRating || cand.getPrediction() >= userAverage)
            return true;
        return false;
    }

    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData newData) {
        if (null == this.data)
            ratingsEventIndex = newData.dataStore.getEventsDescriptor(settings.getSetting(name + "_BUYS")).index;
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
        if (null == this.data)
        	ratingsEventIndex = data.getEventsDescriptor(settings.getSetting(name + "_BUYS")).index;
        return EmptyCommit.INSTANCE;
	}    
}

