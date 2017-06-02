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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;

/**
 * This decision module combines all other three decision modules. It takes some results from each of them.
 */
public class HybridDecisionModule extends DecisionModule {
    private DecisionModule best, largestOffset, popular;
    
    public HybridDecisionModule(String name, Settings settings) {
        best  = new GetBestRatingProducts(name, settings);
        largestOffset  = new GetLargestOffsetProducts(name, settings);
        popular  = new GetPopularProducts(name, settings);
    }
    
    @Override
    public ProductRating[] selectBestCandidates(List<ProductRating> candidates, int maxCandidates, boolean randomizeResults, Map<String,String> tags)
    {
        ProductRating[] bestProducts = best.selectBestCandidates(candidates, maxCandidates, randomizeResults, tags);
        ProductRating[] offsetProducts = largestOffset.selectBestCandidates(candidates, maxCandidates, randomizeResults, tags);
        ProductRating[] popularProducts = popular.selectBestCandidates(candidates, maxCandidates, randomizeResults, tags);
        
        Set<ProductRating> products = new HashSet<ProductRating>();

        ProductRating[][] hyperCandidates = {bestProducts, offsetProducts, popularProducts};
        int counter = 0;
        outer:
        for (int i = 0; i < maxCandidates; i++) {
            for (int j = 0; j < 3; j++) {
                if (hyperCandidates[j].length > i) {
                    products.add(hyperCandidates[j][i]);
                    counter++;
                    // we gathered enough products
                    if (counter >= maxCandidates)
                        break outer;
                }
            }
        }
        // change set to an array
        ProductRating[] coolProducts = products.toArray(new ProductRating[products.size()]);

        // sort by estimated rating
        Arrays.sort(coolProducts);
        return coolProducts;
    }
    
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData newData) {
        ArrayList<Commitable> cs = new ArrayList<Commitable>();
        cs.add(best.updateModelIncremental(newData));
        cs.add(largestOffset.updateModelIncremental(newData));
        cs.add(popular.updateModelIncremental(newData));
        return new CommitAll(newData.dataStore, cs);
    }

    @Override
    public Commitable updateProducts(UpdateProductsDelta delta) {
        ArrayList<Commitable> cs = new ArrayList<Commitable>();
        cs.add(best.updateProducts(delta));
        cs.add(largestOffset.updateProducts(delta));
        cs.add(popular.updateProducts(delta));
        return new CommitAll(delta.dataStore, cs);
    }
    
	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        ArrayList<Commitable> cs = new ArrayList<Commitable>();
        cs.add(best.updateModelFromFile(in, data));
        cs.add(largestOffset.updateModelFromFile(in, data));
        cs.add(popular.updateModelFromFile(in, data));
        return new CommitAll(data, cs);
	}
    

    class CommitAll implements Commitable
    {
        private final DataStore tmp_data;
        private final ArrayList<Commitable> all;
        
        CommitAll(final DataStore tmp_data, final ArrayList<Commitable> all)
        {
            this.tmp_data = data;
            this.all = all;
        }
        
        @Override
        public void commit() {
            data = tmp_data;
            for (Commitable c : all)
                c.commit();
        }
    }

}



