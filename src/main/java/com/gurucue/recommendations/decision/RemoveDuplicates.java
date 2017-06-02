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
import com.gurucue.recommendations.recommender.dto.ProductData;
import gnu.trove.list.array.TFloatArrayList;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;

/**
 * This decision module filters products by two attributes so that not more than N 
 * recommended products have the same first attribute value and no more than 1 product
 * have the same second attribute value. 
 * 
 * At the moment it works only with multi-valued attributes.
 */

public class RemoveDuplicates extends DecisionModule {
	private final static Logger logger = LogManager.getLogger(RemoveDuplicates.class.getName());
	
    final private float MIN_PREDICTION;
    final private int MAIN_ATTR, MAIN_ATTR_COUNT, SECOND_ATTR, SECOND_ATTR_COUNT, TITLE_ATTR;
    final private boolean PREF_EXPLAIN;
    
    public RemoveDuplicates(String name, Settings settings) {
        super(name, settings);
        
        MIN_PREDICTION = settings.getSettingAsFloat(name + "_MIN_PREDICTION");
        Integer main_attr = settings.getSettingAsInt(name + "_MAIN_ATTR");
        if (main_attr != null)
        {
        	MAIN_ATTR = main_attr;
        	MAIN_ATTR_COUNT = settings.getSettingAsInt(name + "_MAIN_ATTR_COUNT");
        }
        else
        {
        	MAIN_ATTR = -1;
        	MAIN_ATTR_COUNT = -1;
        }
        Integer second_attr = settings.getSettingAsInt(name + "_SECOND_ATTR");
        if (second_attr != null)
        {
        	SECOND_ATTR = second_attr;
        }
        else
        	SECOND_ATTR = -1;
        Integer sac = settings.getSettingAsInt(name + "_SECOND_ATTR_COUNT");;
        if (sac == null)
        	SECOND_ATTR_COUNT = 3;
        else
        	SECOND_ATTR_COUNT = sac;
        TITLE_ATTR = settings.getSettingAsInt(name + "_TITLE_ATTR");
        PREF_EXPLAIN = settings.getSetting(name + "_PREF_EXPLAIN").equalsIgnoreCase("yes")?true:false;
    }

    public ProductRating[] selectBestCandidates(List<ProductRating> candidates, int maxCandidates, boolean randomizeResults, Map<String,String> tags)
    {
        if (candidates == null || candidates.size() == 0) {
            return new ProductRating[0];
        }
        
        // get tags
        ProductAdder adder;
        if (tags.containsKey("MasterRecommender_similar"))
        	adder = new ProductAdder(tags, TITLE_ATTR, -1, PREF_EXPLAIN);
        else
        	adder = new ProductAdder(tags, TITLE_ATTR, 7, PREF_EXPLAIN);
        
        // take only acceptable candidates
        ArrayList<ProductRating> acc = new ArrayList<ProductRating>();
        for (ProductRating pr: candidates)
        {
            if (recommendProduct(pr))
                acc.add(pr);
        }
        
        // define weights (if explanations are preferred, count all explanations and multiply weight by (count + 1)
        TFloatArrayList weights = new TFloatArrayList();  
        for (ProductRating pr: acc)
        {
        	weights.add((float) (pr.getPrediction() - MIN_PREDICTION));
        }

        ProductRating[] recommended = selectSubset(acc, weights, weights.size(), randomizeResults);

        for (ProductRating pr : recommended)
        {
        	adder.addStrict(pr);
            if (adder.size() >= maxCandidates)
                break;
        }

        if (adder.size() < maxCandidates)
        {
            for (ProductRating pr : recommended)
            {
                adder.add(pr);
                if (adder.size() >= maxCandidates)
                    break;
            }
        }        
        
        if (adder.size() < maxCandidates)
        {
            for (ProductRating pr : recommended)
            {
                adder.addForced(pr);
                if (adder.size() >= maxCandidates)
                    break;
            }
        }
        
        if (tags.containsKey("force_recommend"))
        {
        	String ids = tags.get("force_recommend");
        	for (String id : ids.split(","))
        	{
        		if (id.equalsIgnoreCase(""))
        			continue;
        		long lid = Long.parseLong(id);
        		boolean found = false;
        		for (ProductRating pr : candidates)
        		{
        			if (pr.getProductId() == lid)
        			{
        				found = true;
        				adder.addForced(pr);
        			}
        		}
        		if (!found)
        		{   
                    Long val = Long.valueOf(id);
                    ProductData pr = data.getProductById(val);
                    if (pr == null)
                    {
            			logger.warn("Product with id " + id + " not found within candidates. This product is not even in recommender products.");
                    }
                    else
            			logger.warn("Product with id " + id + ", title = " + pr.getStrAttrValue(0, 0) + " not found within candidates.");
        		}
        	}
        }
        return adder.getRecommended();
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
        if (this.data == null)
            this.data = data.dataStore;

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
        if (this.data == null)
            this.data = data;
        return EmptyCommit.INSTANCE;
	}   
}

