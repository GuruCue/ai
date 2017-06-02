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
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import gnu.trove.map.TLongByteMap;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongByteHashMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TDoubleSet;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TDoubleHashSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;


/**
 * This predictor computes ranks of all predictors; new prediction is sum all ranks
 */

public class CombineByRanks extends Predictor implements Cloneable {
    private static final int DEFAULT_MAX_RANK = 1000;
    private static final int DEFAULT_PERCENTILE = 10;

    private final Logger logger;
    
    // predictors that will be used together 
    protected final int [] PREDICTOR_IDS;
    // Weights that each rank gets.
    protected final float [] PREDICTOR_WEIGHTS;
    protected final float [] PREDICTOR_SIMILAR_WEIGHTS;
    protected final TIntSet DECIDING_PREDICTOR;
    protected final int MAX_RANK;
    protected final boolean REQUIRE_EXPLANATION;
    protected final int PERCENTILE;
    
    
    public CombineByRanks(String name, Settings settings) {
        super(name, settings);
        logger = LogManager.getLogger(CombineByRanks.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        
        // create predictors
        PREDICTOR_IDS = settings.getAsIntArray(name + "_PREDICTOR_IDS");
        int [] dec = settings.getAsIntArray(name + "_DECIDING_PREDICTOR");
        if (dec == null)
        	DECIDING_PREDICTOR = new TIntHashSet();
        else
        	DECIDING_PREDICTOR = new TIntHashSet(dec);
        PREDICTOR_WEIGHTS = settings.getAsFloatArray(name + "_PREDICTOR_WEIGHTS");
        PREDICTOR_SIMILAR_WEIGHTS = settings.getAsFloatArray(name + "_PREDICTOR_SIMILAR_WEIGHTS");
        Integer maxRank = settings.getSettingAsInt(name + "_MAX_RANK");
        if (maxRank == null)
        	MAX_RANK = DEFAULT_MAX_RANK;
        else 
        	MAX_RANK = maxRank;
        Boolean reqexpl = settings.getSettingAsBoolean(name + "_PREDICTOR_REQUIRE_EXPLANATION");
        if (reqexpl == null)
        	REQUIRE_EXPLANATION = false;
        else 
        	REQUIRE_EXPLANATION = reqexpl; 
        Integer percentile = settings.getSettingAsInt(name + "_PERCENTILE");
        if (percentile == null)
        	PERCENTILE = DEFAULT_PERCENTILE;
        else
        	PERCENTILE = percentile;
        
        
    }
    
    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) {
        TLongFloatMap ranks = new TLongFloatHashMap();
        TLongFloatMap weights = new TLongFloatHashMap();
        TLongByteMap above_half = new TLongByteHashMap();
        
        TLongObjectMap<String> explanations = new TLongObjectHashMap<String>();
        
        
        for (int i = 0; i < PREDICTOR_IDS.length; i++)
        {
            final int predid = PREDICTOR_IDS[i];
            final float predweight = PREDICTOR_WEIGHTS[i];
            if (predweight <= 0)
                continue;
            try {
            	Collections.sort(predictions, new ProductRating.ProductDataComparator(predid));
            } catch(IllegalArgumentException e) 
            {
            	logger.error("Sorting failed");
                String user = "(null)";
                if (predictions.size() > 0) {
                    final ProductRating r = predictions.get(0);
                    if (r != null) {
                        final ConsumerData d = r.getConsumerData();
                        if (d != null) user = Long.toString(d.consumerId, 10);
                    }
                }
                logger.error("Error while sorting results for predictor with id: " + predid + " for user: " + user);
            	// contains NaN ir INF?
            	boolean nan = false, inf = false;
            	long probProduct = 0L;
            	String explain = "";
            	for (ProductRating pr : predictions)
            	{
            		if (Double.isInfinite(pr.getPrediction(predid)))
            		{
            			inf = true;
            			probProduct = pr.getProductId();
            			explain = pr.getExplanation(predid);
            		}
            		if (Double.isNaN(pr.getPrediction(predid)))
            		{
            			nan = true;
            			probProduct = pr.getProductId();
            			explain = pr.getExplanation(predid);
            		}
            	}
            	logger.error("Error occured because: nan = " + nan + ", inf = " + inf + ", problematicProduct = " + probProduct + ", explanation = " + explain);
            	e.printStackTrace(); 
            }
            final float half_rank = MAX_RANK/2;
            double threshold = predictions.size() * PERCENTILE / 100f;
            
            double k1 = half_rank / threshold;
            double k2 = half_rank / (predictions.size() - threshold);
            
            float rank = MAX_RANK;
            double previousRat = -1;
            final boolean dec = DECIDING_PREDICTOR.contains(predid);
            int counter = 0;
            
            for (ProductRating p : predictions)
            {
            	counter ++;
            	
            	if (p.getPrediction(predid) <= 0 || rank <= 0.01)
            	{
            		weights.put(p.getProductId(), weights.get(p.getProductId()) + predweight * p.getReliability(predid));
            		continue;
            	}
            	ranks.put(p.getProductId(), ranks.get(p.getProductId()) + rank * predweight * p.getReliability(predid));
            	weights.put(p.getProductId(), weights.get(p.getProductId()) + predweight * p.getReliability(predid));

            	if (!explanations.containsKey(p.getProductId()))
            		explanations.put(p.getProductId(), "");
                explanations.put(p.getProductId(), explanations.get(p.getProductId()) + ";" + predid + ":" + rank + "(" + p.getPrediction(predid) + "," + p.getReliability(predid) + ")");
                final double rat = p.getPrediction(predid);
                if (counter > 1 && previousRat != rat)
                {
                    if (rank > half_rank)
                    {
                    	rank = (float) (MAX_RANK - counter * k1);
                    }
                    else
                    {
                    	rank = (float) (half_rank - Math.max(0, counter - threshold) * k2);
                    }
                	if (rank < 0)
                		rank = 0;
                }
                previousRat = rat;
            }
        }
        // set predictions
        for (ProductRating p : predictions)
        {
        	final String expl = explanations.get(p.getProductId());
        	int add = 0;
        	Set<Float> differentExplanations = new HashSet<Float> (p.getPrettyExplanations().values());
        	if (differentExplanations.size() >= 2)
        		add = MAX_RANK * 2;
        	else if (differentExplanations.size() >= 1)
        		add = MAX_RANK;

        	if (expl == null)
        		addProductRating(p, ranks.get(p.getProductId()) / Math.max(1e-6, weights.get(p.getProductId())) + add, "");
        	else
        	{
        		addProductRating(p, ranks.get(p.getProductId()) / Math.max(1e-6, weights.get(p.getProductId())) + add, explanations.get(p.getProductId()));
        	}
        }
    }    

    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int id_shift, Map<String,String> tags) {
        TLongFloatMap ranks = new TLongFloatHashMap();
        TLongFloatMap weights = new TLongFloatHashMap();
        TLongObjectMap<String> explanations = new TLongObjectHashMap<String>();
        
        for (int i = 0; i < PREDICTOR_IDS.length; i++)
        {
            final int predid = PREDICTOR_IDS[i];
            final float predweight = PREDICTOR_WEIGHTS[i];
            if (predweight <= 0)
                continue;
            try {
            	Collections.sort(predictions, new ProductRating.ProductDataComparator(predid));
            } catch(IllegalArgumentException e) 
            {
                String user = "(null)";
                if (predictions.size() > 0) {
                    final ProductRating r = predictions.get(0);
                    if (r != null) {
                        final ConsumerData d = r.getConsumerData();
                        if (d != null) user = Long.toString(d.consumerId, 10);
                    }
                }
                logger.error("Error while sorting results for predictor with id: " + predid + " for user: " + user);
            	// contains NaN ir INF?
            	boolean nan = false, inf = false;
            	long probProduct = 0L;
            	String explain = "";
            	for (ProductRating pr : predictions)
            	{
            		if (Double.isInfinite(pr.getPrediction(predid)))
            		{
            			inf = true;
            			probProduct = pr.getProductId();
            			explain = pr.getExplanation(predid);
            		}
            		if (Double.isNaN(pr.getPrediction(predid)))
            		{
            			nan = true;
            			probProduct = pr.getProductId();
            			explain = pr.getExplanation(predid);
            		}
            	}
            	logger.error("Error occured because: nan = " + nan + ", inf = " + inf + ", problematicProduct = " + probProduct + ", explanation = " + explain);
            	e.printStackTrace(); 
            }
            int max_rank = MAX_RANK;
            int rank = max_rank;
            int step = 1;
            double previousRat = -1;
            for (ProductRating p : predictions)
            {
            	if (p.getPrediction(predid) <= 0 || rank <= 0)
            	{
            		weights.put(p.getProductId(), weights.get(p.getProductId()) + predweight * p.getReliability(predid));
            		continue;
            	}
            	ranks.put(p.getProductId(), ranks.get(p.getProductId()) + rank * predweight * p.getReliability(predid));
            	weights.put(p.getProductId(), weights.get(p.getProductId()) + predweight * p.getReliability(predid));
            	
            	if (!explanations.containsKey(p.getProductId()))
            		explanations.put(p.getProductId(), "");
                explanations.put(p.getProductId(), explanations.get(p.getProductId()) + ";" + predid + ":" + rank + "(" + p.getPrediction(predid) + ")");
                final double rat = p.getPrediction(predid);
                if (previousRat == rat)
                {
                    // same rating, do not decrease rank, just increase step
                    step += 1;
                }
                else
                {
                    rank -= step;
                    step = 1;
                }
                previousRat = rat;
            }
        }
        // set predictions
        for (ProductRating p : predictions)
        {
        	final String expl = explanations.get(p.getProductId());
        	if (expl == null)
        		addProductRating(p, ranks.get(p.getProductId()) / Math.max(1e-6, weights.get(p.getProductId())), "");
        	else
        	{
        		addProductRating(p, ranks.get(p.getProductId()) / Math.max(1e-6, weights.get(p.getProductId())), explanations.get(p.getProductId()));
        	}
        }

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
	public void serialize(ObjectOutputStream out) throws IOException {
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
	{
		return EmptyCommit.INSTANCE;
	}

    
    @Override
    public Commitable updateProducts(UpdateProductsDelta delta) {
        return EmptyCommit.INSTANCE;
    }

}
