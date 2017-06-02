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
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import gnu.trove.set.TIntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.misc.Misc;
import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;


/**
 * This predictor simply puts together predictions of several predictors. Predictions are not combined in any way.
 * A simple predictor; code is self explanatory.
 */

public class StickTogetherHybridPredictor extends Predictor implements Cloneable {

    private final Logger logger;
    
    // predictors that will be used together 
    protected ArrayList<Predictor> predictors;
    
    public StickTogetherHybridPredictor(String name, Settings settings) {
        super(name, settings);
        logger = LogManager.getLogger(StickTogetherHybridPredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        
        // create predictors
        predictors = new ArrayList<Predictor> ();
        String [] predictorNames = settings.getAsStringArray(name + "_PREDICTORS");
        for (String pn : predictorNames)
        {
            final String className = settings.getSetting(pn+"_CLASS");
	    predictors.add((Predictor) Misc.createClassObject(className, pn, settings));
        }
    }
    
    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) {
        for (Predictor p: predictors)
        {
            p.getPredictionsTime(predictions, tags);
        }
    }    

    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int id_shift, Map<String,String> tags) {
        for (Predictor p: predictors)
        {
            p.getSimilarProductsTime(productIndices, predictions, id_shift, tags);
        }
    }
    
    @Override
    public Commitable updateConsumer(DataStore.UpdateData updateData) {
        final Commitable[] childCommitables = new Commitable[predictors.size()];
        int i = 0;
        for (Predictor p: predictors)
        {
            childCommitables[i] = p.updateConsumer(updateData);
            i++;
        }
        return new UpdateDelta(childCommitables);
    }

    @Override
    public void updateModel(DataStore data) {
        for (Predictor p: predictors)
            p.updateModel(data);
    }

    @Override
    public ProductPair getBestPair(int consumerIndex) {
        logger.error("Get best pair not implemented for StickTogetherHybridPredictor");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumerIndex) {
        logger.error("Needs profiling not implemented for StickTogetherHybridPredictor");
        return false;
    }

    @Override
    public void updateModelQuick(DataStore data) {
        logger.error("UpdateModelQuick not implemented for StickTogetherHybridPredictor");
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException {
        StickTogetherHybridPredictor clone = (StickTogetherHybridPredictor)super.clone();
        clone.predictors = new ArrayList<Predictor> ();
        for (Predictor p : predictors)
        {
            clone.predictors.add((Predictor) p.clone());
        }
        return clone;
    }

    private static class UpdateDelta implements Commitable {
        private final Commitable[] childCommitables;

        UpdateDelta(final Commitable[] childCommitables) {
            this.childCommitables = childCommitables;
        }

        @Override
        public void commit() {
            for (int i = 0; i < childCommitables.length; i++)
                childCommitables[i].commit();
        }
    }

    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData data) {
        ArrayList<Commitable> commits = new ArrayList<Commitable>();
        for (Predictor p: predictors)
            commits.add(p.updateModelIncremental(data));
        return new UpdateAll(commits);
    }

    @Override
    public Commitable updateProducts(UpdateProductsDelta delta) {
        ArrayList<Commitable> commits = new ArrayList<Commitable>();
        for (Predictor p: predictors)
            commits.add(p.updateProducts(delta));
        return new UpdateAll(commits);
    }
    
	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
        for (Predictor p: predictors)
            p.serialize(out);
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        ArrayList<Commitable> commits = new ArrayList<Commitable>();
        for (Predictor p: predictors)
            commits.add(p.updateModelFromFile(in, data));
        return new UpdateAll(commits);
	}        
    
    
    private class UpdateAll implements Commitable {
        private final ArrayList<Commitable> commits;

        UpdateAll(final ArrayList<Commitable> commits) {
            this.commits = commits;
        }

        @Override
        public void commit() {
            for (Commitable c : commits)
                if (c != null)
                    c.commit();
        }
    }
}
