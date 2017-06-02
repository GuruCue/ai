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
package com.gurucue.recommendations.prediction.content;

import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.TIntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.misc.Misc;
import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;


/**
 * Use this recommender when events have an attribute as a product_id, however you need recommendation of the product. 
 */

public class AttributeRecommender extends Predictor implements Cloneable {
    private final Logger logger;
    
    // predictors that will be used together 
    protected Predictor predictor;
    
    // attribute index 
    protected final int attribute_index;
    
    DataStore data;
    
    public AttributeRecommender(String name, Settings settings) {
        super(name, settings);
        logger = LogManager.getLogger(AttributeRecommender.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        
        // create predictors
        String predictorName = settings.getSetting(name + "_PREDICTOR");
        predictor = (Predictor) Misc.createClassObject(settings.getSetting(predictorName+"_CLASS"), predictorName, settings);
        
        attribute_index = settings.getSettingAsInt(name + "_ATTRIBUTE_INDEX");
    }
    
    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) {
    	if (predictions.size() == 0)
    		return;
    	
        // change ids to ids of
        TLongList products_ind = new TLongArrayList();
        for (ProductRating pr : predictions)
        {
            products_ind.add(pr.getProductId());
            final Long newID = pr.getProductData().getLongAttrValue(attribute_index);
            if (newID == null)
                pr.setProductID(data, -1);
            else
            {
                pr.setProductID(data, newID);
            }
        }
        predictor.getPredictions(predictions, tags);
        // restore products 
        final int prSize = predictions.size();
        for (int i = 0; i < prSize; i++)
        {
            predictions.get(i).setProductID(data, products_ind.get(i));
        }
    }    

    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int id_shift, Map<String,String> tags) {
    }
    
    @Override
    public Commitable updateConsumer(DataStore.UpdateData updateData) {
        logger.error("Update consumer not implemented for AttributeRecommender");
        return null;
    }

    @Override
    public void updateModel(DataStore data) {
        logger.error("Update model not implemented for AttributeRecommender");
    }

    @Override
    public ProductPair getBestPair(int consumerIndex) {
        logger.error("Get best pair not implemented for AttributeRecommender");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumerIndex) {
        logger.error("Needs profiling not implemented for AttributeRecommender");
        return false;
    }

    @Override
    public void updateModelQuick(DataStore data) {
        logger.error("UpdateModelQuick not implemented for AttributeRecommender");
    }

    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData newData) {
        return new Update(predictor.updateModelIncremental(newData), newData.dataStore);
    }

    @Override
    public Commitable updateProducts(UpdateProductsDelta delta) {
        return new Update(predictor.updateProducts(delta), delta.dataStore);
    }


	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		predictor.serialize(out);
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
		return new Update(predictor.updateModelFromFile(in, data), data);
	}     
    
    private class Update implements Commitable {
        private final Commitable commit;
        private final DataStore newData;

        Update(final Commitable commit, final DataStore newData) {
            this.commit = commit;
            this.newData = newData;
        }

        @Override
        public void commit() {
            data = newData;
            commit.commit();
        }
    }
   
}
