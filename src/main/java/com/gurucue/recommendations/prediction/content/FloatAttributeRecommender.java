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

import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.ProductData;
import gnu.trove.set.TIntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;


/**
 * A simple recommender that sorts product according to a selected attribute. 
 */

public class FloatAttributeRecommender extends Predictor implements Cloneable {
    private static final long serialVersionUID = 1L;

    private final Logger logger;
    
    // attribute index 
    protected final int attribute_index;
    
    protected float attribute_avg;
    
    DataStore data;
    
    public FloatAttributeRecommender(String name, Settings settings) {
        super(name, settings);
        logger = LogManager.getLogger(FloatAttributeRecommender.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        
        attribute_index = settings.getSettingAsInt(name + "_ATTRIBUTE_INDEX");
    }
    
    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) {
        for (ProductRating pr : predictions)
        {
            final Float v = pr.getProductData().getFloatAttrValue(attribute_index);
            if (v != null)
            {
                String explanation;
                if (v > attribute_avg)
                    explanation = "high value of " + pr.getProductData().getAttribute(attribute_index).name + " attribute:" + v + ";";
                else
                    explanation = "";
                addProductRating(pr, v, explanation);
            }
            else
                addProductRating(pr, attribute_avg, "");
        }
    }    

    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int id_shift, Map<String,String> tags) {
        // not implemented ... 
        // it could return a set of products with a similar IMDB rating? 
        // However, it does not make much sense. 
    }
    
    @Override
    public Commitable updateConsumer(DataStore.UpdateData updateData) {
        logger.error("Update consumer not implemented for FloatAttributeRecommender");
        return null;
    }

    @Override
    public void updateModel(DataStore data) {
        logger.error("Update model not implemented for FloatAttributeRecommender");
    }

    @Override
    public ProductPair getBestPair(int consumerIndex) {
        logger.error("Get best pair not implemented for FloatAttributeRecommender");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumerIndex) {
        logger.error("Needs profiling not implemented for FloatAttributeRecommender");
        return false;
    }

    @Override
    public void updateModelQuick(DataStore data) {
        logger.error("UpdateModelQuick not implemented for FloatAttributeRecommender");
    }

    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData newData) {
        // does nothing unless data is not defined; otherwise update average of attribute
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = newData.dataStore;
            attribute_avg = computeAverage(this.data.getProducts());
        }        
        return null;
    }

    @Override
    public Commitable updateProducts(UpdateProductsDelta delta) {
        // should simply recompute the average value of the attribute
        return new UpdateAverage(computeAverage(delta.newProducts));
    }
    
	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        if (this.data == null) // initialization of attributes is needed
        {
            this.data = data;
            attribute_avg = computeAverage(this.data.getProducts());
        } 
        return null;
	}       
    
    /**
     * Recomputes average of the float attribute.
     * @param productData
     */
    private float computeAverage(final List<ProductData> products)
    {
        float average = 0;
        int counter = 0;
        for (ProductData pr : products)
        {
            final Float v = pr.getFloatAttrValue(attribute_index);
            if (v != null)
            {
                average += v;
                counter ++;
            }
        }
        if (counter == 0)
            return 0.0f;
        return average/counter;
    }
 
    private class UpdateAverage implements Commitable {
        private final float average;

        UpdateAverage(final float average) {
            this.average = average;
        }

        @Override
        public void commit() {
            attribute_avg = average;
        }
    }

 
}
