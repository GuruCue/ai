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
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

import com.gurucue.recommendations.misc.Misc;
import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;


/**
 * This predictor takes another predictor and uses it  to generate predictions. 
 * Then, it adds another prediction using getSimilarProducts by considering only N last bought items of the user.
 * 
 * Requirement: products of users must be ordered by time, where last products correspond to products bought latest.
 * It works with ratings and with buys.
 */
public class AddSimilarLastProductsPredictor extends Predictor {
    // N last items
    final int NITEMS;
    
    // shift of IDs for adding similar items predictions
    final int ID_SHIFT;
    
    // event where products 
    final String EVENTNAME;
    
    // data
    DataStore data;
    
    // predictor
    final Predictor predictor; // predictor that will produce all necessary predictions (should be a stick together predictor or something similar that can hold several predictor)
    
    public AddSimilarLastProductsPredictor(final String name, final Settings settings) throws CloneNotSupportedException {
        super(name, settings);

        NITEMS = settings.getSettingAsInt(name + "_NITEMS");
        ID_SHIFT = settings.getSettingAsInt(name + "_ID_SHIFT");
        EVENTNAME = settings.getSetting(name + "_EVENT");
        String pn = settings.getSetting(name + "_PREDICTOR");
        final String className = settings.getSetting(pn+"_CLASS");
        predictor = (Predictor) Misc.createClassObject("com.gurucue.recommendations.prediction."+className, pn, settings);
    }

    @Override
    public void updateModel(final DataStore data) {
        predictor.updateModel(data);
        this.data = data;
    }

    @Override
    public Commitable updateConsumer(final DataStore.UpdateData updateData) {
        return predictor.updateConsumer(updateData);
    }
    
    @Override
    public void getPredictions(final List<ProductRating> predictions, final Map<String, String> tags) {
        predictor.getPredictions(predictions, null);

        // (assuming we deal with only one user, the same predictions can be used)
        // just create an array of last products
        final int [] products = this.data.getConsumerByIndex(predictions.get(0).getConsumerIndex()).events[this.data.getEventsDescriptor(EVENTNAME).index].getProductIndices();
        TIntSet lproducts = new TIntHashSet();
        int counter = products.length-1;
        while (counter > 0 && lproducts.size() < NITEMS)
        {
            lproducts.add(products[counter]);
            counter --;
        }

        if (lproducts.size() > 0)
            predictor.getSimilarProducts(lproducts, predictions, ID_SHIFT, null);
        else
        {
            // simply copy values of predictor
            for (ProductRating pr : predictions)
            {
                addProductRating(pr, pr.getPrediction(predictor.getID()), "", ID_SHIFT - ID + predictor.getID());
            }
        }
    }

    @Override
    public void getSimilarProducts(TIntSet productIndices, List<ProductRating> items, int id_shift, Map<String,String> tags) {
        predictor.getSimilarProducts(productIndices, items, id_shift, null);
    }

    @Override
    public ProductPair getBestPair(int consumerIndex) {
        return predictor.getBestPair(consumerIndex);
    }

    @Override
    public boolean needsProfiling(int consumerIndex) {
        return predictor.needsProfiling(consumerIndex);
    }

    @SuppressWarnings("deprecation")
	@Override
    public void updateModelQuick(DataStore data) {
        predictor.updateModelQuick(data);
    }
    
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData newData) {
    	if (this.data == null)
    		this.data = newData.dataStore;
        return predictor.updateModelIncremental(newData);
    }

    @Override
    public Commitable updateProducts(UpdateProductsDelta delta) {
        return predictor.updateProducts(delta);
    }

	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
    	if (this.data == null)
    		this.data = data;
        return predictor.updateModelFromFile(in, data);
	}
}
