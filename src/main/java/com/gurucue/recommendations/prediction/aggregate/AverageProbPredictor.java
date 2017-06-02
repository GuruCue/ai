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
package com.gurucue.recommendations.prediction.aggregate;

import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ProductData;
import gnu.trove.set.TIntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.ConsumerBuysData;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;



/**
 * A recommender that returns average product rating and adds average offset of a consumer.
 * A similar procedure is used for predicting the probability.
 */


public class AverageProbPredictor extends Predictor {
    private final Logger logger;
    
    // the name of the event
    private final String BUYSNAME;

    private final long timestamp;
    
    // index of dates meta
    private final int DATES_ID;

    private Data data;
    
    public AverageProbPredictor(String name, Settings settings) {
        super(name, settings);
        logger = LogManager.getLogger(AverageProbPredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        BUYSNAME = settings.getSetting(name + "_BUYS");
        final int ndays = settings.getSettingAsInt(name + "_NDAYS");
        DATES_ID = settings.getSettingAsInt(name+"_DATE_META");
        timestamp = ndays * 86400000L;
    }

    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) 
    {
        for (ProductRating pr : predictions)
        {
            float pred = Math.min(Math.max(0.0f, data.productsAverages[pr.getProductIndex()]), 1.0f); // + consumersOffsets[pr.getConsumerIndex()]), 1.0f);
            addProductRating(pr, pred, "");
        }
    }
    
    
    @Override
    public void updateModel(final DataStore dataStore) {
        logger.info("updating AverageProbPredictor start");
        data = new Data(dataStore, null);
        logger.info("updating AverageProbPredictor end");
    }

    @Override
    public void updateModelQuick(DataStore data) {
        updateModel(data);
    }
    
    @Override
    public Object clone() throws CloneNotSupportedException {
        final AverageProbPredictor clone = (AverageProbPredictor)super.clone();
        if (null != data)
            clone.data = new Data(data);
        return clone;
    }

    @Override
    public ProductPair getBestPair(int consumer_index) {
        logger.error("getBestPair not implemented yet for AverageProbPredictor.");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumer_index) {
        logger.error("needsProfiling not implemented yet for AverageProbPredictor.");
        return false;
    }

    @Override
    public Commitable updateConsumer(DataStore.UpdateData updateData) {
        if (updateData.consumerIndex >= data.consumersAverages.length)
            return new Data(updateData.dataStore, updateData);
        else
            return data.updateAverages(updateData);
    }

    @Override
    public void getSimilarProducts(TIntSet productIndices,
            List<ProductRating> predictions, int id_shift, Map<String,String> tags) {
        for (ProductRating pr : predictions)
        {
            float pred = Math.min(Math.max(0.0f, data.productsAverages[pr.getProductIndex()]), 1.0f);
            addProductRating(pr, pred, "", id_shift);
        }
    }

    private class Data implements Commitable {
        private final long soonest, latest;

        // computed averages of ratings
        private float globalAverage;
        private final float [] productsAverages;
        private final float [] productsN;
        private final float [] consumersAverages;
        private final float [] consumersOffsets;

        /**
         * Compute averages of products and users.
         *
         * @param dataStore
         * @param updateData
         */
        Data(final DataStore dataStore, final DataStore.UpdateData updateData) {
            // read data
            final List<ConsumerData> consumers = dataStore.getConsumers();
            final List<ProductData> products = dataStore.getProducts();
            final int buysEventIndex = dataStore.getEventsDescriptor(BUYSNAME).index;
            final int updatedConsumerIndex = null == updateData ? -1 : updateData.consumerIndex;
            final int cl = consumers.size() + (updatedConsumerIndex < consumers.size() ? 0 : 1);
            final int pl = products.size();

            // initialize ratings
            globalAverage = 0.0f;
            productsAverages = new float [pl];
            productsN = new float[pl];
            consumersAverages = new float [cl];
            consumersOffsets = new float [cl];

            for (int i=0; i<pl; i++)
            {
                productsN[i] = 0;
                productsAverages[i] = 0.01f;
            }
            if (DATES_ID >= 0)
            {
                long tmpLatest = 0;
                for (int ci = 0; ci < cl; ci++)
                {
                    final ConsumerData cd = updatedConsumerIndex == ci ? updateData.newData : consumers.get(ci);
                    final ConsumerBuysData c = (ConsumerBuysData) cd.events[buysEventIndex];
                    if ((null != c) && (c.indices.length > 0))
                    {
                        final long date = c.getMeta().getLongValue(DATES_ID, c.indices.length-1);
                        if (date > tmpLatest)
                            tmpLatest = date;
                    }
                }

                latest = tmpLatest;
                soonest = latest - timestamp;
            }
            else
            {
                latest = 0;
                soonest = 0;
            }

            // sum up all relevant ratings
            int consWithBuys = 0; // number of consumers with at least one bought product
            for (int ci = 0; ci < cl; ci++)
            {
                final ConsumerBuysData c = (ConsumerBuysData) (updatedConsumerIndex == ci ? updateData.newData : consumers.get(ci)).events[buysEventIndex];
                final int indLen = null == c ? 0 : c.indices.length;
                int consumerAverage = 0;
                if (indLen > 0)
                {
                    final long [] dates;
                    if (DATES_ID >= 0)
                        dates = c.getMeta().getLongValuesArray(DATES_ID);
                    else
                        dates = null;
                    for (int i=0; i< indLen; i++)
                    {
                        if (DATES_ID >= 0)
                        {
                            if (dates[i] < soonest)
                                continue;
                        }
                        consumerAverage += 1;
                        globalAverage += 1;
                        final int pi = c.indices[i];

                        productsAverages[pi] += 1;
                        productsN[pi] += 1;
                    }
                    if (consumerAverage > 0)
                        consWithBuys ++;
                }
                consumersAverages[ci] = consumerAverage/pl;
            }

            globalAverage /= consWithBuys * pl;

            for (int i=0; i<pl; i++)
            {
                productsAverages[i] = productsAverages[i]/consWithBuys; // probability of buying this product
            }
            for (int i=0; i<cl; i++)
            {
                consumersOffsets[i] = consumersAverages[i] - globalAverage;
            }

        }

        /**
         * Copy constructor.
         *
         * @param parent
         */
        private Data(final Data parent) {
            soonest = parent.soonest;
            latest = parent.latest;
            productsAverages = Arrays.copyOf(parent.productsAverages, parent.productsAverages.length);
            productsN = Arrays.copyOf(parent.productsN, parent.productsN.length);
            consumersAverages = Arrays.copyOf(parent.consumersAverages, parent.consumersAverages.length);
            consumersOffsets = Arrays.copyOf(parent.consumersOffsets, parent.consumersOffsets.length);
        }

        /**
         * Updates averages of a single consumer. Averages of products are left untouched.
         *
         * @param updateData
         */
        Commitable updateAverages(final DataStore.UpdateData updateData) {
            final int buysEventIndex = updateData.dataStore.getEventsDescriptor(BUYSNAME).index;
            final ConsumerBuysData c = (ConsumerBuysData) updateData.newData.events[buysEventIndex];
            final int pl = updateData.dataStore.getProducts().size();
            int consumerAverage = 0;
            int indLen = null == c ? 0 : c.indices.length;
            if (indLen > 0)
            {
                final long [] dates;
                if (DATES_ID >= 0)
                    dates = c.getMeta().getLongValuesArray(DATES_ID);
                else
                    dates = null;
                for (int i=0; i<indLen; i++)
                {
                    if (DATES_ID >= 0)
                    {
                        if (dates[i] < soonest)
                            continue;
                    }
                    consumerAverage += 1;
                }
                consumerAverage /= pl;
            }
            return new UpdateDelta(updateData.consumerIndex, consumerAverage, consumerAverage - globalAverage);
        }

        @Override
        public void commit() {
            data = this;
        }

        private class UpdateDelta implements Commitable {
            private final int consumerIndex;
            private final float consumerAverage;
            private final float consumerOffset;

            UpdateDelta(final int consumerIndex, final float consumerAverage, final float consumerOffset) {
                this.consumerIndex = consumerIndex;
                this.consumerAverage = consumerAverage;
                this.consumerOffset = consumerOffset;
            }

            @Override
            public void commit() {
                consumersOffsets[consumerIndex] = consumerOffset;
                consumersAverages[consumerIndex] = consumerAverage;
            }
        }
    }

    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData data) {
        logger.error("Update incremental not implemented yet, the predictor cannot be used!");
        return null;
    }

    @Override
    public Commitable updateProducts(UpdateProductsDelta delta) {
        logger.error("Update products not implemented yet, the predictor cannot be used!");
        return null;
    }

	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
        logger.error("Serialization not implemented yet, the predictor cannot be used!");
	}

	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        logger.error("UpdateModelFromFile not implemented yet, the predictor cannot be used!");
        return null;
	}
}
