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
import gnu.trove.iterator.TByteFloatIterator;
import gnu.trove.iterator.TByteIterator;
import gnu.trove.list.TFloatList;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.map.TByteFloatMap;
import gnu.trove.map.hash.TByteFloatHashMap;
import gnu.trove.set.TIntSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.ConsumerBuysData;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;


/**
 * A recommender that returns the average probability of buying this product. Probabilities depend on the context of events. 
 * 
 * This class can be used also by other predictors if average values of users and products are needed. 
 * All the necessary getters are provided. 
 */
public class AverageProbPredictorContext extends Predictor {
    private static final long serialVersionUID = 1L;
    
    private static final byte NULL_CONTEXT_ID = -10; // what key plays the role of null in TByteFloatHashMap
    private static final float NULL_FLOAT_VALUE = -1f; // what value plays the role of null in TByteFloatHashMap
    private final Logger logger;
    
    // the name of the event
    private final String BUYSNAME;
    
    // context specific information
    private final String CONTEXT_NAME;
    private final int CONTEXT_META_ID;
    private static final byte NOCONTEXT_ID = -1;

    private final long timestamp;

    private Data data;
    
    // index of dates meta
    private final int DATES_ID;
    
    public AverageProbPredictorContext(String name, Settings settings) {
        super(name, settings);
        logger = LogManager.getLogger(AverageProbPredictorContext.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        BUYSNAME = settings.getSetting(name + "_BUYS");
        int ndays = settings.getSettingAsInt(name + "_NDAYS");
        DATES_ID = settings.getSettingAsInt(name+"_DATE_META");
        //dates = settings.getSettingAsBoolean(BUYSNAME+"_DATE");
        CONTEXT_NAME = settings.getSetting(name + "_CONTEXT_NAME");
        CONTEXT_META_ID = settings.getSettingAsInt(name + "_CONTEXT_META");

        timestamp = ndays * 86400000L;
    }

    @Override
    public void getPredictions(final List<ProductRating> predictions, final Map<String, String> tags)
    {
        final byte contextId;
        if (tags.containsKey(CONTEXT_NAME))
        {
            byte c;
            try {
                c = Byte.parseByte(tags.get(CONTEXT_NAME), 10);
            }
            catch (NumberFormatException e) {
                c = NOCONTEXT_ID;
            }
            contextId = c;
        }
        else
            contextId = NOCONTEXT_ID;

        final ArrayList<PredictorProductData> productsData = data.productsData;
        for (ProductRating pr : predictions)
        {
            float avgVal = productsData.get(pr.getProductIndex()).averages.get(contextId);
            float pred = Math.min(Math.max(0.0f, avgVal), 1.0f);
            addProductRating(pr, pred, "");
        }
    }
    
    private static TFloatList initializeArray(int capacity)
    {
        TFloatList al = new TFloatArrayList(capacity);
        for (int i = 0; i<capacity; i++)
            al.add(0.0f);
        return al;
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
        AverageProbPredictorContext clone = (AverageProbPredictorContext)super.clone();
        if (null != data) clone.data = new Data(data);
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
    public Commitable updateConsumer(final DataStore.UpdateData updateData) {
        final Commitable c;
        if (updateData.consumerIndex >= data.consumersData.size())
        {
            logger.info("updateConsumer: the consumer is not yet accounted for, performing full recalculation");
            c = new Data(updateData.dataStore, updateData);
            logger.info("updateConsumer: full recalculation done");
        }
        else
        {
            logger.info("updateConsumer: updating data for the specified consumer");
            c = data.updateAverages(updateData);
            logger.info("updateConsumer: consumer data update done");
        }
        return c;
    }

    @Override
    public void getSimilarProducts(final TIntSet productIndices,
            final List<ProductRating> predictions, final int id_shift, final Map<String,String> tags) {
        final byte contextId;
        if (null != tags && tags.containsKey(CONTEXT_NAME))
        {
            byte c;
            try {
                c = Byte.parseByte(tags.get(CONTEXT_NAME), 10);
            }
            catch (NumberFormatException e) {
                c = NOCONTEXT_ID;
            }
            contextId = c;
        }
        else
            contextId = NOCONTEXT_ID;

        final ArrayList<PredictorProductData> productsData = data.productsData;

        for (ProductRating pr : predictions)
        {
            float avgVal = productsData.get(pr.getProductIndex()).averages.get(contextId);
            float pred = Math.min(Math.max(0.0f, avgVal), 1.0f);
            addProductRating(pr, pred, "", id_shift);
        }
    }

    private class Data implements Commitable {
        // consider only events that occured after soonest
        private final long soonest, latest;

        // computed averages of ratings
        private final TByteFloatMap globalAverage;

        private final ArrayList<PredictorConsumerData> consumersData; // indexed by consumerIndex
        private final ArrayList<PredictorProductData> productsData; // indexed by productIndex

        /**
         * Construct data by computing it: averages of products and users.
         *
         * @param dataStore
         * @param updateData optional data about changed consumer, if there was a change (i.e. if this was not meant to be a full update)
         */
        Data(final DataStore dataStore, final DataStore.UpdateData updateData) {
            // read data
            final List<ConsumerData> consumers = dataStore.getConsumers();
            final List<ProductData> products = dataStore.getProducts();
            // read buys data
            final int buysEventIndex = dataStore.getEventsDescriptor(BUYSNAME).index;
            final int updatedConsumerIndex = null == updateData ? -1 : updateData.consumerIndex;

            int cl = consumers.size() + (updatedConsumerIndex < consumers.size() ? 0 : 1);
            final int pl = products.size();

            globalAverage = new TByteFloatHashMap();
            productsData = new ArrayList<PredictorProductData>(pl);
            consumersData = new ArrayList<PredictorConsumerData>(cl);

            // compute soonest
            // first find latest and then subtract timestamp
            if (DATES_ID >= 0)
            {
                long tmpLatest = 0;
                for (int i = 0; i < cl ; i++)
                {
                    final ConsumerData cd = updatedConsumerIndex == i ? updateData.newData : consumers.get(i);
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

            // initialize products data
            for (int pi = pl - 1; pi >= 0; pi--) {
                productsData.add(new PredictorProductData());
            }

            // sum up all relevant ratings
            float noContextGlobalAverage = 0.0f;
            for (int ci = 0; ci < cl; ci++)
            {
                final ConsumerBuysData c = (ConsumerBuysData) (updatedConsumerIndex == ci ? updateData.newData : consumers.get(ci)).events[buysEventIndex];
                final PredictorConsumerData consumerData = new PredictorConsumerData();
                consumersData.add(consumerData);
                final TByteFloatMap consumerAverages = consumerData.averages;
                final TByteFloatMap consumerOffsets = consumerData.offsets;
                float noContextConsumerAverage = 0.0f;

                final int indLen = null == c ? 0 : c.indices.length; // count of products for this consumer
                if (indLen > 0)
                {
                    final long [] dates;
                    if (DATES_ID >= 0)
                        dates = c.getMeta().getLongValuesArray(DATES_ID);
                    else
                        dates = null;
                    final byte [] context = c.getMeta().getByteValuesArray(CONTEXT_META_ID);
                    for (int i=0; i<indLen; i++)
                    {
                        if (DATES_ID >= 0)
                        {
                            if (dates[i] < soonest)
                                continue;
                        }
                        // add 1 to consumer averages (NOCONTEX and CONTEX)
                        final int pi = c.indices[i]; // product index
                        final PredictorProductData productData = productsData.get(pi);
                        final TByteFloatMap productAverages = productData.averages;
                        final TByteFloatMap productsN = productData.sN;

                        noContextConsumerAverage += 1;
                        productAverages.put(NOCONTEXT_ID, productAverages.get(NOCONTEXT_ID) + 1);
                        productsN.put(NOCONTEXT_ID, productsN.get(NOCONTEXT_ID) + 1);
                        // update global average
                        noContextGlobalAverage += 1;

                        final byte contextId = context[i];
                        float cavg = consumerAverages.get(contextId);
                        float pavg = productAverages.get(contextId); // product average for this contextId
                        float psn = productsN.get(contextId); // product sN for this contextId
                        float gavg = globalAverage.get(contextId); // global average for this contextId
                        if (cavg == NULL_FLOAT_VALUE)
                        {
                            cavg = 0.0f;
                            consumerOffsets.put(contextId, 0.0f);
                        }
                        if (pavg == NULL_FLOAT_VALUE)
                        {
                            pavg = 0.0f;
                            psn = 0.0f;
                        }
                        if (gavg == NULL_FLOAT_VALUE)
                            gavg = 0.0f;
                        consumerAverages.put(contextId, cavg + 1);
                        productAverages.put(contextId, pavg + 1);

                        productsN.put(contextId, psn + 1);
                        // update global average
                        globalAverage.put(contextId, gavg + 1);

                    }
                }

                consumerAverages.put(NOCONTEXT_ID, noContextConsumerAverage);
            }
            globalAverage.put(NOCONTEXT_ID, noContextGlobalAverage);


            final TByteFloatIterator globalAverageIterator = globalAverage.iterator();
            while (globalAverageIterator.hasNext())
            {
                globalAverageIterator.advance();                
                final byte contextId = globalAverageIterator.key();
                final float value = globalAverageIterator.value();
                // how many users actually bought something in this context?
                int consWithBuys = 0;
                for (int ci = 0; ci < cl; ci++)
                {
                    if (consumersData.get(ci).averages.get(contextId) != NULL_FLOAT_VALUE)
                        consWithBuys++;
                }

                final float ga_context = value / (consWithBuys * pl);
                globalAverage.put(contextId, ga_context);


                for (int i=0; i<pl; i++)
                {
                    final TByteFloatMap avgs = productsData.get(i).averages;
                    avgs.put(contextId, avgs.get(contextId) / consWithBuys);
                }
                for (int i=0; i<cl; i++)
                {
                    final PredictorConsumerData cdata = consumersData.get(i);
                    final float caverage = cdata.averages.get(contextId) / pl;
                    cdata.averages.put(contextId, caverage);
                    cdata.offsets.put(contextId, caverage - ga_context);
                }

            }
         }

        /**
         * Copy constructor, for cloning.
         */
        Data(final Data parentData) {
            soonest = parentData.soonest;
            latest = parentData.latest;
            globalAverage = new TByteFloatHashMap(parentData.globalAverage);
            final ArrayList<PredictorConsumerData> parentConsumersData = parentData.consumersData;
            final ArrayList<PredictorProductData> parentProductsData = parentData.productsData;
            consumersData = new ArrayList<PredictorConsumerData>(parentConsumersData.size());
            productsData = new ArrayList<PredictorProductData>(parentProductsData.size());
            for (final PredictorConsumerData d : parentConsumersData)
                consumersData.add(new PredictorConsumerData(d));
            for (final PredictorProductData d : parentProductsData)
                productsData.add(new PredictorProductData(d));
        }

        /**
         * Updates averages of a single consumer. Averages of products are left untouched.
         *
         * @param updateData
         */
        Commitable updateAverages(final DataStore.UpdateData updateData) {
            final List<ProductData> products = updateData.dataStore.getProducts();
            final ConsumerBuysData c = (ConsumerBuysData) updateData.newData.events[updateData.dataStore.getEventsDescriptor(BUYSNAME).index];
            final int indLen = null == c ? 0 : c.indices.length; // count of products for this consumer

            // new data
            final PredictorConsumerData newData = new PredictorConsumerData();
            final TByteFloatMap newAverages = newData.averages;
            final TByteFloatMap newOffsets = newData.offsets;

            final TByteIterator contextIterator = globalAverage.keySet().iterator();
            while (contextIterator.hasNext())
            {
                newAverages.put(contextIterator.next(), 0.0f);
            }
            float noContextConsumerAverage = 0.0f;
            if (indLen > 0)
            {
                final long [] dates;
                if (DATES_ID >= 0)
                    dates = c.getMeta().getLongValuesArray(DATES_ID);
                else
                    dates = null;
                final byte [] context = c.getMeta().getByteValuesArray(CONTEXT_META_ID);
                for (int i=0; i<indLen; i++)
                {
                    if (DATES_ID >= 0)
                    {
                        if (dates[i] < soonest)
                            continue;
                    }
                    noContextConsumerAverage += 1.0f;
                    final byte contextId = context[i];
                    newAverages.put(contextId, newAverages.get(contextId) + 1);
                }
            }
            newAverages.put(NOCONTEXT_ID, noContextConsumerAverage);

            final int pl = products.size();
            final TByteFloatIterator globalAverageIterator = globalAverage.iterator();
            while (globalAverageIterator.hasNext())
            {
                globalAverageIterator.advance();
                final byte contextId = globalAverageIterator.key();
                final float globalContextAverage = globalAverageIterator.value();

                // update all keys
                final float caverage = newAverages.get(contextId) / pl;
                newOffsets.put(contextId, caverage - globalContextAverage);
                newAverages.put(contextId, caverage);
            }

            return new UpdateDelta(updateData.consumerIndex, newData);

        }

        @Override
        public void commit() {
            data = this;
        }

        private class UpdateDelta implements Commitable {
            final int consumerIndex;
            final PredictorConsumerData newData;

            UpdateDelta(final int consumerIndex, final PredictorConsumerData newData) {
                this.consumerIndex = consumerIndex;
                this.newData = newData;
            }

            @Override
            public void commit() {
                consumersData.set(consumerIndex, newData);
            }
        }
    }

    /**
     * Contains all data about a consumer for this predictor.
     */
    static class PredictorConsumerData {
        final TByteFloatMap averages; // from consumersAverages, indexed by contextId
        final TByteFloatMap offsets; // from consumersOffsets, indexed by contextId

        PredictorConsumerData() {
            averages = new TByteFloatHashMap(5, 0.8f, NULL_CONTEXT_ID, NULL_FLOAT_VALUE);
            offsets = new TByteFloatHashMap(5, 0.8f, NULL_CONTEXT_ID, NULL_FLOAT_VALUE);
            averages.put(NOCONTEXT_ID, 0.0f);
            offsets.put(NOCONTEXT_ID, 0.0f);
        }

        /**
         * Copy constructor.
         * @param parentData
         */
        private PredictorConsumerData(final PredictorConsumerData parentData) {
            averages = new TByteFloatHashMap(parentData.averages);
            offsets = new TByteFloatHashMap(parentData.offsets);
        }
    }

    /**
     * Contains all data about a product for this predictor.
     */
    static class PredictorProductData {
        final TByteFloatMap averages;
        final TByteFloatMap sN;

        PredictorProductData() {
            averages = new TByteFloatHashMap(5, 0.8f, NULL_CONTEXT_ID, NULL_FLOAT_VALUE);
            sN = new TByteFloatHashMap(5, 0.8f, NULL_CONTEXT_ID, NULL_FLOAT_VALUE);
            averages.put(NOCONTEXT_ID, 0.0f);
            sN.put(NOCONTEXT_ID, 0.0f);
        }

        /**
         * Copy constructor.
         * @param parentData
         */
        private PredictorProductData(final PredictorProductData parentData) {
            averages = new TByteFloatHashMap(parentData.averages);
            sN = new TByteFloatHashMap(parentData.sN);
        }
    }

    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData newData) {
        logger.error("Update model incremental not implemented yet. Class can not be used yet in learning.");
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Commitable updateProducts(UpdateProductsDelta delta) {
        logger.error("Update products not implemented yet. Class can not be used yet in learning.");
        // TODO Auto-generated method stub
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
