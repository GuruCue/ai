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
package com.gurucue.recommendations.recommender;

import com.gurucue.recommendations.recommender.RecommendProduct;
import com.gurucue.recommendations.recommender.RecommendationSettings;
import com.gurucue.recommendations.recommender.RecommenderNotReadyException;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.decision.DecisionModule;
import com.gurucue.recommendations.misc.Misc;
import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ConsumerEventsData;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.ProductData;
import com.gurucue.recommendations.recommender.dto.TagsManager;
import com.gurucue.recommendations.recommender.reader.Reader;

/**
 * Main recommender class.
 * 
 */
public class MasterRecommender implements Recommender {
    private final Logger logger;
    private final String logPrefix;
    
    // rating predictor
    private Predictor predictor;
    // decision module
    private DecisionModule decision;
    // data source
    private final Reader reader;
    
    // events that prevent further recommending of the product (e.g. buying, rating)
    String [] NOT_RECOMMEND_EVENTS; 
    
    // data
    DataStore data;
    
    
    // model lock used to take care of updating and read-only operations on data and predictor module
    private final ManagedReadWriteLock modelLock;
    // lock for reading
    private final ReentrantReadWriteLock.ReadLock modelReadLock;
    // lock for writing
    private final ReentrantReadWriteLock.WriteLock modelWriteLock;

    // and internal class taking care of updating the predictor and the data
    private final RecommenderUpdater updater;

    /**
     * Main constructor for the MasterRecommender class.
     * @param r Reader with access to database.
     */
    protected MasterRecommender(final Reader r)
    {
        this.reader = r;
        final String id = r.getRecommenderId().toString();
        this.logPrefix = "[REC " + id + "] ";
        logger = LogManager.getLogger(MasterRecommender.class.getName());
        modelLock = new ManagedReadWriteLock(false, id); // use unfair locking
        modelReadLock = modelLock.readLock();
        modelWriteLock = modelLock.writeLock();

        final Settings settings = reader.getSettings(); // cache Settings for the processing below

        // PREDICTOR
        // get name of predictor
        String predictorName = settings.getSetting("PREDICTOR");
        // get className of predictor
        String className = settings.getSetting(predictorName + "_CLASS");
        predictor = (Predictor) Misc.createClassObject(className, predictorName, settings);

        // DECISION MODULE
        // get name of decision module
        String decisionName = settings.getSetting("DECISION");
        // get className of decision
        className = settings.getSetting(decisionName + "_CLASS");
        decision = (DecisionModule) Misc.createClassObject(className, decisionName, settings);

        NOT_RECOMMEND_EVENTS = settings.getAsStringArray("NOT_RECOMMEND_EVENTS");

        this.updater = new RecommenderUpdater(this, this.logPrefix);
        this.updater.start();
    }
    
    @Override
    public void stop() {
        try {
            // stop the updater thread
            updater.stop();
        }
        catch (Throwable e) {
            logger.error(logPrefix + "Exception while stopping the updater thread: " + e.toString(), e);
        }
        try {
            // stop the lock manager
            modelLock.stop();
        }
        catch (Throwable e) {
            logger.error(logPrefix + "Exception while stopping the managed lock: " + e.toString(), e);
        }
    }

    @Override
    public ProductRating getRecommendation(final long consumerId, final RecommendProduct product) throws RecommenderNotReadyException, InterruptedException {
        // do not update the predictor right now, I am going to use it ...
        final long startNanos = System.nanoTime();
        modelReadLock.lockInterruptibly();
        try {
            final long deltaNanos = System.nanoTime() - startNanos;
            if (deltaNanos > 5000000000L) {
                // it took more than 5 seconds to commit deltas
                logger.warn(logPrefix + "It took more than 5 seconds to obtain a read lock for getRecommendation(): " + ((deltaNanos / 1000000L) / 1000.0) + " seconds"); // a float with 3 decimals
            }

            if (predictor == null)
                throw new RecommenderNotReadyException();
            ArrayList<ProductRating> pr = new ArrayList<ProductRating> ();
            
            if (data.getConsumerIndex(consumerId) < 0 || data.getProductIndex(product.productID) < 0)
                return null; 

            pr.add(new ProductRating(data.getProductById(product.productID), 
                    data.getProductIndex(product.productID), 
                    data.getConsumerById(consumerId),
                    data.getConsumerIndex(consumerId),
                    product.productTag));
            
            predictor.getPredictions(pr, null);
            return pr.get(0);
        }
        finally {
            modelReadLock.unlock();
        }
    }

    
    @Override
    public ProductRating[] getRecommendation(
            final long consumerId,
            final RecommendProduct [] products,
            final RecommendationSettings recset
    ) throws RecommenderNotReadyException, InterruptedException 
    {
        final long startNanos = System.nanoTime();
        modelReadLock.lockInterruptibly();
        try {
            final long deltaNanos = System.nanoTime() - startNanos;
            if (deltaNanos > 50000000L) {
                // it took more than 50 ms to commit deltas
                logger.warn(logPrefix + "It took more than 50 ms to obtain a read lock for getRecommendation(): " + (deltaNanos / 1000000L) + " ms");
            }

            if (predictor == null) throw new RecommenderNotReadyException();
            if (recset == null)
            {
                logger.error(logPrefix + "Recommendation settings are not provided, cannot produce recommendation");
                return null;
            }
            final int maxResults = recset.getMaxResults();
            final boolean randomizeResults = recset.isRandomizeResults();
            Map<String,String> tags = recset.getTags();
            if (tags == null)
                tags = new HashMap<String, String>();
            tags.put(TagsManager.MAX_RECOMMEND_TAG, String.valueOf(maxResults));
            tags.put(TagsManager.SECONDARY_TAG, "");

            ArrayList<ProductRating> resultList = new ArrayList<ProductRating>();
            int consumerIndex = data.getConsumerIndex(consumerId);
            ConsumerData consumerData;
            if (consumerIndex < 0)
                consumerData = null;
            else
                consumerData = data.getConsumerByIndex(consumerIndex);

            outer:
            for (RecommendProduct rp : products)
            {
                final int productIndex = data.getProductIndex(rp.productID);
                if (productIndex < 0)
                    continue;

                ProductData pd = data.getProductByIndex(productIndex);
                if (pd == null)
                    continue;

                // if product already rated (or seen or bought etc.), skip it
                if (null != NOT_RECOMMEND_EVENTS && null != consumerData)
                    for (String e: NOT_RECOMMEND_EVENTS)
                    {
                        final ConsumerEventsData d = consumerData.events[data.getEventsDescriptor(e).index];
                        if ((d != null) && (d.containsProduct(productIndex)))
                            continue outer;
                    }
                resultList.add(new ProductRating(pd,
                                         productIndex,
                                         consumerData,
                                         consumerIndex,
                                         rp.productTag));
            }
            if (resultList != null && resultList.size() > 0)
            {
                predictor.getPredictionsTime(resultList, tags);
                // select maxResults with a decision module
                ProductRating[] prating = decision.selectBestCandidatesTime(resultList, maxResults, randomizeResults, tags);
                return prating;
            }
            else
                return new ProductRating[0];
        }
        finally {
            modelReadLock.unlock();
        }
    }

    @Override
    public ProductRating[] getSimilarProducts(
            final long [] seedProducts,
            final RecommendProduct [] candidateProducts,
            final RecommendationSettings recset
    ) throws RecommenderNotReadyException, InterruptedException {
        final long startNanos = System.nanoTime();
        modelReadLock.lockInterruptibly();
        try {
            final long deltaNanos = System.nanoTime() - startNanos;
            if (deltaNanos > 50000000L) {
                // it took more than 50 ms to commit deltas
                logger.warn(logPrefix + "It took more than 50 ms to obtain a read lock for getSimilarProducts(): " + (deltaNanos / 1000000L) + " ms");
            }

            if (predictor == null)
                throw new RecommenderNotReadyException();

            final int maxResults = recset.getMaxResults();
            final boolean randomizeResults = recset.isRandomizeResults();
            Map<String,String> tags = recset.getTags();
            if (tags == null)
                tags = new HashMap<String, String>();
            tags.put(TagsManager.MAX_RECOMMEND_TAG, String.valueOf(maxResults));
            tags.put(TagsManager.SECONDARY_TAG, "");

            // create empty ProductRatings
            ArrayList<ProductRating> pr = new ArrayList<ProductRating> ();
            // prepare a set of seed products
            TIntList prIndices = new TIntArrayList ();
            for (int i=0; i<seedProducts.length; i++)
            {
                final int indx = data.getProductIndex(seedProducts[i]);
                if (indx < 0)
                    continue;
                prIndices.add(indx);
            }
            TIntSet prSet = new TIntHashSet(prIndices);

            for (RecommendProduct rp : candidateProducts)
            {
                final int idx = data.getProductIndex(rp.productID);
                if (idx < 0 || prSet.contains(idx))
                    continue;

                pr.add(new ProductRating(data.getProductByIndex(idx), idx, null, -1, rp.productTag));
            }
        
            predictor.getSimilarProductsTime(prSet, pr, 0, tags);
            
            tags.put("MasterRecommender_similar", "true");
            return decision.selectBestCandidates(pr, maxResults, randomizeResults, tags);
        }
        finally {
            modelReadLock.unlock();
        }
    }

    public Settings getSettings()
    {
        return reader.getSettings();
    }

    @Override
    public RecommenderUpdaterJob updateModel(boolean async) {
        final RecommenderUpdaterJob job = new FullUpdate(this);
        scheduleJob(job, async);
        return job;
    }

    @Override
    public RecommenderUpdaterJob updateIncremental(boolean async) throws InterruptedException {
        final RecommenderUpdaterJob job = new IncrementalUpdate(this);
        scheduleJob(job, async);
        return job;
    }

    @Override
    public RecommenderUpdaterJob updateIncrementalAndProductsUntilFinished(boolean async) throws InterruptedException {
        final RecommenderUpdaterJob job = new IncrementalAndProductsUpdateUntilFinished(this);
        scheduleJob(job, async);
        return job;
    }

    @Override
    public RecommenderUpdaterJob updateProducts(boolean async)
            throws InterruptedException {
        final RecommenderUpdaterJob job = new ProductsUpdate(this);
        scheduleJob(job, async);
        return job;
    }
    

    private void scheduleJob(RecommenderUpdaterJob job, final boolean async) {
        try {
            job = updater.update(job);
            if (!async) {
                job.waitUntilFinished();
            }
        }
        catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted while scheduling a model update", e); // convert to unchecked exception
        }
    }

    @Override
    public long [] getNextPair(final long consumerId) throws InterruptedException {
        final long startNanos = System.nanoTime();
        modelReadLock.lockInterruptibly();
        try {
            final long deltaNanos = System.nanoTime() - startNanos;
            if (deltaNanos > 5000000000L) {
                // it took more than 5 seconds to commit deltas
                logger.warn(logPrefix + "It took more than 5 seconds to obtain a read lock for getNextPair(): " + ((deltaNanos / 1000000L) / 1000.0) + " seconds"); // a float with 3 decimals
            }

            ProductPair pair = predictor.getBestPair(data.getConsumerIndex(consumerId));

            long [] result = new long [2];
            result[0] = data.getProductByIndex(pair.i1).productId; 
            result[1] = data.getProductByIndex(pair.i2).productId; 
            return result;
        }
        finally {
            modelReadLock.unlock();
        }
    }

    @Override
    public boolean needsProfiling(final long consumerId) throws InterruptedException {
        final long startNanos = System.nanoTime();
        modelReadLock.lockInterruptibly();
        try {
            final long deltaNanos = System.nanoTime() - startNanos;
            if (deltaNanos > 5000000000L) {
                // it took more than 5 seconds to commit deltas
                logger.warn(logPrefix + "It took more than 5 seconds to obtain a read lock for needsProfiling(): " + ((deltaNanos / 1000000L) / 1000.0) + " seconds"); // a float with 3 decimals
            }

            return predictor.needsProfiling(data.getConsumerIndex(consumerId));
        }
        finally {
            modelReadLock.unlock();
        }
    }
    
    public DataStore getData() throws InterruptedException {
        final long startNanos = System.nanoTime();
        modelReadLock.lockInterruptibly();
        try {
            final long deltaNanos = System.nanoTime() - startNanos;
            if (deltaNanos > 5000000000L) {
                // it took more than 5 seconds to commit deltas
                logger.warn(logPrefix + "It took more than 5 seconds to obtain a read lock for getData(): " + ((deltaNanos / 1000000L) / 1000.0) + " seconds"); // a float with 3 decimals
            }

            return data;
        }
        finally {
            modelReadLock.unlock();
        }
    }

    public void saveToFile(final String path) throws InterruptedException
    {
        scheduleJob(new SaveModel(this, path), false);
    }

    public RecommenderUpdaterJob loadFromFile(String path, boolean async)
    {
        // loading requires a write lock; it is the same as update. 
        final RecommenderUpdaterJob job = new LoadUpdate(this, path);
        scheduleJob(job, async);
        return job;
    }

    /**
     * Creates a new recommender object based on the given reader.
     * @param r Reader with access to data. 
     * @param async Update predictors asynchronously or synchronously?
     * @return
     */
    public static Recommender getRecommender(Reader r, boolean async)
    {
        Recommender recommender = new MasterRecommender(r);
        recommender.updateModel(async);
        return recommender;
    }

    /**
     * Loads a recommender from file
     * 
     * @param path Path to the file containing recommender 
     * @param r Reader with access to data. 
     * @return
     */
    public static Recommender getRecommender(String path, Reader r, boolean async)
    {
        Recommender recommender = new MasterRecommender(r);
        recommender.loadFromFile(path, async);
        return recommender;
    }
    
    
    
    void changeModel(final DataStore data, final Predictor predictor, final DecisionModule decision) throws InterruptedException
    {
        final long startNanos = System.nanoTime();
        modelWriteLock.lockInterruptibly();
        try {
            final long deltaNanos = System.nanoTime() - startNanos;
            if (deltaNanos > 5000000000L) {
                // it took more than 5 seconds to commit deltas
                logger.warn(logPrefix + "It took more than 5 seconds to obtain a write lock to change the model: " + ((deltaNanos / 1000000L) / 1000.0) + " seconds"); // a float with 3 decimals
            }

            if (null != data) {
                this.data = data;
//                decision.setData(data);
            }
            if (null != predictor) this.predictor = predictor;
            if (null != decision) this.decision = decision;
//            recommenderInitialized.signalAll(); // tell anyone waiting that recommender is now in business
        }
        finally {
            modelWriteLock.unlock();
        }
    }

    Predictor getPredictor() {
        return predictor;
    }

    Predictor getPredictorClone() throws InterruptedException, CloneNotSupportedException {
        final long startNanos = System.nanoTime();
        modelReadLock.lockInterruptibly();
        try {
            final long deltaNanos = System.nanoTime() - startNanos;
            if (deltaNanos > 5000000000L) {
                // it took more than 5 seconds to commit deltas
                logger.warn(logPrefix + "It took more than 5 seconds to obtain a read lock for getPredictorClone(): " + ((deltaNanos / 1000000L) / 1000.0) + " seconds"); // a float with 3 decimals
            }

            if (null == predictor) throw new NullPointerException("Cannot clone the predictor: it is null");
            return (Predictor)predictor.clone();
        }
        finally {
            modelReadLock.unlock();
        }
    }

    void commitDeltas(final DataStore.UpdateIncrementalData dataDelta, final Commitable predictorDelta, final Commitable decisionDelta) throws InterruptedException {
    	// dataDelta needs to be first merged with old data and redundant events need to be filtered out.
    	// write lock is unnecessary, since original data is not altered
    	if (dataDelta != null)
    		dataDelta.mergeData();
        final long startNanos = System.nanoTime();
        modelWriteLock.lockInterruptibly();
        try {
            final long deltaNanos = System.nanoTime() - startNanos;
            if (deltaNanos > 5000000000L) {
                // it took more than 5 seconds to commit deltas
                logger.warn(logPrefix + "It took more than 5 seconds to obtain a write lock to commit data deltas: " + ((deltaNanos / 1000000L) / 1000.0) + " seconds"); // a float with 3 decimals
            }

            if (dataDelta != null)
            {
            	final long start = System.currentTimeMillis();
                dataDelta.commit();
                logger.info(logPrefix + "Complete data commit took " + ((System.currentTimeMillis() - start) / 1000) + " seconds");
            }
            if (predictorDelta != null)
            {
            	final long start = System.currentTimeMillis();
                predictorDelta.commit();
                logger.info(logPrefix + "Predictor commit took " + ((System.currentTimeMillis() - start) / 1000) + " seconds");
            }
            if (decisionDelta != null)
            {
            	final long start = System.currentTimeMillis();
                decisionDelta.commit();
                logger.info(logPrefix + "Decision commit took " + ((System.currentTimeMillis() - start) / 1000) + " seconds");
            }
        }
        finally {
            modelWriteLock.unlock();
        }
    }

    void commitDeltas(final DataStore.UpdateProductsDelta productsDelta, final Commitable predictorDelta, final Commitable decisionDelta) throws InterruptedException {
        final long startNanos = System.nanoTime();
        modelWriteLock.lockInterruptibly();
        try {
            final long deltaNanos = System.nanoTime() - startNanos;
            if (deltaNanos > 5000000000L) {
                // it took more than 5 seconds to commit deltas
                logger.warn(logPrefix + "It took more than 5 seconds to obtain a write lock to commit product deltas: " + ((deltaNanos / 1000000L) / 1000.0) + " seconds"); // a float with 3 decimals
            }

            if (productsDelta != null)
                productsDelta.commit();
            if (predictorDelta != null)
                predictorDelta.commit();
            if (decisionDelta != null)
                decisionDelta.commit();
        }
        finally {
            modelWriteLock.unlock();
        }
    }
    
    
    static class FullUpdate extends RecommenderUpdaterJob {
        private static final Logger logger = LogManager.getLogger(FullUpdate.class);
        private final MasterRecommender recommender;

        FullUpdate(final MasterRecommender recommender) {
            this.recommender = recommender;
        }

        void work() throws InterruptedException {
            final String logPrefix = recommender.logPrefix;
            logger.info(logPrefix + "Reading data + full update of predictor");
            long startTime, reading=0, updating=0; 

            Predictor newPredictor = null;
            DecisionModule newDecision = null;
            try {
				newPredictor = recommender.predictor.createEmptyClone();
	            newDecision = recommender.decision.createEmptyClone();
            } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                logger.error(logPrefix + "Failed to clone predictor and decision module: " + e.toString(), e);
            }            
            

            // time reading data from the database updating model 
            startTime = System.currentTimeMillis();
            // reader and settings are declared as final, therefore no locking necessary
            final DataStore data = new DataStore(recommender.reader); // DataStore constructor does not read events.
            DataStore.UpdateIncrementalData dataDelta = data.readNextBatch();
            reading += System.currentTimeMillis() - startTime;
            
            // update model
            startTime = System.currentTimeMillis();
            logger.info(logPrefix + "Before update models");
            Commitable predictorDelta = newPredictor.updateModelIncremental(dataDelta);
            Commitable decisionDelta = newDecision.updateModelIncremental(dataDelta);
            updating += System.currentTimeMillis() - startTime;
            logger.info(logPrefix + "Update took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
            
            startTime = System.currentTimeMillis();
            recommender.commitDeltas(dataDelta, predictorDelta, decisionDelta);
            updating += System.currentTimeMillis() - startTime;
            logger.info(logPrefix + "Commit took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
            dataDelta = null;
            predictorDelta = null;
            decisionDelta = null;
            
            final Runtime runtime = Runtime.getRuntime();
            
            while (!data.finishedReading())
            {
                // read additional batch of data
                startTime = System.currentTimeMillis();
                dataDelta = data.readNextBatch();
                reading += System.currentTimeMillis() - startTime;
                // make an incremental update
                startTime = System.currentTimeMillis();
                predictorDelta = newPredictor.updateModelIncremental(dataDelta);
                decisionDelta = newDecision.updateModelIncremental(dataDelta);
                updating += System.currentTimeMillis() - startTime;
                logger.info(logPrefix + "Update took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");

                startTime = System.currentTimeMillis();
                recommender.commitDeltas(dataDelta, predictorDelta, decisionDelta);
                logger.info(logPrefix + "Commit took " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
                updating += System.currentTimeMillis() - startTime;
                logger.debug(logPrefix + "Current free memory: " + runtime.freeMemory() + " bytes");
            }

            logger.info(logPrefix + "Reading of data took " + ((reading) / 1000) + " seconds");
            logger.info(logPrefix + "Model computation took " + ((updating) / 1000) + " seconds, changing to the new model");

            recommender.changeModel(data, newPredictor, newDecision);

            logger.debug(logPrefix + "Full update finished, new model is active, current free memory: " + runtime.freeMemory() + " bytes");
        }

        /**
         * Implementation of an {@link Object#equals(Object)} for jobs,
         * so the queuing method can easily spot duplicates.
         */
        @Override
        public boolean equals(Object obj) {
            // Full update doesn't have any state information, so every instance of it is equivalent to any other instance of it.
            return (null != obj) && (obj instanceof FullUpdate);
        }

        /**
         * Implementation of an {@link Object#hashCode()} for jobs, so
         * it's consistent with {@link #equals(Object)}.
         */
        @Override
        public int hashCode() {
            // basically it's a singleton instance, so return a constant
            // beware: in the future the MasterRecommender instance may become a part of the state, then it will have to be included in the hashCode() computation
            return 17;
        }
    }

    
    
    static class IncrementalUpdate extends RecommenderUpdaterJob {
        private static final Logger logger = LogManager.getLogger(IncrementalUpdate.class);
        protected final MasterRecommender recommender;

        IncrementalUpdate(final MasterRecommender recommender) {
            this.recommender = recommender;
        }

        void work() throws InterruptedException {
            final String logPrefix = recommender.logPrefix;
            logger.info(logPrefix + "Reading data + incremental update of predictor");
            long startTime, reading=0, updating=0, commiting;

            // time reading data from the database updating model 
            startTime = System.currentTimeMillis();
            // reader and settings are declared as final, therefore no locking necessary
            final DataStore data = this.recommender.getData();

            if (data == null) {
                logger.warn(logPrefix + "data is null; engaging full update instead");
                new FullUpdate(recommender).work();
                return;
            }

            DataStore.UpdateIncrementalData dataDelta = data.readNextBatch();
            reading += System.currentTimeMillis() - startTime;
            
            // update model
            startTime = System.currentTimeMillis();
            Commitable predictorDelta = recommender.predictor.updateModelIncremental(dataDelta);
            Commitable decisionDelta = recommender.decision.updateModelIncremental(dataDelta);
            updating = System.currentTimeMillis() - startTime;
            startTime = System.currentTimeMillis();
            recommender.commitDeltas(dataDelta, predictorDelta, decisionDelta);
            commiting = System.currentTimeMillis() - startTime;

            logger.info(logPrefix + "Reading of data took " + (reading / 1000L) + " seconds, model incremental update took " + (updating / 1000L) + " seconds, commiting new data took " + (commiting / 1000L) + " seconds");
        }

        /**
         * Implementation of an {@link Object#equals(Object)} for jobs,
         * so the queuing method can easily spot duplicates.
         */
        @Override
        public boolean equals(Object obj) {
            // Full update doesn't have any state information, so every instance of it is equivalent to any other instance of it.
            return (null != obj) && (obj instanceof IncrementalUpdate);
        }

        /**
         * Implementation of an {@link Object#hashCode()} for jobs, so
         * it's consistent with {@link #equals(Object)}.
         */
        @Override
        public int hashCode() {
            // basically it's a singleton instance, so return a constant
            // beware: in the future the MasterRecommender instance may become a part of the state, then it will have to be included in the hashCode() computation
            return 18;
        }
    }

    /**
     * Invokes ProductsUpdate.work() and IncrementalUpdate.work() until the DataStore.finishedReading()
     * returns true.
     * Extends the IncrementalUpdate without redefining equals() and hashCode(), so the queueing
     * methods sees them as the same classes (queueing both classes at the same time is meaningless).
     */
    static class IncrementalAndProductsUpdateUntilFinished extends IncrementalUpdate {
        private static final Logger logger = LogManager.getLogger(IncrementalAndProductsUpdateUntilFinished.class);

        IncrementalAndProductsUpdateUntilFinished(final MasterRecommender recommender) {
            super(recommender);
        }

        @Override
        void work() throws InterruptedException {
            final String logPrefix = recommender.logPrefix;

            if (recommender.getData() == null) {
                logger.warn(logPrefix + "data is null; engaging full update instead");
                new FullUpdate(recommender).work();
                return;
            }

            final Runtime runtime = Runtime.getRuntime();

            final ProductsUpdate productsUpdater = new ProductsUpdate(recommender);
            logger.info(String.format("%sInvoking initial ProductsUpdate, current free memory: %.3f MB, total: %.3f MB, max: %.3f MB", logPrefix, runtime.freeMemory() / 1048576.0, runtime.totalMemory() / 1048576.0, runtime.maxMemory() / 1048576.0));
            productsUpdater.work();
            logger.info(String.format("%sInvoking initial IncrementalUpdate, current free memory: %.3f MB, total: %.3f MB, max: %.3f MB", logPrefix, runtime.freeMemory() / 1048576.0, runtime.totalMemory() / 1048576.0, runtime.maxMemory() / 1048576.0));
            super.work();

            int repeats = 1;
            while (!this.recommender.getData().finishedReading()) {
                logger.info(String.format("%sInvoking IncrementalUpdate #%d, current free memory: %.3f MB, total: %.3f MB, max: %.3f MB", logPrefix, repeats, runtime.freeMemory() / 1048576.0, runtime.totalMemory() / 1048576.0, runtime.maxMemory() / 1048576.0));
                super.work();
                repeats++;
            }

            logger.info(String.format("%sDone, current free memory: %.3f MB, total: %.3f MB, max: %.3f MB", logPrefix, runtime.freeMemory() / 1048576.0, runtime.totalMemory() / 1048576.0, runtime.maxMemory() / 1048576.0));
        }
    }

    static class ProductsUpdate extends RecommenderUpdaterJob {
        private static final Logger logger = LogManager.getLogger(ProductsUpdate.class);
        private final MasterRecommender recommender;

        ProductsUpdate(final MasterRecommender recommender) {
            this.recommender = recommender;
        }

        void work() throws InterruptedException {
            final String logPrefix = recommender.logPrefix;
            logger.info(logPrefix + "Updating products in recommender");
            long startTime, reading=0, updating=0; 

            // time reading data from the database updating model 
            startTime = System.currentTimeMillis();
            // reader and settings are declared as final, therefore no locking necessary
            final DataStore data = this.recommender.getData();

            if (data == null) {
                logger.warn(logPrefix + "data is null; engaging full update instead");
                new FullUpdate(recommender).work();
                return;
            }

            DataStore.UpdateProductsDelta productsDelta = data.updateProducts();
            reading += System.currentTimeMillis() - startTime;
            
            // update model
            startTime = System.currentTimeMillis();
            Commitable predictorDelta = recommender.predictor.updateProducts(productsDelta);
            Commitable decisionDelta = recommender.decision.updateProducts(productsDelta);
            updating = System.currentTimeMillis() - startTime;
            
            startTime = System.currentTimeMillis();
            recommender.commitDeltas(productsDelta, predictorDelta, decisionDelta);
            long commiting = System.currentTimeMillis() - startTime;

            logger.info(logPrefix + "Reading of products took " + (reading / 1000L) + " seconds, model updating with new products took " + (updating / 1000L) + " seconds, commiting took " + (commiting / 1000L) + " seconds");
        }

        /**
         * Implementation of an {@link Object#equals(Object)} for jobs,
         * so the queuing method can easily spot duplicates.
         */
        @Override
        public boolean equals(Object obj) {
            // Full update doesn't have any state information, so every instance of it is equivalent to any other instance of it.
            return (null != obj) && (obj instanceof ProductsUpdate);
        }

        /**
         * Implementation of an {@link Object#hashCode()} for jobs, so
         * it's consistent with {@link #equals(Object)}.
         */
        @Override
        public int hashCode() {
            // basically it's a singleton instance, so return a constant
            // beware: in the future the MasterRecommender instance may become a part of the state, then it will have to be included in the hashCode() computation
            return 19;
        }
    }

    static class LoadUpdate extends RecommenderUpdaterJob {
        private static final Logger logger = LogManager.getLogger(LoadUpdate.class);
        private final MasterRecommender recommender;
        private final String path;

        LoadUpdate(final MasterRecommender recommender, String path) {
            this.recommender = recommender;
            this.path = path;
        }

        void work() throws InterruptedException {
            final String logPrefix = recommender.logPrefix;
            logger.info(logPrefix + "Reading recommender from file started: " + path);
            
            Predictor newPredictor = null;
            DecisionModule newDecision = null;
            DataStore newData = null;
            try
            {
               FileInputStream fileIn = new FileInputStream(path);
               ObjectInputStream in = new ObjectInputStream(fileIn);
               
               newData = DataStore.deserialize(recommender.reader, in);
               
               try {
                   newPredictor = recommender.predictor.createEmptyClone();
                   newDecision = recommender.decision.createEmptyClone();
               } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                   logger.error(logPrefix + "Failed to clone predictor and decision module: " + e.toString(), e);
               }

               Commitable c1 = newPredictor.updateModelFromFile(in, newData);
               Commitable c2 = newDecision.updateModelFromFile(in, newData);
               c1.commit();
               c2.commit();

               in.close();
               fileIn.close();
            }catch(IOException | ClassNotFoundException i)
            {
                logger.error(logPrefix + "Failed to update predictor and decision module from file: " + i.toString(), i);
                if (recommender.getData() == null) {
                    logger.warn(logPrefix + "data is null; engaging full update instead");
                    new FullUpdate(recommender).work();
                }
                return;
            }

            recommender.changeModel(newData, newPredictor, newDecision);
            final Runtime runtime = Runtime.getRuntime();
            logger.debug(logPrefix + "Load update finished, new model is active, current free memory: " + runtime.freeMemory() + " bytes");
        }

        /**
         * Implementation of an {@link Object#equals(Object)} for jobs,
         * so the queuing method can easily spot duplicates.
         */
        @Override
        public boolean equals(Object obj) {
            // Full update doesn't have any state information, so every instance of it is equivalent to any other instance of it.
            return (null != obj) && (obj instanceof FullUpdate);
        }

        /**
         * Implementation of an {@link Object#hashCode()} for jobs, so
         * it's consistent with {@link #equals(Object)}.
         */
        @Override
        public int hashCode() {
            // basically it's a singleton instance, so return a constant
            // beware: in the future the MasterRecommender instance may become a part of the state, then it will have to be included in the hashCode() computation
            return 21;
        }
    }
    
    static class SaveModel extends RecommenderUpdaterJob {
        private final MasterRecommender recommender;
        private final String path;
        private final String logPrefix;

        SaveModel(final MasterRecommender recommender, final String path) {
            this.recommender = recommender;
            this.path = path;
            this.logPrefix = recommender.logPrefix;
        }

        @Override
        void work() throws InterruptedException {
            final File realFile = new File(path); // after the serialization has been successfully finished, only then the serialization file is renamed to the given filename
            final File tmpFile = new File(path + ".tmp"); // we serialize to a temporary file, which can be removed if anything goes wrong, so the old correct serialization data remains
            // saving requires a read lock; it does not change the recommender in anyway.
            FileOutputStream fileOut = null;
            ObjectOutputStream out = null;
            final long startNanos = System.nanoTime();
            recommender.modelReadLock.lockInterruptibly();
            try {
                final long deltaNanos = System.nanoTime() - startNanos;
                if (deltaNanos > 5000000000L) {
                    // it took more than 5 seconds to commit deltas
                    recommender.logger.warn(logPrefix + "It took more than 5 seconds to obtain a read lock for saveToFile(): " + ((deltaNanos / 1000000L) / 1000.0) + " seconds"); // a float with 3 decimals
                }

                recommender.logger.info(logPrefix + "Serialization of recommender to file "+ path + " started.");
                fileOut = new FileOutputStream(tmpFile);
                out = new ObjectOutputStream(fileOut);
                recommender.data.serialize(out);
                recommender.predictor.serialize(out);
                recommender.decision.serialize(out);
                out.close();
                out = null;
                fileOut.close();
                fileOut = null;
                if (!tmpFile.renameTo(realFile)) {
                    recommender.logger.error(logPrefix + "Failed to rename temporary serialization file to " + path);
                }
                else {
                    recommender.logger.info(logPrefix + "Serialization of recommender to file " + path + " ended.");
                }
            }
            catch (Exception i)
            {
                recommender.logger.error(logPrefix + "Failed to serialize to file " + path + ": " + i.toString(), i);
                // try not to leak resources
                if (out != null) {
                    try {
                        out.close();
                    }
                    catch (Exception e) {}
                }
                if (fileOut != null) {
                    try {
                        fileOut.close();
                    }
                    catch (Exception e) {}
                }
                tmpFile.delete();
            }
            finally {
                recommender.modelReadLock.unlock();
            }
        }

        /**
         * Implementation of an {@link Object#equals(Object)} for jobs,
         * so the queuing method can easily spot duplicates.
         */
        @Override
        public boolean equals(final Object obj) {
            // It's equal if the job operates on the same recommender and writes to the same file
            if (!(obj instanceof SaveModel)) return false;
            final SaveModel other = (SaveModel)obj;
            return (other.recommender == this.recommender) && this.path.equals(other.path);
        }

        /**
         * Implementation of an {@link Object#hashCode()} for jobs, so
         * it's consistent with {@link #equals(Object)}.
         */
        @Override
        public int hashCode() {
            return path == null ? 17 : path.hashCode();
        }
    }

}
