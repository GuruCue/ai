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
package com.gurucue.recommendations;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.procedure.TObjectProcedure;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.gurucue.recommendations.recommender.*;
import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.MasterRecommender;
import com.gurucue.recommendations.recommender.Recommender;
import com.gurucue.recommendations.recommender.RecommenderUpdaterJob;
import com.gurucue.recommendations.recommender.reader.JdbcProviderReader;

import java.io.File;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * Daemon container for recommender. It configures, starts, and binds a recommender to a RMI name.
 */
public final class BasicRecommenderImpl extends UnicastRemoteObject implements BasicRecommenderRemote {
    private final static String RECOMMENDER_FILENAME_PREFIX = "/opt/GuruCue/RecommenderStates/recommender-";
    private final static String RECOMMENDER_FILENAME_SUFFIX = ".serialized";

    private final Logger log = LogManager.getLogger(BasicRecommenderImpl.class);
    private final long recommenderId;
    private final Recommender recommender;
    private final String stateFilename;
    private final IntervalRunner updateRunner;
    private final IntervalRunner saveRunner;
    private final String logPrefix;
    private RecommenderUpdaterJob lastJob = null;

    public BasicRecommenderImpl(final long recommenderId) throws RemoteException {
        this.recommenderId = recommenderId;
        this.stateFilename = RECOMMENDER_FILENAME_PREFIX + recommenderId + RECOMMENDER_FILENAME_SUFFIX;
        this.logPrefix = "[REC " + recommenderId + "] ";
        log.info(logPrefix + "creating...");
        if (new File(stateFilename).exists()) {
            this.recommender = MasterRecommender.getRecommender(stateFilename, new JdbcProviderReader(recommenderId), true);
            try {
                lastJob = this.recommender.updateIncrementalAndProductsUntilFinished(true);
            } catch (InterruptedException e) {
                log.error(logPrefix + "Initial incremental update on recommender " + recommenderId + " failed: " + e.toString(), e);
            }
        }
        else {
            this.recommender = MasterRecommender.getRecommender(new JdbcProviderReader(recommenderId), true);
        }

        updateRunner = new IntervalRunner(logPrefix + "updater");
        saveRunner = new IntervalRunner(logPrefix + "saver");
        updateRunner.start(600000L, new Runnable() { // every 10 minutes
            @Override
            public void run() {
                updateModel();
            }
        });
        saveRunner.start(1800000L, new Runnable() { // every 30 minutes
            @Override
            public void run() {
                saveModel();
            }
        });
        log.info(logPrefix + "created");
    }

    @Override
    public Recommendations recommendations(final long consumerId, final RecommendProduct[] candidateProducts, final RecommendationSettings recset) throws RecommenderNotReadyException, InterruptedException, RemoteException {
        final long startTime = System.nanoTime();
        final long threadId = Thread.currentThread().getId();
        final StringBuilder logString = new StringBuilder(300);
        logString.append(logPrefix)
                .append("[").append(threadId).append("] requesting ").append(recset == null ? "null" : recset.getMaxResults()).append(" recommendations for consumer ")
                .append(consumerId).append(" from ").append(candidateProducts == null ? "null" : candidateProducts.length).append(" products");
        log.debug(logString.toString());
        try {
            final Recommendations result = toRecommendations(recommender.getRecommendation(consumerId, candidateProducts, recset));
            final long durationMillis = (System.nanoTime() - startTime) / 1000000L;
            logString.setLength(0);
            logString.append(logPrefix)
                    .append("[").append(threadId).append("] returning ").append((result == null) || (result.recommendations == null) ? "null" : result.recommendations.length)
                    .append(" recommendations for consumer ").append(consumerId)
                    .append(" from ").append(candidateProducts == null ? "null" : candidateProducts.length).append(" products, ")
                    .append(durationMillis).append(" ms");
            log.debug(logString.toString());
            return result;
        }
        catch (Throwable e) {
            final StringBuilder logBuilder = new StringBuilder(512);
            logBuilder.append(logPrefix)
                    .append("[")
                    .append(threadId)
                    .append("] recommendations() invocation failed for consumer ")
                    .append(consumerId).append(" on ");
            if (candidateProducts == null) logBuilder.append("(null)");
            else logBuilder.append(candidateProducts.length);
            logBuilder.append(" candidate products: [");
            if ((candidateProducts != null) && (candidateProducts.length > 0)) {
                logBuilder.append(candidateProducts[0].productID);
                final int n = candidateProducts.length;
                for (int i = 1; i < n; i++) logBuilder.append(", ").append(candidateProducts[i].productID);
            }
            logBuilder.append("] (").append(((System.nanoTime() - startTime) / 1000000L)).append(" ms): ").append(e.toString());
            log.error(logBuilder.toString(), e);
            throw e;
        }
    }

    @Override
    public Recommendations similar(final long[] seedProducts, final RecommendProduct[] candidateProducts, final RecommendationSettings recset) throws RecommenderNotReadyException, InterruptedException, RemoteException {
        final long startTime = System.nanoTime();
        final StringBuilder productsDescription = new StringBuilder(32 + ((seedProducts == null ? 0 : seedProducts.length) * 12));
        if ((seedProducts == null) || (seedProducts.length == 0)) productsDescription.append("[no products specified!]");
        else if (seedProducts.length == 1) productsDescription.append("product ").append(seedProducts[0]);
        else {
            productsDescription.append("products ").append(seedProducts[0]);
            for (int i = 1; i < seedProducts.length; i++) productsDescription.append(", ").append(seedProducts[i]);
        }
        final long threadId = Thread.currentThread().getId();
        final StringBuilder logString = new StringBuilder(300);
        logString.append(logPrefix)
                .append("[").append(threadId).append("] requesting ")
                .append(recset.getMaxResults()).append(" similar products to ").append(productsDescription)
                .append(" from ").append(candidateProducts == null ? "null" : candidateProducts.length).append(" products");
        log.debug(logString.toString());
        try {
            final Recommendations result = toRecommendations(recommender.getSimilarProducts(seedProducts, candidateProducts, recset));
            final long durationMillis = (System.nanoTime() - startTime) / 1000000L;
            logString.setLength(0);
            logString.append(logPrefix)
                    .append("[").append(threadId).append("] returning ")
                    .append(result.recommendations.length).append(" similar products to ").append(productsDescription)
                    .append(" from ").append(candidateProducts == null ? "null" : candidateProducts.length).append(" products, ").append(durationMillis).append(" ms");
            log.debug(logString.toString());
            return result;
        }
        catch (Throwable e) {
            final StringBuilder logBuilder = new StringBuilder(512);
            logBuilder.append(logPrefix)
                    .append("[")
                    .append(threadId)
                    .append("] similar() invocation failed for ");
            if (seedProducts == null) logBuilder.append("(null)");
            else logBuilder.append(seedProducts.length);
            logBuilder.append(" seed products [");
            if ((seedProducts != null) && (seedProducts.length > 0)) {
                logBuilder.append(seedProducts[0]);
                final int n = seedProducts.length;
                for (int i = 1; i < n; i++) logBuilder.append(", ").append(seedProducts[i]);
            }
            logBuilder.append("] on ");
            if (candidateProducts == null) logBuilder.append("(null)");
            else logBuilder.append(candidateProducts.length);
            logBuilder.append(" candidate products: [");
            if ((candidateProducts != null) && (candidateProducts.length > 0)) {
                logBuilder.append(candidateProducts[0].productID);
                final int n = candidateProducts.length;
                for (int i = 1; i < n; i++) logBuilder.append(", ").append(candidateProducts[i].productID);
            }
            logBuilder.append("] (").append(((System.nanoTime() - startTime) / 1000000L)).append(" ms): ").append(e.toString());
            log.error(logBuilder.toString(), e);
            throw e;
        }
    }

    @Override
    public long getUpdateInterval() throws InterruptedException {
        return updateRunner.getIntervalMillis() / 1000L;
    }

    @Override
    public void setUpdateInterval(final long updateInterval) throws InterruptedException {
        log.debug(logPrefix + "changing update interval to " + updateInterval + " seconds");
        updateRunner.setIntervalMillis(updateInterval * 1000L);
    }

    @Override
    public long getPersistInterval() throws InterruptedException {
        return saveRunner.getIntervalMillis() / 1000L;
    }

    @Override
    public void setPersistInterval(final long persistInterval) throws InterruptedException {
        log.debug(logPrefix + "changing persist interval to " + persistInterval + " seconds");
        saveRunner.setIntervalMillis(persistInterval * 1000L);
    }

    @Override
    public void updateNow() {
        log.debug(logPrefix + "forcing AI model update");
        updateModel();
    }

    @Override
    public void persistNow() {
        log.debug(logPrefix + "forcing AI model save");
        saveModel();
    }

    @Override
    public long getRecommenderId() {
        return recommenderId;
    }

    @Override
    public String getModelFilename() {
        return stateFilename;
    }

    private Recommendations toRecommendations(final ProductRating[] ratings) {
        final StringBuilder logBuilder = new StringBuilder(1024);
        logBuilder.append(logPrefix).append("[").append(Thread.currentThread().getId()).append("] Returning products with predictions: ");
        final int n = ratings == null ? 0 : ratings.length;
        final Recommendation[] recommendations = new Recommendation[n];
        final StringBuilder explanationBuilder = new StringBuilder();
        final TObjectProcedure<String> explanationFiller = new TObjectProcedure<String>() {
            @Override
            public boolean execute(final String explanation) {
                explanationBuilder.append(explanation);
                return true;
            }
        };
        for (int i = 0; i < n; i++) {
            final ProductRating rating = ratings[i];
            final TIntObjectMap<String> allExplanations = rating.getAllExplanations();
            String explanations = null;
            if ((allExplanations != null) && (allExplanations.size() > 0)) {
                allExplanations.forEachValue(explanationFiller);
                if (explanationBuilder.length() > 0) {
                    explanations = explanationBuilder.toString();
                    explanationBuilder.delete(0, explanationBuilder.length());
                }
            }
            recommendations[i] = new Recommendation(rating.getProductId(), rating.getTags(), rating.getPrediction(), explanations, rating.getPrettyExplanations());
            if (i > 0) logBuilder.append(", ");
            logBuilder.append(rating.getProductId()).append("(p=").append(rating.getPrediction()).append(")");
        }
        log.debug(logBuilder.toString());
        return new Recommendations(recommendations);
    }

    public synchronized void updateModel() {
        if ((lastJob == null) || lastJob.isFinished()) {
            log.info(logPrefix + "updating AI model");
            try {
                lastJob = recommender.updateIncrementalAndProductsUntilFinished(true);
            }
            catch (InterruptedException e) {
                log.error("Incremental update on recommender " + recommenderId + " failed: " + e.toString(), e);
            }
        }
        else {
            log.info(logPrefix + "skipping AI model update: the previous job hasn't finished yet");
        }
    }

    public void stop() {
        log.info(logPrefix + "stopping...");
        updateRunner.stop();
        saveRunner.stop();
        recommender.stop();
        log.info(logPrefix + "stopped");
    }

    public void saveModel() {
        log.info(logPrefix + "saving AI model...");
        try {
            recommender.saveToFile(stateFilename);
        } catch (InterruptedException e) {
            log.error("Saving recommender model state to a file failed: " + e.toString(), e);
        }
    }
}
