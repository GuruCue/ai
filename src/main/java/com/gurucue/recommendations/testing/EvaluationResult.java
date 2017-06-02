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
package com.gurucue.recommendations.testing;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.TByteList;
import gnu.trove.list.TDoubleList;
import gnu.trove.list.TFloatList;
import gnu.trove.list.TIntList;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.gurucue.recommendations.testing.measures.Measure;

/**
 * The EvaluationResult class contains evaluation(s) of a recommender on complete testing data (see TestRecommender)
 * and possibly on subsets of data representing specific, focused groups of consumers and products. 
 * To correctly use this class, the tester should follow the following steps:
 * Step 1: Create a new instance of EvaluationResult, where you specify the measures.
 * Step 2: Add focused groups. Each focused group is determined by two sets: consumers and products.
 * Step 3: Incrementally add consumer-specific results: 
 *         a) a list of relevant products (usually those were consumed by the consumer),
 *         b) a list of recommended products (in the same order as returned by the recommender), and
 *         c) a list of predictions for each relevant product (in the same order as recommended products).
 *         Either of these values can be null or an empty list (if not relevant).
 */
public class EvaluationResult {
    ArrayList<Measure> measures; // 
    Measure recommendingTime;
    List<FocusedResult> focusedResults; // evaluation on subgroups (using the same measures)
    float learningTime;

    public EvaluationResult(ArrayList<Measure> measures) throws CloneNotSupportedException
    {
        this.measures = new ArrayList<Measure>();
        for (Measure m: measures)
            this.measures.add(m.clone());
        focusedResults = new ArrayList<FocusedResult>  ();
    }
    
    /**
     * Ands a subgroup (of users and products) to be tested separately.
     * 
     * @param consumers A set of consumers in the subgroup.
     * @param products A set of products in the subgroup.
     */
    public void addFocusedGroup(String focusedDesc, TLongSet consumers, TLongSet products)
    {
        focusedResults.add(new FocusedResult(focusedDesc, consumers, products, measures));
    }
    
    public void setLearningTime(float learningTime)
    {
        this.learningTime = learningTime;
    }
    
    public void addConsumerResults (int consumerIndex, TLongArrayList relevantID, TByteList ratings, TDoubleList predictions, TLongArrayList recommendedID, float recommendingTime)
    {
        for (Measure m : measures)
            m.addConsumerResults(consumerIndex, relevantID, ratings, predictions, recommendedID, recommendingTime);
        
        // for each focused group prepare a new set of relevant and recommended products.
        for (FocusedResult f : focusedResults)
        {
            if (f.containsConsumer(consumerIndex) == false)
                continue;
            final int relSize = relevantID.size();
            TLongList focusedRelevant = new TLongArrayList(); 
            TLongList focusedRecommended = new TLongArrayList(); 
            TDoubleList focusedPredictions = new TDoubleArrayList(); 
            TByteList focusedRatings = new TByteArrayList();
            for (int i=0; i<relSize; i++)
            {
                if (f.containsProduct(relevantID.get(i)))
                {
                    if (relevantID != null && relevantID.size() > 0)                    
                        focusedRelevant.add(relevantID.get(i));
                    if (predictions != null && predictions.size() > 0)
                        focusedPredictions.add(predictions.get(i));
                    if (ratings != null && ratings.size() > 0)
                        focusedRatings.add(ratings.get(i));
                }
            }
            final int recSize = recommendedID.size();
            for (int i=0; i<recSize; i++)
            {
                if (f.containsProduct(recommendedID.get(i)))
                    focusedRecommended.add(recommendedID.get(i));
            }
            // send data to focused
            f.addConsumerResults(consumerIndex, focusedRelevant, focusedRatings, focusedPredictions, focusedRecommended, recommendingTime);
        }
    }
    
    
    
    public void finalizeComputations()
    {
        for (Measure m : measures)
            m.finalizeComputations();        
        for (FocusedResult f : focusedResults)
            f.finalizeComputations();
    }
    
    public String toString()
    {
        String ret = "learning time took " + learningTime + " seconds";
        for (Measure m: measures)
            ret += " " + m.toString();
        for (FocusedResult f: focusedResults)
            ret += f.toString();
        return ret;
    }
    
    
    class FocusedResult
    {
        TLongSet consumers, products;
        String focusedDesc;
        ArrayList<Measure> measures = new ArrayList<Measure> ();
        
        FocusedResult(String focusedDesc, TLongSet consumers, TLongSet products, ArrayList<Measure> orig_measures)
        {
            this.focusedDesc = focusedDesc;
            this.consumers = consumers;
            this.products = products;
            // create an empty copy of every measure
            for (Measure m: orig_measures)
                try {
                    measures.add(m.clone());
                } catch (CloneNotSupportedException e) {
                    e.printStackTrace();
                }
        }
        
        public void addConsumerResults (int consumerIndex, TLongList relevantProducts, TByteList ratings, TDoubleList predictions, TLongList recommendedProducts, double recommendingTime)
        {
            for (Measure m : measures)
                m.addConsumerResults(consumerIndex, relevantProducts, ratings, predictions, recommendedProducts, recommendingTime);
        }
        
        public void finalizeComputations()
        {
            for (Measure m : measures)
                m.finalizeComputations();
        }
        
        boolean containsConsumer(int consumerIndex)
        {
            return consumers.contains(consumerIndex);
        }
        
        boolean containsProduct(long l)
        {
            return products.contains(l);
        }
        
        public String toString()
        {
            String ret = "Focused group: " + focusedDesc + ", n consumers = " + consumers.size() + ", n products = "+ products.size() + "\n";
            for (Measure m: measures)
                ret += " " + m.toString();
            return ret;
        }
        
    }    
}
