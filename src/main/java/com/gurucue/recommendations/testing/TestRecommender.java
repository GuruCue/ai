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

import com.gurucue.recommendations.recommender.MasterRecommender;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.DataStore;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.gurucue.recommendations.recommender.dto.*;
import com.gurucue.recommendations.testing.measures.Measure;
import com.gurucue.recommendations.recommender.RecommendationSettings;
import com.gurucue.recommendations.recommender.RecommenderNotReadyException;

/**
 * Class for testing recommenders. It contains several methods for different types of data. 
 * Every method must accept an ArrayList<Measure> object.
 */
public class TestRecommender {
    private static org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(TestRecommender.class.getName());
    
    /**
     * A standard learn data - test data validation of recommender. 
     * Required: the dates of events need to be stored. 
     * All events before and on trainDate are used as training examples,
     * and dates after trainDate and before or equal testDate are used as testing examples.

     * @param recommender Recommender instance to be tested.
     * @param trainDate All events up to this date are used as training data.
     * @param testDate All events up to this date (and after trainDate) are used as testing data.
     * @param event Testing event. 
     * @param testPercentage Percentage of data before trainDate that are used as testing.
     * @param set
     * @param ms
     * @return
     * @throws InterruptedException
     * @throws RecommenderNotReadyException
     */
    public static EvaluationResult evaluateByDate(MasterRecommender recommender, Date trainDate, Date testDate, String event, float testPercentage,
                                                  RecommendationSettings set, ArrayList<Measure> ms) throws InterruptedException, RecommenderNotReadyException {
        // create an empty evaluation result and add focused groups
        EvaluationResult r = prepareEvaluationResult(recommender.getData(), event, ms);
        return r; // TODO
    }
    
    private static EvaluationResult prepareEvaluationResult(DataStore data, String event, ArrayList<Measure> ms)
    {
        final int eventIndex = data.getEventsDescriptor(event).index;
        final List<ConsumerData> consumersData = data.getConsumers();
        EvaluationResult tmp = null;
        try {
            tmp = new EvaluationResult(ms);
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        
        int [] consumerBounds = {0, 20, 50, 10000, 10000};
        int [] productBounds = {0, 20, 50, 10000, 10000};
        for (int i=0; i<consumerBounds.length-2; i++)
            for (int j=0; j<productBounds.length-2; j++)
            {
                TLongSet consumers = new TLongHashSet(); 
                TLongSet products = new TLongHashSet(); 
                
                // select consumers that are within bounds
                for (int c = consumersData.size()-1; c>=0; c--)
                {
                    if (consumersData.get(c).events == null || consumersData.get(c).events[eventIndex] == null)
                        continue;
                    final int n = consumersData.get(c).events[eventIndex].getN();
                    if (n >= consumerBounds[i] && n <= consumerBounds[i+1])
                        consumers.add(c);
                }
                
                // select products that are within bounds
                for (int p = data.getProducts().size()-1; p>=0; p--)
                {
                    if (data.getProducts().get(p).freq >= productBounds[j] && data.getProducts().get(p).freq <= productBounds[j+1])
                    {
                        products.add(p);
                    }
                }
                
                tmp.addFocusedGroup("cons_bounds:("+consumerBounds[i]+","+consumerBounds[i+1]+"),prod_bounds:("+productBounds[j]+","+productBounds[j+1]+")", consumers, products);
            }
        return tmp;
    }    
}
