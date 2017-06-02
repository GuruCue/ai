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
package com.gurucue.recommendations.decision;

import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.ProductData;
import com.gurucue.recommendations.recommender.dto.TagsManager;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.dto.Attr;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.LongAttr;
import com.gurucue.recommendations.recommender.dto.MultiValAttr;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateProductsDelta;

/**
 * This decision module arranges products into N virtual channels. 
 */

public class SelectVirtualChannels extends DecisionModule {
    final private static int LEFT_WINDOW = 30*60;
    final private static int RIGHT_WINDOW = 0*60;
    final private static int SIXHOURS = 60*60*6;
    
    final private int N_VIRTUAL; // number of virtual channels
    final private int START_ATTRIBUTE; // index of attribute containing begin times
    final private int END_ATTRIBUTE; // index of attribute containing begin times
    final private int SIM_ATTRIBUTE; // inde of attribute for computing similarity
    final private int CHANNEL_ATTRIBUTE; // index of attribute containing channel number
    final private int TITLE_ATTRIBUTE; // index of attribute containing channel number
    final private float CATHCUP_PERCENTAGE; // maximal percentage of recommended items as catchup
    final private float AFTER_SIX_HOURS_PERCENTAGE; // maximal percentage of recommended items after six hours
    
    TIntIntMap freq;
    Map<Integer, TIntIntMap> commonFreq;
    
    public SelectVirtualChannels(String name, Settings settings) {
        super(name, settings);
        N_VIRTUAL = settings.getSettingAsInt(name + "_N_VIRTUAL");
        START_ATTRIBUTE = settings.getSettingAsInt(name + "_START_ATTRIBUTE");
        END_ATTRIBUTE = settings.getSettingAsInt(name + "_END_ATTRIBUTE");
        SIM_ATTRIBUTE = settings.getSettingAsInt(name + "_SIM_ATTRIBUTE");
        CHANNEL_ATTRIBUTE = settings.getSettingAsInt(name + "_CHANNEL_ATTRIBUTE");
        TITLE_ATTRIBUTE = settings.getSettingAsInt(name + "_TITLE_ATTRIBUTE");
        
        if (settings.getSetting(name + "_CATHCUP_PERCENTAGE") == null)
            CATHCUP_PERCENTAGE = 0.5f;
        else
            CATHCUP_PERCENTAGE = settings.getSettingAsFloat(name + "_CATHCUP_PERCENTAGE");
        if (settings.getSetting(name + "_AFTER_SIX_HOURS_PERCENTAGE") == null)
            AFTER_SIX_HOURS_PERCENTAGE = 0.25f;
        else
        	AFTER_SIX_HOURS_PERCENTAGE = settings.getSettingAsFloat(name + "_AFTER_SIX_HOURS_PERCENTAGE");
    }

    public ProductRating[] selectBestCandidates(List<ProductRating> candidates, int maxCandidates, boolean randomizeResults, Map<String,String> tags)
    {
        // TODO
        // select virtual should give more weight to programmes in the near or future or past.
        // if a good item can not be added into the appropriate virtual channel, then add it somewhere else. 
        if (candidates == null || candidates.size() == 0) {
            return null;
        }
        
        List<ProductRating> acc = candidates;
        Collections.sort(acc);
        
        long currentTime = TagsManager.getCurrentTimeSeconds(tags);
        ProductAdder adder = new ProductAdder(tags, TITLE_ATTRIBUTE, -1);

        ArrayList<TLongList> starts = new ArrayList<TLongList> ();
        ArrayList<TLongList> ends = new ArrayList<TLongList> ();
        for (int i=0; i<N_VIRTUAL; i++)
        {
            starts.add(new TLongArrayList ());
            ends.add(new TLongArrayList ());
        }
        
        // first: accept only items that are on programme right now
        // add one item to each channel
        // it should not be similar to any other channels in order to add it
        List <TIntSet> clusters = new ArrayList<TIntSet> ();
        TIntList virtualCounter = new TIntArrayList();
        TIntList catchupCounter = new TIntArrayList();
        TIntList after6Counter = new TIntArrayList();
        for (int i = 0; i < N_VIRTUAL; i++)
        {
            clusters.add(new TIntHashSet());
            virtualCounter.add(0);
            catchupCounter.add(0);
            after6Counter.add(0);
        }
        
        Collections.sort(acc);
        ArrayList<ProductRating> pivotCandidates = new ArrayList<ProductRating> ();
        for (ProductRating pr: acc)
        {
        	if (pr.getPrediction() <= 0)
        		break;
        	
            final Attr startAttr = pr.getProductData().getAttribute(START_ATTRIBUTE);
            final Attr endAttr = pr.getProductData().getAttribute(END_ATTRIBUTE);
            if (startAttr == null || endAttr == null)
                continue;
            
            final long start = ((LongAttr) startAttr).value;
            final long end = ((LongAttr) endAttr).value;
            // if product is out of the window, skip it
            if (start < currentTime && currentTime - start > LEFT_WINDOW)
                continue;
            if (start > currentTime && start - currentTime > RIGHT_WINDOW)
                continue;
            if (end - currentTime < currentTime - start)
                continue;

            final Attr genreAtt = pr.getProductData().getAttribute(SIM_ATTRIBUTE);
            if (genreAtt == null)
                continue;
            pivotCandidates.add(pr);
            if (pivotCandidates.size() > N_VIRTUAL * 4) // have enough pivot candidates
                break;
        }
        
        // if there are less that N_VIRTUAL candidates, simply add first N_VIRTUAL
        int counter = 0;
        while (pivotCandidates.size() < N_VIRTUAL * 4 && counter < acc.size())
        {
            final ProductRating pr = acc.get(counter);
            final Attr genreAtt = pr.getProductData().getAttribute(SIM_ATTRIBUTE);
            if (genreAtt == null)
                continue;
            pivotCandidates.add(pr);
            counter += 1;
        }
        
        // from pivot candidates, select pivots
        // for each virtual channel
        for (int i=0; i<N_VIRTUAL; i++)
        {
            // select best candidate
            int selected = -1;
            double selected_sim = N_VIRTUAL;
            if (i == 0)
                selected = 0;
            else
            {
                // iterate through all products
                for (int j = pivotCandidates.size()-1; j >= 0; j--)
                {
                    // can be added?
                    if (!adder.can_add(pivotCandidates.get(j)))
                        continue;
                    
                    // get genres of candidate products
                    final ProductRating pr1 = pivotCandidates.get(j);
                    final Attr genreAtt1 = pr1.getProductData().getAttribute(SIM_ATTRIBUTE);
                    final TIntSet genres1 = ((MultiValAttr) genreAtt1).getValues();
                    if (genres1 == null || genres1.size() == 0)
                        continue;
                    
                    double sum_sim = 0;
                    boolean same = false;
                    
                    for (int vi=0; vi<N_VIRTUAL; vi++)
                    {
                        if (clusters.get(vi).size() == 0)
                            continue;
                        final double sim = similarity(clusters.get(vi), genres1);
                        if (sim > 0.8)
                        {
                            same = true;
                            break;
                        }
                        sum_sim += sim;
                    }
                    if (!same && selected_sim > sum_sim)
                    {
                        selected = j;
                        selected_sim = sum_sim;
                    }
                }   
            }
               
            if (selected < 0)
                break;
            
            virtualCounter.set(i, virtualCounter.get(i)+1);
            final TLongList start_intervals = starts.get(i);
            final TLongList end_intervals = ends.get(i);
            
            final ProductRating pr = pivotCandidates.get(selected);
            final Attr startAttr = pr.getProductData().getAttribute(START_ATTRIBUTE);
            final Attr endAttr = pr.getProductData().getAttribute(END_ATTRIBUTE);
            final long start = ((LongAttr) startAttr).value;
            final long end = ((LongAttr) endAttr).value;
            final Attr genreAtt = pr.getProductData().getAttribute(SIM_ATTRIBUTE);
            final TIntSet genres = ((MultiValAttr) genreAtt).getValues();
            
            pr.setTag("virtual_channel", String.valueOf(i));
            adder.add(pr);
            start_intervals.add(start);
            end_intervals.add(end);
            clusters.get(i).addAll(genres);
        }
        for (ProductRating pr: acc)
        {
            addProduct(pr, clusters, starts, ends, virtualCounter, catchupCounter, after6Counter, maxCandidates, currentTime, adder, false);
            
            if (adder.size() >= maxCandidates)
                break;
        }

        if (adder.size () < maxCandidates) 
        {
            for (ProductRating pr: acc)
            {
            	addProduct(pr, clusters, starts, ends, virtualCounter, catchupCounter, after6Counter, maxCandidates, currentTime, adder, true);
            
            	if (adder.size() >= maxCandidates)
            		break;
            }
        }
        return adder.getRecommended();
    }
    
    private void addProduct(ProductRating pr, List <TIntSet> clusters, ArrayList<TLongList> starts, ArrayList<TLongList> ends, TIntList virtualCounter, TIntList catchupCounter, TIntList after6Counter, int maxCandidates, long currentTime, ProductAdder adder, boolean forcedAdd)
    {
        int best = -1, secondBest = -1;
        double bestSim = -1;
        final Attr genreAtt = pr.getProductData().getAttribute(SIM_ATTRIBUTE);
        if (genreAtt == null)
            return;
        final TIntSet genres = ((MultiValAttr) genreAtt).getValues();
        
        final Attr startAttr = pr.getProductData().getAttribute(START_ATTRIBUTE);
        final Attr endAttr = pr.getProductData().getAttribute(END_ATTRIBUTE);
        if (startAttr == null || endAttr == null)
            return;
        
        final long start = ((LongAttr) startAttr).value;
        final long end = ((LongAttr) endAttr).value;
        
        for (int i=0; i<N_VIRTUAL; i++)
        {
            final double sim = similarity(clusters.get(i), genres);
            if (sim > bestSim)
            {
                bestSim = sim;
                secondBest = best;
                best = i;
            }
            else if (sim == bestSim && starts.get(i).size() < starts.get(best).size())
            {
                secondBest = best;
                best = i;
            }
            
            if (clusters.get(i).size() == 0)
            {
                if (best < 0)
                    best = i;
                else
                    secondBest = i;
                break;
            }
        }
        
        
        if (virtualCounter.get(best) > maxCandidates / N_VIRTUAL)
            best = -1;
        if (secondBest > -1 && virtualCounter.get(secondBest) > maxCandidates / N_VIRTUAL)
            secondBest = -1;

        // if catchup is filled up
        if (best >-1 && end < currentTime && catchupCounter.get(best) > CATHCUP_PERCENTAGE * maxCandidates)
            best = -1;
        if (secondBest > -1 && end < currentTime && catchupCounter.get(secondBest) > CATHCUP_PERCENTAGE * maxCandidates)
            secondBest = -1;
        
        // if after 6 hours is filled up
        if (best >-1 && start > (currentTime+SIXHOURS) && after6Counter.get(best) > AFTER_SIX_HOURS_PERCENTAGE * maxCandidates)
            best = -1;
        if (secondBest > -1 && start > (currentTime+SIXHOURS) && after6Counter.get(secondBest) > AFTER_SIX_HOURS_PERCENTAGE * maxCandidates)
            secondBest = -1;
        
        // found best? if yes, check if there is space
        if (best == -1)
            return;
        
        TLongList start_intervals = starts.get(best);
        TLongList end_intervals = ends.get(best);
        int interlen = start_intervals.size();
        
        boolean isSpace = true;
        for (int i=0; i<interlen; i++)
        {
            if (start >= start_intervals.get(i) && start <= end_intervals.get(i) ||
                end >= start_intervals.get(i) && end <= end_intervals.get(i) ||
                (start <= start_intervals.get(i) && end >= end_intervals.get(i)))
            {
                isSpace = false;
                break;
            }
        }
        
        if (!isSpace && secondBest > -1)
        {
            isSpace = true;
            start_intervals = starts.get(secondBest);
            end_intervals = ends.get(secondBest);
            interlen = start_intervals.size();
            
            for (int i=0; i<interlen; i++)
            {
                if (start >= start_intervals.get(i) && start <= end_intervals.get(i) ||
                    end >= start_intervals.get(i) && end <= end_intervals.get(i) ||
                    (start <= start_intervals.get(i) && end >= end_intervals.get(i)))
                {
                    isSpace = false;
                    break;
                }
            }
            best = secondBest;
        }  
        
        if (isSpace) // we can add this product
        {
        	boolean added = true;
        	if (forcedAdd)
        		adder.addForced(pr);
        	else
                added = adder.add(pr);
            if (!added)
                return;
            pr.setTag("virtual_channel", String.valueOf(best));
            start_intervals.add(start);
            end_intervals.add(end);
            if (genres != null)
                clusters.get(best).addAll(genres);
            virtualCounter.set(best, virtualCounter.get(best)+1);
            if (end < currentTime)
                catchupCounter.set(best, catchupCounter.get(best)+1);
            if (start > currentTime + SIXHOURS)
                after6Counter.set(best, after6Counter.get(best)+1);
        }
        
    }
    
    private boolean different(TIntSet newCand, List<TIntSet> cands)
    {
        for (TIntSet c : cands)
            if (similarity(c, newCand) >= 0.5)
                return false;
        return true;
    }
    
    private double similarity(TIntSet a, TIntSet b)
    {
        if (a == null || b == null)
            return 0;
        return (similarity_oneside(a, b) + similarity_oneside(b, a)) / (a.size() + b.size());
    }

    private double similarity_oneside(TIntSet a, TIntSet b)
    {
        if (a == null || b == null)
            return 0.0f;
        if (a.size() == 0 || b.size() == 0)
            return 0.0f;
        double sim = 0.0f;
        for (TIntIterator it1 = a.iterator(); it1.hasNext();)
        {
            final int val = it1.next();
            double maxsim = 0;
            for (TIntIterator it2 = b.iterator(); it2.hasNext();)
            {
                final int val2 = it2.next();
                if (val == val2)
                {
                    maxsim = 1;
                    break;
                }
                else
                    maxsim = Math.max(maxsim, commonFreq.get(val).get(val2) / Math.sqrt (freq.get(val)) / Math.sqrt (freq.get(val2)));
            }
            sim += maxsim;
        }
        return sim;
    }
    
    private Commitable updateFrequencies(DataStore newData, List<ProductData> newProducts)
    {
        TIntIntMap newFreq = new TIntIntHashMap();
        Map<Integer, TIntIntMap> newCommonFreq = new HashMap<Integer, TIntIntMap>();     
        
        // import all products in the following Map, where key is the channel.
        Map<String, ArrayList<ProductData>> programmes = new HashMap<String, ArrayList<ProductData>> ();
        for (ProductData p : newProducts)
        {
            if (p.getAttribute(CHANNEL_ATTRIBUTE) == null || p.getAttribute(START_ATTRIBUTE) == null || !p.getAttribute(CHANNEL_ATTRIBUTE).hasValue() || !p.getAttribute(START_ATTRIBUTE).hasValue())
                continue;
            final String channel = p.getAttribute(CHANNEL_ATTRIBUTE).toString();
            
            if (!programmes.containsKey(channel))
                programmes.put(channel, new ArrayList<ProductData> ());
            programmes.get(channel).add(p);
        }

        Comparator<ProductData> comp = new ProductsComparator();
        for (ArrayList<ProductData> val : programmes.values())
        {
            // sort products in each arraylist by start time 
            Collections.sort(val, comp);
            
            // iterate through val and update newFreq and commonFreq
            final int valLen = val.size();
            for (int i = 0; i<valLen; i++)
            {
                if (val.get(i).getAttribute(SIM_ATTRIBUTE) == null)
                    continue;
                final TIntSet genres1 = val.get(i).getAttribute(SIM_ATTRIBUTE).getValues();
                if (genres1 == null || genres1.size() == 0)
                    continue;
                for (TIntIterator it = genres1.iterator(); it.hasNext();)
                {
                    final int gen = it.next();
                    newFreq.put(gen, newFreq.get(gen)+1);
                    // this is to add co-occurences withing the same programme
                    for (TIntIterator it1 = genres1.iterator(); it1.hasNext();)
                    {
                        final int gen1 = it1.next();
                        if (!newCommonFreq.containsKey(gen1))
                            newCommonFreq.put(gen1, new TIntIntHashMap());
                        if (!newCommonFreq.containsKey(gen))
                            newCommonFreq.put(gen, new TIntIntHashMap());
                        newCommonFreq.get(gen1).put(gen, newCommonFreq.get(gen1).get(gen)+1);
                        newCommonFreq.get(gen).put(gen1, newCommonFreq.get(gen).get(gen1)+1);
                    }
                }
                // this is to add co-occurences withing two consequtive programmes
                if (i < valLen - 1)
                {
                    if (val.get(i+1).getAttribute(SIM_ATTRIBUTE) == null)
                        continue;
                    
                    final TIntSet genres2 = val.get(i+1).getAttribute(SIM_ATTRIBUTE).getValues();
                    if (genres2 == null || genres2.size() == 0)
                        continue;
                    for (TIntIterator it = genres1.iterator(); it.hasNext();)
                    {
                        final int gen1 = it.next();
                        for (TIntIterator it2 = genres2.iterator(); it2.hasNext();)
                        {
                            final int gen2 = it2.next();
                            if (!newCommonFreq.containsKey(gen1))
                                newCommonFreq.put(gen1, new TIntIntHashMap());
                            if (!newCommonFreq.containsKey(gen2))
                                newCommonFreq.put(gen2, new TIntIntHashMap());
                            newCommonFreq.get(gen1).put(gen2, newCommonFreq.get(gen1).get(gen2)+1);
                            newCommonFreq.get(gen2).put(gen1, newCommonFreq.get(gen2).get(gen1)+1);
                        }
                    }
                }
            }
        }

        return new UpdateSimilarity(newData, newFreq, newCommonFreq);
        
    }
    
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData data) {
        // if freq is null - do a updateProducts
        if (freq == null)
            return updateFrequencies(data.dataStore, data.dataStore.getProducts());
        return new DoNothingUpdate(data.dataStore);
    }

	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		out.writeObject(freq);
		out.writeObject(commonFreq);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
		final TIntIntMap newFreq = (TIntIntMap) in.readObject();
		final Map<Integer, TIntIntMap> newCommonFreq = (Map<Integer, TIntIntMap>) in.readObject();
        return new UpdateSimilarity(data, newFreq, newCommonFreq);
	}
    
    /**
     * Update products defines similarity between products (by looking at consecutive programes)
     */
    @Override
    public Commitable updateProducts(UpdateProductsDelta delta) {
        return updateFrequencies(delta.dataStore, delta.newProducts);
    }
    
    /**
     * Compares products by their start time
     *
     */
    private class ProductsComparator implements Comparator<ProductData> {
        public int compare(ProductData p1, ProductData p2) {
            final Attr startAttr1 = p1.getAttribute(START_ATTRIBUTE);
            final Attr startAttr2 = p2.getAttribute(START_ATTRIBUTE);
            if (startAttr1 == null || startAttr2 == null)
                return 0;
            // the long result of subtraction may overflow int, so we do it by comparing values
            final long l1 = ((LongAttr)startAttr1).value;
            final long l2 = ((LongAttr)startAttr2).value;
            if (l1 < l2) return -1;
            if (l1 > l2) return 1;
            return 0;
        }
    }
    
    private class UpdateSimilarity implements Commitable {
        final DataStore newData;
        final TIntIntMap newFreq;
        final Map<Integer, TIntIntMap> newCommonFreq;
        
        UpdateSimilarity(final DataStore newData, final TIntIntMap newFreq, final Map<Integer, TIntIntMap> newCommonFreq)
        {
            this.newData = newData;
            this.newFreq = newFreq;
            this.newCommonFreq = newCommonFreq;
        }
        
        @Override
        public void commit() {
            data = newData;
            freq = newFreq;
            commonFreq = newCommonFreq;
        }     
    }
}


