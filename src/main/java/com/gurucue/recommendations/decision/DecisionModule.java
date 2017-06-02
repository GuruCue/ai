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

import gnu.trove.iterator.TFloatIterator;
import gnu.trove.list.TFloatList;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.TagsManager;

/**
 * Abstract class for all decision modules that 
 * select a set of promising products out of a set of 
 * products with highest estimated recommendation scores.
 */
public abstract class DecisionModule {
    private final Logger logger =
        LogManager.getLogger(DecisionModule.class.getName());
    protected DataStore data;
    protected Settings settings;
    protected String name;
    final int ID;
    
    public DecisionModule()
    {
        ID = -1;
    }
    
    public DecisionModule(String name, Settings settings)
    {
        this.name = name;
        this.settings = settings;
        this.ID = settings.getSettingAsInt(this.name + "_ID");
    }
    
    public ProductRating[] selectBestCandidatesTime(List<ProductRating> candidates, int maxCandidates, boolean randomizeResults, Map<String,String> tags)
    {
    	final long start = System.currentTimeMillis();
    	final ProductRating[] result = selectBestCandidates(candidates, maxCandidates, randomizeResults, tags);
    	final long totalTime = System.currentTimeMillis() - start;
    	tags.put("time_decision_"+ID, String.valueOf(totalTime));
    	return result;
    }

    /**
     * Returns a list of recommended items built from the provided list of candidates.
     * 
     * @param candidates the <b>sorted</b> list of candidates
     * @param maxCandidates the max. number of candidates to return
     * @param randomizeResults Return results in a <b>random</b> order?
     * @param tags arbitrary parameters sent to decision module
     * @return the list of recommended items
     */
   public abstract ProductRating[] selectBestCandidates(List<ProductRating> candidates, int maxCandidates, boolean randomizeResults, Map<String,String> tags);
   
   /**
    * A method that can help implementing a decision maker. Candidates are acceptable candidates (all good enough to be recommendable), 
    * a weight array that defines a "quality" value (non-negative) for each candidate, number of returned candidates and whether it should return randomized results or not. 
    * @param candidates
    * @param weights
    * @param maxCandidates
    * @param randomizeResults
    * @return
    */
   public ProductRating[] selectSubset(List<ProductRating> candidates, TFloatList weights, int maxCandidates, boolean randomizeResults)
   {
       // Random generator
       Random gen = new Random();

       ArrayList<CandidateScore> cscores = new ArrayList<CandidateScore> ();
       Iterator<ProductRating> citer = candidates.iterator();
       TFloatIterator witer = weights.iterator();
       // add to cscores all ProductRatings with randomscores multplied by weights
       while (citer.hasNext())
       {
           if (randomizeResults)
               cscores.add(new CandidateScore(citer.next(), witer.next(), gen));
           else
               cscores.add(new CandidateScore(citer.next(), witer.next()));
       }

       // now simply sort the cscores
       Collections.sort(cscores);
       
       // return an array of ProductRatings
       ProductRating [] res = new ProductRating [maxCandidates];
       for (int i=0; i<maxCandidates; i++)
       {
           res[i] = cscores.get(i).pr;
       }
       return res;
   }
   
   // for "randomly" resorting
   private class CandidateScore implements Comparable<CandidateScore>{
       public ProductRating pr;
       public float score;
       
       CandidateScore(ProductRating pr, float weight)
       {
           this.pr = pr;
           this.score = weight;
       }

       CandidateScore(ProductRating pr, float weight, Random gen)
       {
           this.pr = pr;
           this.score = (float) (gen.nextDouble() * weight);
       }

       @Override
       public int compareTo(CandidateScore candidate) {
           float dif = this.score - candidate.score;
           if (dif > 0)
               return -1;
           if (dif < 0)
               return 1;
           return 0;
       }

       @Override
       public boolean equals(Object o) {
           if (o instanceof CandidateScore) {
               CandidateScore c = (CandidateScore) o;
               if (this.score == c.score) {
                   return true;
               }
           }
           return false;
       }
       
   }
   
   public String [] getProductTags(Map<String,String> tags)
   {
       String pt = tags.get("PRODUCT_TAGS");
       if (pt != null && !pt.equals(""))
       {
           return pt.split(";");
       }
       return null;
   }
   
   /**
    * Makes an incremental update of a decision model. 
    * 
    * Important: it needs to update the data store. 
    * 
    * @param updateData
    * @return
    * @throws CloneNotSupportedException 
    */
   public abstract Commitable updateModelIncremental(DataStore.UpdateIncrementalData data);
   
   /**
    * Update to the products in the decision model.
    * 
    * Important: it needs to update the data store. 
    * 
    * @param data
    * @return
    */
   public abstract Commitable updateProducts(DataStore.UpdateProductsDelta delta);
   
   
   class DoNothingUpdate implements Commitable
   {
       private DataStore tmp_data;
       
       DoNothingUpdate(DataStore tmp_data)
       {
           this.tmp_data = data;
       }
       
       @Override
       public void commit() {
           data = tmp_data;
       }
   }
   
   /**
    * Creates a new instance of the predictor (clones only settings, but not learned values)
    * @return
    * @throws SecurityException 
    * @throws NoSuchMethodException 
    * @throws InvocationTargetException 
    * @throws IllegalArgumentException 
    * @throws IllegalAccessException 
    * @throws InstantiationException 
    */
   public DecisionModule createEmptyClone() throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
   {
       Constructor<? extends DecisionModule> constructor = this.getClass().getConstructor(String.class,Settings.class);  
       return constructor.newInstance(new Object[] {name, settings});
   }
   
   /**
    * Serializes a model into an output stream
    * @param out
    * @return
    * @throws IOException 
    */
   public abstract void serialize(ObjectOutputStream out) throws IOException;

   /**
    * Updates the model by reading it from file. 
    * @param in
    * @return
    * @throws IOException 
    * @throws ClassNotFoundException 
    */
   public abstract Commitable updateModelFromFile(ObjectInputStream in, DataStore data) throws ClassNotFoundException, IOException;   
   
   
   protected class ProductAdder
   {
       final int title_length; 
       final int title_attribute; // to check if the same title is already recommended
       final Map<String,String> tags; // tags specifications for recommendation

	   final String [] productTags;
       final TIntList maxCounts;
       TIntList trueCounts;

	   final String [] productTags_secondary;
       final TIntList maxCounts_secondary;
       TIntList trueCounts_secondary;
       
       //  if recommendations require pretty explanations; add forced neglects this condition
       final boolean require_explanation; 
       
       List<ProductRating> recommended;
       TIntSet recommendedSet; // need this set to avoid recommending the same item several times;
       Set<String> recommendedSetTitle;
       
       
       ProductAdder(Map<String,String> tags, int title_attribute, int title_length)
       {
    	   this(tags, title_attribute, title_length, false);
       }
       
       ProductAdder(Map<String,String> tags, int title_attribute, int title_length, boolean require_explanation)
       {
    	   this.tags = tags;
           this.title_attribute = title_attribute;
           this.title_length = title_length;
           this.require_explanation = require_explanation;
           
           // initialize primary constraints
           String pt = tags.get(TagsManager.PRIMARY_TAG);
           if (pt != null && !pt.equals(""))
           {
               productTags = pt.split(";");
               // for each tag read max percentage
               maxCounts = new TIntArrayList();
               trueCounts = new TIntArrayList();
               for (String t : productTags)
               {
                   if (tags.containsKey("MAX_ITEMS_"+t))
                       maxCounts.add(Integer.valueOf(tags.get("MAX_ITEMS_"+t)));
                   else
                       maxCounts.add(Integer.valueOf(tags.get(TagsManager.MAX_RECOMMEND_TAG)));
                   trueCounts.add(0);
               }
           }
           else
           {
               productTags = null;
               maxCounts = null;
           }
           
           // initialize secondary constraints
           pt = tags.get(TagsManager.SECONDARY_TAG);
           if (pt != null && !pt.equals(""))
           {
        	   productTags_secondary = pt.split(";");
               // for each tag read max percentage
        	   maxCounts_secondary = new TIntArrayList();
        	   trueCounts_secondary = new TIntArrayList();
               for (String t : productTags_secondary)
               {
            	   maxCounts_secondary.add(Integer.valueOf(tags.get("MAX_ITEMS_"+t)));
            	   trueCounts_secondary.add(0);
               }
           }
           else
           {
        	   productTags_secondary = null;
        	   maxCounts_secondary = null;
           }           
           
           recommended = new ArrayList<ProductRating> ();
           recommendedSet = new TIntHashSet();
           recommendedSetTitle = new HashSet<String>();
       }
       
       /**
        * Is product already in the recommended set?
        * @param pr
        * @return
        */
       boolean contains(ProductRating pr)
       {
    	   final String title = getTitle(pr);
    	   if (title == null)
    		   return true;
    	   if (recommendedSetTitle.contains(title))
    		   return true;
    	   return false;
       }
       
       /**
        * Gets product title.
        * @param pr
        * @return
        */
       String getTitle(ProductRating pr)
       {
    	   final Object to = pr.getProductData().getAttribute(title_attribute);
    	   if (to == null)
    		   return "";
    	   final String title = to.toString();
    	   if (title_length < 0)
    		   return title;
    	   return title.substring(0, Math.min(title_length, title.length()));
       }
       
       void addTitle(ProductRating pr)
       {
    	   recommendedSetTitle.add(getTitle(pr));
       }
       
       boolean checkTags(final String [] tags, final TIntList mCounts, final TIntList tCounts, ProductRating pr)
       {
           if (tags != null)
               for (int i = tags.length-1; i >= 0; i--)
               {
                   // does it have this tag?
                   if (pr.getTags().containsKey(tags[i]))
                   {
                       if (tCounts.get(i) >= mCounts.get(i))
                       {
                          return false;
                       }
                   }
               }
           return true;
       }
       
       void updateTags(ProductRating pr)
       {
           if (productTags != null)
               for (int i = productTags.length-1; i >= 0; i--)
               {
                   // does it have this tag?
                   if (pr.getTags().containsKey(productTags[i]))
                   {
                       trueCounts.set(i, trueCounts.get(i)+1);
                   }
               }
           if (productTags_secondary != null)
               for (int i = productTags_secondary.length-1; i >= 0; i--)
               {
                   // does it have this tag?
                   if (pr.getTags().containsKey(productTags_secondary[i]))
                   {
                	   trueCounts_secondary.set(i, trueCounts_secondary.get(i)+1);
                   }
               }           
       }
       
       /**
        * Adds the product if none of its tags is full yet. Strict add considers also secondary tags.
        * @return true if successfully added, otherwise false
        */
       boolean addStrict(ProductRating pr)
       {
    	   if (require_explanation && pr.getPrettyExplanations().isEmpty())
    		   return false;
           if (contains(pr))
               return false;
           if (!checkTags(productTags, maxCounts, trueCounts, pr))
        	   return false;
           if (!checkTags(productTags_secondary, maxCounts_secondary, trueCounts_secondary, pr))
        	   return false;

           recommended.add(pr);
           recommendedSet.add(pr.getProductIndex());
           addTitle(pr);
           
           // add to tags
           updateTags(pr);

           return true;
       }       
       
       /**
        * Adds the product if none of its tags is full yet.
        * @return true if successfully added, otherwise false
        */
       boolean add(ProductRating pr)
       {
           if (contains(pr))
               return false;
           if (!checkTags(productTags, maxCounts, trueCounts, pr))
           {
        	   return false;
           }

           recommended.add(pr);
           recommendedSet.add(pr.getProductIndex());
           addTitle(pr);
           // add to tags
           updateTags(pr);

           return true;
       }
       
       /**
        * Adds a product without considering product tags.
        * 
        * @param pr
        */
       void addForced(ProductRating pr)
       {
           if (contains(pr))
               return;
           recommended.add(pr);
           recommendedSet.add(pr.getProductIndex());
           addTitle(pr);
           // add to tags
           updateTags(pr);
       }
       
       /**
        * Can we add the product to the recommended set? Only the basic tags are considered. 
        * @param pr
        * @return
        */
       boolean can_add(ProductRating pr)
       {
           if (recommendedSet.contains(pr.getProductIndex()))
               return false;           
           if (!checkTags(productTags, maxCounts, trueCounts, pr))
        	   return false;
           return true;
       }
       
       ProductRating [] getRecommended()
       {
    	   // sort first according to tag ranks
    	   Collections.sort(recommended, new TagRankComparator(tags));
           return recommended.toArray(new ProductRating[0]);
       }
       
       int size()
       {
           return recommended.size();
       }
   }
   
   private class TagRankComparator implements Comparator<ProductRating>  
   {
	   private final Map<String,String> tags;
       private final String[] productTags;

	   public TagRankComparator(final Map<String,String> tags)
       {
           this.tags = tags;
           String pt = tags.get("PRODUCT_TAGS");
           if (pt != null && !pt.equals(""))
           {
               productTags = pt.split(";");
           }
           else
        	   productTags = null;
       }
       
       private double computeRank(ProductRating pr)
       {
    	   if (productTags == null || productTags.length == 0)
    		   return 0.0;
    	   double rankSum = 0;
    	   int counter = 0;
    	   for (String tag : productTags)
    	   {
    		   if (pr.getTags().containsKey(tag) && tags.containsKey("RANK_"+tag))
    		   {
    			   rankSum += Double.valueOf(tags.get("RANK_"+tag));
    			   counter ++;
    		   }
    	   }
    	   return rankSum/counter;
       }
       
       @Override
       public int compare(ProductRating o1, ProductRating o2) {
           final double dif = computeRank(o1) - computeRank(o2);
           if (dif < 0)
               return -1;
           if (dif > 0)
               return 1;
           return 0;    
       }    
   }   
}
