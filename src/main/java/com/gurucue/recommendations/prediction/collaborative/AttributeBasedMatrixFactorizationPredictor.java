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
package com.gurucue.recommendations.prediction.collaborative;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntFloatIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.TByteList;
import gnu.trove.list.TFloatList;
import gnu.trove.list.TIntList;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.TIntSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.prediction.Predictor;
import com.gurucue.recommendations.prediction.ProductRating;
import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.EmptyCommit;
import com.gurucue.recommendations.recommender.ProductPair;
import com.gurucue.recommendations.recommender.dto.Attr;
import com.gurucue.recommendations.recommender.dto.ConsumerRatingsData;
import com.gurucue.recommendations.recommender.dto.ContextDiscretizer;
import com.gurucue.recommendations.recommender.dto.DataStore;
import com.gurucue.recommendations.recommender.dto.ProductData;
import com.gurucue.recommendations.recommender.dto.DataStore.UpdateIncrementalData;


class MF implements Serializable {
	private static final long serialVersionUID = 8563989867626622801L;
	
	private static final int MAXITERATIONS = 1000; 
	
	double [][] p, q;
	TLongIntMap productMap;
	TIntIntMap consumerMap;
	int k;
	
	MF(int k, TIntIntMap consumerMap, TLongIntMap productMap)
	{
		this.productMap = productMap;
		this.consumerMap = consumerMap;
		this.k = k;
		
		p = new double [consumerMap.size()][k];
		q = new double [k][productMap.size()];
	}
	
	/**
	 * Initialize p and q matrix with random values. 
	 */
	void initializeValues()
	{
		Random rand = new Random();
		
		// initialize p - for consumers;
		for (int i = 0; i < p.length; i++)
			for (int j = 0; j < k; j++)
			{
				// fix first column of p to 1
				if (j == 0)
					p[i][j] = 1;
				else
					p[i][j] = rand.nextFloat()-0.5;
			}
		// initialize p - for items;
		for (int i = 0; i < q.length; i++)
			for (int j = 0; j < k; j++)
			{
				// fix second row of q to 1.0 - deprecated
				q[j][i] = rand.nextFloat()-0.5;
			}
	}

	private float computeError(final TIntList indices, final int iteration, final TFloatList newRatings, final TIntList newConsumers, 
							   final ArrayList<TLongList> productTranslation)
	{
		float error = 0;
		for (TIntIterator it = indices.iterator(); it.hasNext(); )
		{
			final int index = it.next();

			final float rating = newRatings.get(index);
			final int consumer = newConsumers.get(index);
			final TLongList products = productTranslation.get(index);
			
			// loop over all product translations and update
			for (TLongIterator itp = products.iterator(); itp.hasNext(); )
			{
				final long prod = itp.next();
				
				// compute predicted rating
				final int ci = consumerMap.get(consumer);
				final int pi = productMap.get(prod);					
				final float rating_predicted = predict(ci, pi);
				// compute error
				error += (rating - rating_predicted) * (rating - rating_predicted);
			}
		}
		return error;
	}
	/**
	 * Optimize parameters in p and q matrices
	 * 
	 * @param newRatings ratings
	 * @param newConsumers consumers
	 * @param productTranslation product translation (each product is translated to several values in the q matrix)
	 * @param eps stopping criteria in optimization
	 * @param lambda regularization parameter
	 * @param rate learning rate
	 */
	public void optimize(TByteList newRatings, TIntList newConsumers,
			ArrayList<TLongList> productTranslation, float eps, float lambda, float rate) {
		Random rand = new Random();
		TIntList indices = new TIntArrayList();
		for (int i = 0; i < newRatings.size(); i++)
			indices.add(i);
		// subtract averages from newRatings
		TIntFloatMap consAverages = new TIntFloatHashMap();
		TIntIntMap counter = new TIntIntHashMap();
		final int s = newConsumers.size();
		for (int i = 0; i < s; i++)
		{
			consAverages.put(newConsumers.get(i), consAverages.get(newConsumers.get(i)) + newRatings.get(i));
			counter.put(newConsumers.get(i), counter.get(newConsumers.get(i)) + 1);
		}
		for (TIntFloatIterator it = consAverages.iterator(); it.hasNext(); )
		{
			it.advance();
			final int key = it.key();
			final float value = it.value();
			it.setValue(value / counter.get(key));
		}
		// create new ratings
		TFloatList normRatings = new TFloatArrayList();
		for (int i = 0; i < s; i++)
		{
			normRatings.add((newRatings.get(i) - consAverages.get(newConsumers.get(i))));
		}		
		// Optimization!
		float olderror = 1000 * newRatings.size();
		for (int iteration = 0; iteration < MAXITERATIONS; iteration++)
		{
			// create shuffled indices
			indices.shuffle(rand);
			
			// loop over shuffled indices and update p and q values
			float sumerror = 0;
			//System.out.println("indices: " + indices.size());
			// compute pre-error
			final float preerror = computeError(indices, iteration, normRatings, newConsumers, productTranslation);
			for (TIntIterator it = indices.iterator(); it.hasNext(); )
			{
				final int index = it.next();

				final float rating = normRatings.get(index);
				final int consumer = newConsumers.get(index);
				final TLongList products = productTranslation.get(index);
				
				// loop over all product translations and update
				for (TLongIterator itp = products.iterator(); itp.hasNext(); )
				{
					final long prod = itp.next();
					
					// compute predicted rating
					final int ci = consumerMap.get(consumer);
					final int pi = productMap.get(prod);					
					final float rating_predicted = predict(ci, pi);
					// compute error
					final float error = rating - rating_predicted;
					sumerror += error * error;
					
					// update p
					for (int j = 0; j < k; j++)
					{
						// fix first column of p to 1
						if (j == 0)
							continue;
						else
							p[ci][j] = p[ci][j] + rate * (error * q[j][pi] - lambda * p[ci][j]);
					}
					// update q
					for (int j = 0; j < k; j++)
					{
						// fix second row of q to 1.0 - deprecated since averages are now subtracted
						q[j][pi] = q[j][pi] + rate * (error * p[ci][j] - lambda * q[j][pi]);
					}					
				}
			}
			// compute post-error
			final float posterror = computeError(indices, iteration, normRatings, newConsumers, productTranslation);
			
			// if overall change between of p and q values is less than eps, stop learning
			if (preerror - posterror < eps)
				break;
			olderror = sumerror;
		}
	}
	
	/**
	 * Predicts rating a consumer would give to a list of products. Predicts average.
	 * 
	 * @param consumer
	 * @param products
	 * @return
	 */
	public Double predict(int consumer, TLongList products)
	{
		if (!consumerMap.containsKey(consumer))
			return null;
		final int cons = consumerMap.get(consumer);
        
		
		boolean any = false;
		for (TLongIterator it = products.iterator(); it.hasNext(); )
		{
			final long prod = it.next();
			if (productMap.containsKey(prod))
				any = true;
		}
		if (!any)
			return null;
		
		double prediction = 0;
		int counter = 0;
		for (TLongIterator it = products.iterator(); it.hasNext(); )
		{
			final long prod = it.next();
			if (productMap.containsKey(prod))
			{
				final int pi = productMap.get(prod);
				counter++;
				prediction += predict(cons, pi);
			}
		}
		return prediction/counter;
	}
	
	
	private float predict(int ci, int pi)
	{
		float s = 0;
		for (int i = 0; i < k; i++)
		{
			s += p[ci][i] * q[i][pi];
		}
		return s;
	}
}


/**
 * Matrix factorization based on attribute data (an item is a cartesian product of attributes) and on context. A ratings type of event is required.  Attributes describing a product should 
 * be sorted given according to the importance of attributes (e.g., videoid, series, genre, actor ...) 
 * 
 * Ratings are assumed to be between 0 and 100
 * 
 */
public class AttributeBasedMatrixFactorizationPredictor extends Predictor {
    private final Logger logger;

    // name of events
    protected final String RATINGSNAME;
    
    // time between two model updates in seconds
    protected final long UPDATE_TIME_WINDOW; 
    
    // number of stored models;
    protected final int NMODELS;
    
    // number of elements in a circular queue
    protected final int NELEMENTS;
    
    // number of attributes will never exceed this value
    private final int MAX_ATTR_VALUES;
    // number of contexts will never exceed this value
    private final int MAX_CONTEXTS;    
    // date id in meta data
    private final int TIMESTAMP_ID;
    
    // number of hidden features
    private final int K;
    // minimal change to proceed with optimization
    private final float EPS;
    // regularization parameter in optimization
    private final float LAMBDA;
    // learning rate in optimization
    private final float LEARNING_RATE;

    
    private final int [] ATTRIBUTES;    
    private final int [] EXPLAINABLE_ATTRIBUTES;    
    
    private DataStore data; // the data source
    
    private TByteList ratings;
    private TIntList consumers;
    private TIntList products;
    private TLongList dates; // dates of ratings (in seconds since epoch)
    
    // models
    List<MF> models;
    
    // last update timestamp
    long lastUpdateTime;
    
    // context handler
    ContextDiscretizer contextHandler;
    
    
    
    public AttributeBasedMatrixFactorizationPredictor(String name, Settings settings) throws CloneNotSupportedException {
    	
        super(name, settings);
        logger = LogManager.getLogger(AttributeBasedMatrixFactorizationPredictor.class.getName() + "[REC " + settings.getRecommenderId() + "]");
        
        RATINGSNAME = settings.getSetting(name + "_RATINGSNAME");
        UPDATE_TIME_WINDOW = settings.getSettingAsLong(name + "_UPDATE_TIME_WINDOW");
        NMODELS = settings.getSettingAsInt(name + "_NMODELS");
        NELEMENTS = settings.getSettingAsInt(name + "_NELEMENTS");
        MAX_ATTR_VALUES = settings.getSettingAsInt(name + "_MAX_ATTR_VALUES");
        MAX_CONTEXTS = settings.getSettingAsInt(name + "_MAX_CONTEXTS");
        ATTRIBUTES = settings.getAsIntArray(name + "_ATTRIBUTES");
        EXPLAINABLE_ATTRIBUTES = settings.getAsIntArray(name + "_EXPLAINABLE_ATTRIBUTES");
        TIMESTAMP_ID = settings.getSettingAsInt(name + "_TIMESTAMP_ID");
        K = settings.getSettingAsInt(name + "_K");
        EPS = settings.getSettingAsFloat(name + "_EPS");
        LAMBDA = settings.getSettingAsFloat(name + "_LAMBDA");
        LEARNING_RATE = settings.getSettingAsFloat(name + "_LEARNING_RATE");
        
        lastUpdateTime = 0;
        
        // initialize context handler
        contextHandler = new ContextDiscretizer(name, settings);        
    }
    

    @Override
    public void getPredictions(List<ProductRating> predictions, Map<String, String> tags) {
        int consumerIndex = -1;
        for (ProductRating pr : predictions)
        {
            if (consumerIndex < 0)
            {
                consumerIndex = pr.getConsumerIndex();
            }
            final int productIndex = pr.getProductIndex();
            
            TIntList context = contextHandler.getContext(data, productIndex, tags);
            final TLongList attrValues = getAttrValues(productIndex, context);
            
            // iterate over models 
            double sumweights = 0;
            double sumpredictions = 0;
            StringBuilder explanation = new StringBuilder();
            for (int i = models.size()-1; i >= 0; i--) 
            {
            	// make a prediction with the model
            	MF model = models.get(i);
            	
            	// iterate over attributes until a prediction is made
            	Double prediction = null;
            	for (int ai = 0; ai < ATTRIBUTES.length; ai++)
            	{
            		// create attrValues from this attribute
            		TLongList attrValuesThis = new TLongArrayList();
            		for (TLongIterator it = attrValues.iterator(); it.hasNext(); )
            		{
            			final long key = it.next();
            			if (key / (MAX_ATTR_VALUES * MAX_CONTEXTS) == ai)
            				attrValuesThis.add(key);
            		}
            		
            		// make a prediction for this consumer and these values
            		prediction = model.predict(consumerIndex, attrValuesThis);
            		
            		if (prediction != null)
            		{
            			// add explanation if one of the last five models
            			if (i > models.size()-5)
            			{
            				explanation.append("matrix attr:");
            				explanation.append(ai);
            				explanation.append(",");
            				explanation.append(prediction);
            				explanation.append(";");
            			}
            			break;
            		}
            	}
            	
            	if (prediction == null) // do not consider this model
            		continue;
            	
            	// compute model weight
            	final double weight = Math.exp(-(models.size()-1-i));
            	sumweights += weight;
            	sumpredictions += weight * prediction;
            }
            if (sumweights < 1e-6)
            	pr.setPrediction(0, 0, this.ID, "");
            else
            	pr.setPrediction(sumpredictions/sumweights, (float) Math.min(sumweights, 1.0), this.ID, explanation.toString());
        }
    
    }
    
    @Override
    public void getSimilarProducts(TIntSet productIndices, List<ProductRating> predictions, int id_shift, Map<String,String> tags) {
    	return;
    }
    
    @Override
    public Commitable updateModelIncremental(UpdateIncrementalData updateData) {

        logger.info("Update matrix-factorization collaborative started.");
        if (this.data == null) // initialization is needed
        {
            this.data = updateData.dataStore;
            ratings = new TByteArrayList();
            consumers = new TIntArrayList();
            products = new TIntArrayList();
            dates = new TLongArrayList();
        }
        
        // reload data
        TByteList newRatings = new TByteArrayList(ratings);
        TIntList newConsumers = new TIntArrayList(consumers);
        TIntList newProducts = new TIntArrayList(products);
        TLongList newDates = new TLongArrayList(dates);
        final int eventIndex = data.getEventsDescriptor(RATINGSNAME).index;

        // new (changed) consumers
        final List<ConsumerData> tmp_consumers = updateData.newData;
        final TLongIntHashMap tmp_ids = updateData.newConsumerIDs;
        
        for (ConsumerData c: tmp_consumers)
        {
            if (null == c) // no events were added to this user
                continue;
            
            // are there any new events for this user, otherwise continue
            if (null == c.events[eventIndex])
                continue;

            final int consIndex = tmp_ids.get(c.consumerId);
            final int [] tmp_items = c.events[eventIndex].getProductIndices();
            final byte [] tmp_ratings = ((ConsumerRatingsData) c.events[eventIndex]).ratings;
            final long[] tmp_dates = c.events[eventIndex].getMeta().getLongValuesArray(TIMESTAMP_ID);            
            for (int i = 0; i < tmp_items.length; i++)
            {
            	newRatings.add(tmp_ratings[i]);
            	newConsumers.add(consIndex);
            	newProducts.add(tmp_items[i]);
            	newDates.add(tmp_dates[i]);
            }
        }
        
        // remove excessive data
        final int size = newRatings.size();
        if (size > NELEMENTS)
        {
        	newRatings.remove(0, size - NELEMENTS);
        	newConsumers.remove(0, size - NELEMENTS);
        	newProducts.remove(0, size - NELEMENTS);
        	newDates.remove(0, size - NELEMENTS);
        }
        
        // update matrix factorization model
        final long lastRead = data.getEventsDescriptor(RATINGSNAME).last_read_date;
        ArrayList<MF> newModels = new ArrayList<MF> ();
        logger.info("last read: " + lastRead);
        logger.info("last update: " + lastUpdateTime);
        if (lastRead - lastUpdateTime > UPDATE_TIME_WINDOW) {
        	logger.info("Learning new model.");
        	// conduct update
        	lastUpdateTime = lastRead;

        	// create a new MF and add it to the set of models.
        	final TIntIntMap consMap = createConsumersMap(newConsumers);
        	final TLongIntMap prodMap = createProductsMap(newProducts, newDates);
        	logger.info("Cons size: " + consMap.size());
        	logger.info("Prod size: " + prodMap.size());
        	if (consMap.size() > 100 && prodMap.size() > 100)
        	{
	        	MF newModel = new MF(K, consMap, prodMap);
	        	newModel.initializeValues();
	        	// for each product in newProducts prepare and translation list, that is all values to which this product translates to
	        	ArrayList<TLongList> productTranslation = createProductTranslations(newProducts, newDates);
	        	newModel.optimize(newRatings, newConsumers, productTranslation, EPS, LAMBDA, LEARNING_RATE);
	        	newModels.add(newModel);
        	}
        }
        else
        	logger.info("Skipping learning new model.");

        return new UpdateDelta(newRatings, newConsumers, newProducts, newDates, newModels);
    }
    
    private TIntIntMap createConsumersMap(TIntList consumers)
    {
    	// create a consumer map
    	TIntIntMap consMap = new TIntIntHashMap();
    	int counter = 0;
    	for (TIntIterator it = consumers.iterator(); it.hasNext(); )
    	{
    		final int cons = it.next();
    		if (!consMap.containsKey(cons))
    		{
    			consMap.put(cons, counter);
    			counter++;
    		}
    	}
    	return consMap;
    }
    
    private ArrayList<TLongList> createProductTranslations(final TIntList products, final TLongList dates)
    {
    	ArrayList<TLongList> productsTranslations = new ArrayList<TLongList> ();
    	final int size = products.size();
    	for (int i = 0; i < size; i++)
    	{
    		final int prod = products.get(i);
    		final long date = dates.get(i);
    		// create a list of all relevant product ids (given attributes and context)

    		TIntList contexts = contextHandler.getContext(data, prod, date);
    		TLongList productKeys = getAttrValues(prod, contexts);
    		
    		productsTranslations.add(productKeys);
    	}
    	return productsTranslations;
    }
    
    private TLongIntMap createProductsMap(final TIntList products, final TLongList dates)
    {
    	// create a product map
    	TLongIntMap productsMap = new TLongIntHashMap();
    	int counter = 0;
    	final int size = products.size();
    	for (int i = 0; i < size; i++)
    	{
    		final int prod = products.get(i);
    		final long date = dates.get(i);
    		// create a list of all relevant product ids (given attributes and context)

    		TIntList contexts = contextHandler.getContext(data, prod, date);
    		TLongList productKeys = getAttrValues(prod, contexts);
    		
    		for (TLongIterator itkey = productKeys.iterator(); itkey.hasNext(); )
    		{
    			final long key = itkey.next();
        		if (!productsMap.containsKey(key))
        		{
        			productsMap.put(key, counter);
        			counter++;
        		}
    		}
    	}
    	return productsMap;
    }
    
    
    TLongList getAttrValues(final int productIndex, final TIntList contexts)
    {
        TLongList values = new TLongArrayList();
        if (productIndex == -1)
            return values;
        final ProductData pd = data.getProductByIndex(productIndex);
        int counter = 0;
        for (int attr : ATTRIBUTES)
        {
            final Attr a = pd.getAttribute(attr);
            if (a == null)
            {
            	counter ++;
                continue;
            }
            final TIntSet avals = a.getValues();
            if (avals == null)
            {
            	counter ++;
                continue;
            }
            for (TIntIterator it = avals.iterator(); it.hasNext(); )
            {
            	final int val = it.next();
            	final int basic_att_val = val%MAX_ATTR_VALUES + MAX_ATTR_VALUES * counter;
                if (val > MAX_ATTR_VALUES)
                	logger.warn("Attribute index " + val + " is larger than MAX_ATTR_VALUES");

                for (TIntIterator itc = contexts.iterator(); itc.hasNext(); )
            	{
            		final int cont = itc.next();
                    values.add(cont + basic_att_val * MAX_CONTEXTS);
            	}
            	
            }
            counter ++;
        }
        return values;
    }    
    
    @Override
    public Commitable updateConsumer(final DataStore.UpdateData updateData) {
        return EmptyCommit.INSTANCE;
    }
    
    @Override
    public void updateModel(DataStore data) {
        logger.warn("Update model does nothing for the Matrix-factorization collaborative predictor.");
    }

    @Override
    public void updateModelQuick(DataStore data) {
        logger.error("Quick update not implemented for AttributeBasedMatrixFactorizationPredictor.");
    }
    
    @Override
    public ProductPair getBestPair(int consumerIndex) {
        logger.error("Get best pair not implemented for AttributeBasedMatrixFactorizationPredictor.");
        return null;
    }

    @Override
    public boolean needsProfiling(int consumerIndex) {
        logger.error("Needs profiling not implemented for AttributeBasedMatrixFactorizationPredictor.");
        return false;
    }

    @Override
    public Commitable updateProducts(DataStore.UpdateProductsDelta delta) {
        // update product indices in products array
        TIntList newProducts = new TIntArrayList();
        for (TIntIterator it = products.iterator(); it.hasNext(); )
        {
            int index = it.next();
            if (index == -1) // unexisting product; kept only for the sake of consistency
            {
                newProducts.add(-1);
                continue;
            }

            long id = data.getProductByIndex(index).productId;
            int newIndex = delta.newProductIDs.get(id);
            if (newIndex == -1)
            {
                logger.warn("Id of product " + id + " not found, setting -1 instead. ");
            }

            newProducts.add(newIndex);
        }

        return new UpdateProducts(newProducts);
    }
    
	@Override
	public void serialize(ObjectOutputStream out) throws IOException {
		out.writeObject(ratings);
		out.writeObject(consumers);
		out.writeObject(products);
		out.writeObject(dates);
		out.writeObject(models);
	}


	@Override
	public Commitable updateModelFromFile(ObjectInputStream in, DataStore data)
			throws ClassNotFoundException, IOException {
        if (this.data == null) // initialization is needed
        {
            this.data = data;
            ratings = new TByteArrayList();
            consumers = new TIntArrayList();
            products = new TIntArrayList();
            dates = new TLongArrayList();
            models = new ArrayList<MF>();
        }
        
        // reload data
        TByteList newRatings = (TByteList) in.readObject();
        TIntList newConsumers = (TIntList) in.readObject();
        TIntList newProducts = (TIntList) in.readObject();
        TLongList newDates = (TLongList) in.readObject();
        @SuppressWarnings("unchecked")
		ArrayList<MF> newModels = (ArrayList<MF>) in.readObject();

        // similarities and indices
		return new UpdateDelta(newRatings, newConsumers, newProducts, newDates, newModels);
	}       
    
    private class UpdateProducts implements Commitable {
        TIntList newProducts;

        UpdateProducts(TIntList newProducts)
        {
            this.newProducts = newProducts;
        }

        @Override
        public void commit() {
            products = newProducts;
        }
    }
    
    private class UpdateDelta implements Commitable {
        TByteList newRatings;
        TIntList newConsumers;
        TIntList newProducts;
        TLongList newDates;
        ArrayList<MF> newModels;
        
        UpdateDelta(TByteList newRatings, TIntList newConsumers, TIntList newProducts, TLongList newDates, ArrayList<MF> newModels) {
        	this.newRatings = newRatings;
        	this.newConsumers = newConsumers;
        	this.newProducts = newProducts;
        	this.newDates = newDates;
        	this.newModels = newModels;
        }

        @Override
        public void commit() {
        	ratings = newRatings;
        	consumers = newConsumers;
        	products = newProducts;
        	dates = newDates;
        	
        	if (models == null)
        		models = newModels;
        	else
	        	for (MF model : newModels) {
	        		models.add(model);
	            	if (models.size() > NMODELS)
	            		models.remove(0);
	        	}
        	
        }
    }
}
