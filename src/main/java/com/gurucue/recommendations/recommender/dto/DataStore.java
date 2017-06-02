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
package com.gurucue.recommendations.recommender.dto;

import com.gurucue.recommendations.recommender.Commitable;
import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.reader.Reader;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TLongIntHashMap;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.entity.Product;

/**
* Contains data about products, consumers and ratings.
*
*/
public class DataStore {
    protected Logger logger;
  
	protected List<ProductData> productData; // data about learning products
	protected List<ConsumerData> consumerData; // data about consumers used in learning
    protected TLongIntHashMap consumerIDs; // map between IDs in database and indices in consumers array
    protected TLongIntHashMap productIDs; // map between IDs in the database and indices in the products array
    protected Domain domain; // domain describing attributes

	protected Reader reader; // database reader

	String [] eventsNames; // names of all types of events
    protected Map<String, EventsDataDescriptor> eventsDescriptors;
    
    boolean finishedReading; // all events were read?

    // constructor available only to be overridden in child classes
	protected DataStore() {
        logger = LogManager.getLogger(DataStore.class.getName() + "[ REC ?]");
    }
	
	protected DataStore(Reader reader, final List<ProductData> productData, final List<ConsumerData> consumerData, final TLongIntHashMap consumerIDs, final TLongIntHashMap productIDs, final Domain domain)
	{
        logger = LogManager.getLogger(DataStore.class.getName() + "[REC " + reader.getRecommenderId() + "]");
	    this.reader = reader;
	    initializeEvents(reader.getSettings());
	    
	    this.productData = productData;
	    this.consumerData = consumerData;
	    this.consumerIDs = consumerIDs;
	    this.productIDs = productIDs;
	    this.domain = domain;
	    
	    finishedReading = false;
	}
	

    public void serialize(ObjectOutputStream out) {
        try {
        	out.writeObject(productIDs);
        	
        	domain.serializeDomainAndProducts(productData, out);
        	
        	// write consumers
        	out.writeObject(consumerIDs);
        	out.writeObject(consumerData);
        	// write last read ids and dates
        	for (String e : eventsNames)
        	{
        		out.writeLong(eventsDescriptors.get(e).last_read_id);
        		out.writeLong(eventsDescriptors.get(e).last_read_date);
        	}
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    @SuppressWarnings("unchecked")
	public static DataStore deserialize(Reader reader, ObjectInputStream in)
    {
        try {
        	// read products
            final TLongIntHashMap productIDs = (TLongIntHashMap) in.readObject();
            
            List<ProductData> productData = new ArrayList<ProductData>();
            Domain newDomain = Domain.deserializeDomainAndProducts(productData, reader, in);

            
             // read consumers map
            final TLongIntHashMap consumerIDs = (TLongIntHashMap) in.readObject();
            // create empty consumers 
            List<ConsumerData> consumerData = (List<ConsumerData>) in.readObject();
            DataStore dt = new DataStore(reader, productData, consumerData, consumerIDs, productIDs, newDomain);

            // change last read id (so that it does not start from beginning)
            
            for (String e : dt.eventsNames)
            {
                long last_read_id = in.readLong();
                long last_read_date = in.readLong();
                dt.eventsDescriptors.get(e).last_read_id = last_read_id;
                dt.eventsDescriptors.get(e).last_read_date = last_read_date;
            }
            // return deserialized data store
            return dt;
            
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }
	
	public DataStore(Reader reader)
	{
        logger = LogManager.getLogger(DataStore.class.getName() + "[REC " + reader.getRecommenderId() + "]");
        logger.info("constructor start");
        this.reader = reader;
        updateData();
        logger.info("constructor end");
	}

    /**
     * Consumer update. Data (consumerData, productData, etc.) should not be null!
     * Precondition: only one thread calls this at a time. Perhaps declare it synchronized?
     */
    public UpdateData updateConsumer(final long consumerId)
    {
        logger.info("Updating consumer start");

        final ConsumerData newData = new ConsumerData(consumerId, eventsDescriptors.size());

        // read events related to this user
        reader.fillAConsumerWithEvents(newData, eventsDescriptors);

        final int consumerIndex = consumerIDs.get(consumerId);
        final ConsumerData oldData = consumerIndex < 0 ? null : consumerData.get(consumerIndex);

        //     compute basic stats
        computeDeltaStats(oldData, newData);

        logger.info("Updating consumer finished");
        return new UpdateData(this, oldData, newData, consumerIndex < 0 ? consumerData.size() : consumerIndex);
    }
    
    protected void initializeEvents(Settings settings)
    {
        // initialize events
        eventsNames = settings.getAsStringArray("EVENTS");
        eventsDescriptors = new HashMap<String, EventsDataDescriptor>(eventsNames.length);

        int i = 0;
        for (final String e: eventsNames)
        {
            final String type = settings.getSetting(e+"_TYPE");
            final String formula = settings.getSetting(e+"_FORMULA");
            final String[] meta = settings.getAsStringArray(e+"_META");
            final Long last_read_id = settings.getSettingAsLong(e+"_START_ID");
            final Integer batch_size = settings.getSettingAsInt(e+"_BATCH_SIZE");
            final int keep_in_memory_number = settings.getSettingAsInt(e + "_KEEP_CONSUMER_MEMORY_NUM");
            final Long keep_in_memory_time = settings.getSettingAsLong(e + "_KEEP_CONSUMER_MEMORY_TIME");
            eventsDescriptors.put(e, new EventsDataDescriptor(i, EventType.fromIdentifier(type).generator, this, meta, formula, (batch_size == null)?-1:batch_size, (last_read_id == null)?-1:last_read_id, -1, keep_in_memory_number, (keep_in_memory_time==null)?-1:keep_in_memory_time));
            i++;
        }        
    }
    
    /**
     * Updates all main variables (consumerData, productData, etc.) in data store.
     */
	protected void updateData()
	{
        logger.info("updateData start");
        // first create domain
        domain = reader.createDomain(); 
        		
	    // then read products;
        long start = System.currentTimeMillis();
        productData = reader.getProducts(domain);
        logger.info("Reading products took "+ (System.currentTimeMillis()-start) + " miliseconds.");
        // create a mapping from ids to indices
        final int prodLength = productData.size();
        productIDs = new TLongIntHashMap((int)Math.ceil(prodLength/0.8), (float)0.8, -1L, -1); // we want "null" values to be represented with negative values, other arguments are something arbitrary
        for (int i=0; i<prodLength; i++)
            productIDs.put(productData.get(i).productId, i);

        start = System.currentTimeMillis();
        // fill products with attributes AND create a mapping from attributeIDs to their indices
        //attributeIDs = 
        reader.fillProductsWithAttributes(productData, productIDs);
    	// testing: print 5398730

    	postProcessAttributes(productData);
    	// testing: print 5398730
        
        logger.info("Filling products with attributes took "+ (System.currentTimeMillis()-start) + " miliseconds.");
        
        // initialize settings
        final Settings settings = reader.getSettings(); // cache the Settings object for further processing
        
        initializeEvents(settings);

        // create a mapping for consumers
        consumerData = new ArrayList<ConsumerData>(30000);
        consumerIDs = new TLongIntHashMap(30000, (float)0.8, -1L, -1); // we want "null" values to be represented with negative values, other arguments are something arbitrary
        logger.info("updateData end");
	}
	
	/**
	 * Reads next batch from database. Before the update, if readNextBatch is run asynchronously, DataStore should be copied, otherwise multi-threading conflict could occur. 
	 */
	
    public UpdateIncrementalData readNextBatch()
	{
    	logger.info("Starting reading next batch");
        ArrayList<ConsumerData> newConsumerData = new ArrayList<ConsumerData>(consumerData.size());
        for (int i=consumerData.size(); i>0; i--)
            newConsumerData.add(null);
        TLongIntHashMap newConsumerIDs = new TLongIntHashMap(consumerIDs); 
        finishedReading = reader.batchFillConsumersWithEvents(consumerData, newConsumerData, newConsumerIDs, productIDs, eventsDescriptors);
        return new UpdateIncrementalData(this, newConsumerData, newConsumerIDs, eventsDescriptors, updateBasicStats(newConsumerData));
	}
    
    
    public UpdateProductsDelta updateProducts()
    {
    	// testing: print 5398730
        long start = System.currentTimeMillis();
        List<ProductData> newProductData = reader.getProducts(domain);
        logger.info("Reading products took "+ (System.currentTimeMillis()-start) + " miliseconds.");
        // create a mapping from ids to indices
        final int prodLength = newProductData.size();
        TLongIntHashMap newProductIDs = new TLongIntHashMap((int)Math.ceil(prodLength/0.8), (float)0.8, -1L, -1); // we want "null" values to be represented with negative values, other arguments are something arbitrary
        int product_counter=0;
        for (; product_counter<prodLength; product_counter++)
            newProductIDs.put(newProductData.get(product_counter).productId, product_counter);

        start = System.currentTimeMillis();
        // fill products with attributes AND create a mapping from attributeIDs to their indices
        //attributeIDs = 
        reader.updateProductsWithAttributes(newProductData, productData, newProductIDs, productIDs);
        logger.info("Updating products with attributes took "+ (System.currentTimeMillis()-start) + " miliseconds.");        
        
        // compute new product indices given new productIDs;
        ArrayList<ArrayList<TIntList>> newIndices = new ArrayList<ArrayList<TIntList>> ();
        final int consSize = consumerData.size();
        for (int i = 0; i < consSize; i++)
        {
            ArrayList<TIntList> consIndices = new ArrayList<TIntList>();
            final ConsumerEventsData [] consEvents = consumerData.get(i).events;
            for (int j = 0; j < consEvents.length; j++)
            {
                TIntList newConsumerIndices = new TIntArrayList();
                if (consEvents[j] != null)
                    for (int ci : consEvents[j].getProductIndices())
                    {
                    	if (ci < 0)
                    	{
                    		logger.error("Negative index of a product!!! This should never occur, the data were probably corrupted.");
                    		newConsumerIndices.add(ci);
                    	}
                    	else
                    	{
                    		if (newProductIDs.get(productData.get(ci).productId) < 0)
                    		{
                    			logger.warn("Product with id " + productData.get(ci).productId + " missing");
                    			newProductIDs.put(productData.get(ci).productId, product_counter);
                    			product_counter++;
                    			newProductData.add(productData.get(ci));
                    		}
                    		newConsumerIndices.add(newProductIDs.get(productData.get(ci).productId));
                    	}
                    }
                consIndices.add(newConsumerIndices);
            }
            newIndices.add(consIndices);
        }
        
        postProcessAttributes(newProductData);
        return new UpdateProductsDelta(this, newProductData, newProductIDs, newIndices);
    }
    
    private void postProcessAttributes(List<ProductData> pd)
    {
    	for (ProductData p : pd)
    	{
    		if (p != null)
    		{
	    		Attr [] ats = p.getAttributes();
	    		if (ats != null)
		    		for (Attr a : ats)
		    			if (a != null)
		    				a.postprocess();
    		}
    	}
    }
    
	/**
	 * returns a list of products
	 * @return
	 */
	public List<ProductData> getProducts() {
		return productData;
	}

	/**
	 * returns a list of consumers
	 * @return
	 */
	public List<ConsumerData> getConsumers() {
		return consumerData;
	}
	
	public boolean finishedReading()
	{
	    return finishedReading;
	}

	public TLongIntHashMap getProductsIDMap()
	{
	    return productIDs;
	}
	
    public TLongIntHashMap getConsumerIDMap()
    {
        return consumerIDs;
    }

    public ProductData getProductById(Long id) {
    	final int index = productIDs.get(id);
    	if (index == -1)
    		return null;
        return productData.get(index);
    }

    public ConsumerData getConsumerById(final long id) {
        final int index = getConsumerIndex(id);
        if ((index >= 0) && (index < consumerData.size()))
        {
            return consumerData.get(index);
        }
        return null;
    }

    public int getProductIndex(long productID)
    {
        if (!productIDs.contains(productID))
        {
            return -1;
        }
        return productIDs.get(productID);
    }
    
    public int getConsumerIndex(long consumerID)
    {
        if (!consumerIDs.contains(consumerID))
        {
            return -1;
        }
        return consumerIDs.get(consumerID);
    }    
    
    public ConsumerData getConsumerByIndex(int consumerIndex)
    {
        if (consumerIndex < 0 || consumerIndex >= consumerData.size())
            return null;
        return consumerData.get(consumerIndex);
    }
    
    public ProductData getProductByIndex(int productIndex)
    {
        return productData.get(productIndex);
    }

    public Reader getReader()
    {
        return reader;
    }

    public EventsDataDescriptor getEventsDescriptor(final String eventName)
    {
        if (eventsDescriptors.containsKey(eventName))
            return eventsDescriptors.get(eventName);
        else
        {
            logger.error("Event "+eventName+" not found in DataStore.");
            return null;
        }
    }
    
    public void removeData()
    {
        for (ConsumerData c : consumerData)
        {
            c.removeData(eventsDescriptors);
        }
        
    }    

    /**
     * Compute basic stats for each event.
     * Deprecated - use updateBasicStats instead
     */
    public void computeBasicStats()
    {
        // reset frequencies of products
        for (final ProductData p : productData)
            p.freq = 0;

        for (int i = consumerData.size() - 1; i >= 0; i--)
        {
            final ConsumerEventsData[] events = consumerData.get(i).events;
            for (int j = events.length - 1; j >= 0; j--)
            {
                final ConsumerEventsData e = events[j];
                if (null == e) continue;
                final int [] prods = events[j].getProductIndices();
                for (int p=prods.length-1; p>=0; p--)
                    productData.get(prods[p]).freq++;
            }
        }
    }
    
    public StatsUpdateDelta updateBasicStats(final ArrayList<ConsumerData> newData)
    {
        final int productSize = productData.size();
        int [] freq = new int[productSize];
        for (int i=productSize-1; i>=0; i--)
            freq[i] = 0;   
        
        for (int i = newData.size() - 1; i >= 0; i--)
        {
            ConsumerData cons = newData.get(i);
            if (null == cons)
                continue;
            final ConsumerEventsData[] events = cons.events;
            for (int j = events.length - 1; j >= 0; j--)
            {
                final ConsumerEventsData e = events[j];
                if (null == e) continue;
                final int [] prods = events[j].getProductIndices();
                for (int p=prods.length-1; p>=0; p--)
                    freq[prods[p]]++;
            }
        }
        return new StatsUpdateDelta(this, freq);
    }

    /**
     * Computes and then stores the statistics delta, based on the old and new
     * consumer data. The algorithm first orders and then compares the data,
     * where all the data elements that are not paired (=same in both the old
     * and the new set) constitute a delta.
     *
     * @param oldData
     * @param newData
     */
    public void computeDeltaStats(final ConsumerData oldData, final ConsumerData newData) {
//        final int eventTypeCount = eventsDescriptors.size();
        final TIntIntHashMap deltas = new TIntIntHashMap();
        for (int i = eventsDescriptors.size() - 1; i >= 0; i--)
        {
            final int [] newProds; // elements from the new data
            final int [] oldProds; // elements from the old data
            if ((null == newData) || (null == newData.events[i]))
                newProds = new int[0]; // empty new set
            else
            {
                final int [] originalNewProds = newData.events[i].getProductIndices();
                newProds = Arrays.copyOf(originalNewProds, originalNewProds.length);
                Arrays.sort(newProds);
            }
            if ((null == oldData) || (null == oldData.events[i]))
                oldProds = new int[0]; // empty old set
            else
            {
                final int [] originalOldProds = oldData.events[i].getProductIndices();
                oldProds = Arrays.copyOf(originalOldProds, originalOldProds.length);
                Arrays.sort(oldProds);
            }

            int iNew = 0;
            int iOld = 0;
            // compute deltas
            for (;;) {
                // skip corresponding pairs of product indices, they're not a part of delta
                while ((iNew < newProds.length) && (iOld < oldProds.length) && (newProds[iNew] == oldProds[iOld]))
                {
                    iNew++;
                    iOld++;
                }
                if ((iNew >= newProds.length) && (iOld >= oldProds.length))
                    break; // reached the end
                // count delta for the current index
                final int productIndex;
                int deltaCount = 0;
                if ((iOld >= oldProds.length) || ((iNew < newProds.length) && (newProds[iNew] < oldProds[iOld])))
                {
                    // old array reached end, or skipped to a new product index
                    productIndex = newProds[iNew];
                    while ((iNew < newProds.length) && (productIndex == newProds[iNew]))
                    {
                        deltaCount++;
                        iNew++;
                    }
                }
                else
                {
                    // new array reached end, or skipped to a new product index
                    productIndex = oldProds[iOld];
                    while ((iOld < oldProds.length) && (productIndex == oldProds[iOld]))
                    {
                        deltaCount--;
                        iOld++;
                    }
                }
                deltas.put(productIndex, deltas.get(productIndex) + deltaCount);
            }
        }
        // commit deltas
        final TIntIntIterator deltasIterator = deltas.iterator();
        while (deltasIterator.hasNext())
        {
            deltasIterator.advance();
            productData.get(deltasIterator.key()).freq += deltasIterator.value();
        }
    }

    public int eventCount(final String eventName) {
        final int eventIndex = getEventsDescriptor(eventName).index;
        int count = 0;
        for (final ConsumerData cd : consumerData) {
            final ConsumerEventsData d = cd.events[eventIndex];
            if (null != d)
            {
                count += d.getN();
            }
        }
        return count;
    }

    public Attr getAttr(final int productIndex, final int attributeIndex)
    {
        return getProductByIndex(productIndex).getAttribute(attributeIndex);
    }
    
    public Domain getDomain()
    {
    	return domain;
    }

    /**
     * This class will make an incremental update to the consumerData arrayList. Replaces all consumers 
     * in the dataStore.consumerData that have NON-NULL value in the newData arrayList.
     */
    public static class UpdateIncrementalData implements Commitable {
        public final DataStore dataStore;
        public final ArrayList<ConsumerData> newData;
        public final TLongIntHashMap newConsumerIDs;
        public final Map<String, EventsDataDescriptor> eventsDescriptors;
        public final StatsUpdateDelta statsDelta;
        private boolean merged;

        UpdateIncrementalData(final DataStore dataStore, ArrayList<ConsumerData> newData, TLongIntHashMap newConsumerIDs, final Map<String, EventsDataDescriptor> eventsDescriptors, final StatsUpdateDelta statsDelta) {
            this.dataStore = dataStore;
            this.newData = newData;
            this.newConsumerIDs = newConsumerIDs;
            this.eventsDescriptors = eventsDescriptors;
            this.statsDelta = statsDelta;
            merged = false;
        }
        
        public void mergeData()
        {
            final int newSize = newData.size();
            for (int i=0; i<newSize; i++)
            {
                ConsumerData c = newData.get(i);
                ConsumerData oldc = null, newc = null;
                if (c == null && i < dataStore.consumerData.size())
                {
                }
                // merge
                else if (i < dataStore.consumerData.size())
                {
                    oldc = dataStore.consumerData.get(i);
                    newc = new ConsumerData(oldc, false);
                    newc.concatenate(c);
                }
                else
                {
                    newc = c;
                }
                // remove data
                if (newc != null)
                {
                    newc.removeData(eventsDescriptors);
                    newc.removeDataTime(eventsDescriptors);
                    newData.set(i, newc);
                }
            }
            merged = true;
        }

        @Override
        public void commit() {
        	if (!merged)
        		dataStore.logger.error("Data was not yet merged and therefore commit is not possible.");
            this.statsDelta.commit();
            dataStore.consumerIDs = this.newConsumerIDs;
            final int newSize = newData.size();
            for (int i=0; i<newSize; i++)
                if (i == dataStore.consumerData.size())
                    dataStore.consumerData.add(newData.get(i));
                else if (newData.get(i) != null)
                    dataStore.consumerData.set(i, newData.get(i));
        }
    }
    
    public static class StatsUpdateDelta implements Commitable {
        public final int [] freq;
        public final DataStore dataStore;
        
        StatsUpdateDelta(final DataStore dataStore, final int [] freq)
        {
            this.freq = freq;
            this.dataStore = dataStore;
        }
        
        @Override
        public void commit() {
            final int productSize = dataStore.productData.size();
            for (int i=productSize-1; i>=0; i--)
                dataStore.productData.get(i).freq += freq[i];
        }
        
    }

    public static class UpdateProductsDelta implements Commitable {
        public final DataStore dataStore;
        public final List<ProductData> newProducts;
        public final TLongIntHashMap newProductIDs;
        final ArrayList<ArrayList<TIntList>> newIndices;
        
        UpdateProductsDelta(final DataStore dataStore, final List<ProductData> newProducts, final TLongIntHashMap newProductIDs, final ArrayList<ArrayList<TIntList>> newIndices)
        {
            this.dataStore = dataStore;
            this.newProducts = newProducts;
            this.newProductIDs = newProductIDs;
            this.newIndices = newIndices;
        }
        
        @Override
        public void commit() {
            dataStore.productData = newProducts;
            dataStore.productIDs = newProductIDs;
            // set new product indices in all consumers
            final int consSize = dataStore.consumerData.size();
            for (int i = 0; i < consSize; i++)
            {
                final ArrayList<TIntList> inds = newIndices.get(i);
                final ConsumerData c =  dataStore.consumerData.get(i);
                for (int j = 0; j < c.events.length; j++)
                {
                    if (c.events[j] != null)
                        c.events[j].setProductIndices(inds.get(j).toArray());
                }
            }
        }
        
    }

    public static class UpdateData implements Commitable {
        public final DataStore dataStore;
        public final ConsumerData oldData;
        public final ConsumerData newData;
        public final int consumerIndex;

        UpdateData(final DataStore dataStore, final ConsumerData oldData, final ConsumerData newData, final int consumerIndex) {
            this.dataStore = dataStore;
            this.oldData = oldData;
            this.newData = newData;
            this.consumerIndex = consumerIndex;
        }

        @Override
        public void commit() {
            if (consumerIndex >= dataStore.consumerData.size())
            {
                dataStore.logger.debug("UpdateData.commit(): adding consumer with ID " + newData.consumerId + " at index " + consumerIndex);
                dataStore.consumerIDs.put(newData.consumerId, dataStore.consumerData.size());
                dataStore.consumerData.add(newData);
            }
            else
            {
                dataStore.logger.debug("UpdateData.commit(): replacing consumer with ID " + newData.consumerId + " at index " + consumerIndex);
                dataStore.consumerData.set(consumerIndex, newData);
            }
        }
    }

}
