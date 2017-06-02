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
package com.gurucue.recommendations.recommender.reader;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.ConsumerData;
import com.gurucue.recommendations.recommender.dto.ProductData;
import gnu.trove.list.TLongList;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.hash.TIntHashSet;

import com.gurucue.recommendations.entity.Product;

import com.gurucue.recommendations.recommender.dto.Domain;
import com.gurucue.recommendations.recommender.dto.EventsDataDescriptor;

/**
 * A general interface for a reader used as an intermediate between AI module and the database.
 */
public interface Reader {
    /**
     * Returns an array of products. Products are stored as ProductData objects.
     */
    public List<ProductData> getProducts(final Domain domain);

    /**
     * Returns products relevant for recommendation.
     */
    public TIntHashSet getRecommendProducts(List<ProductData> products, TLongIntHashMap prodMap);

    /**
     * Fill products with attributes. 
     * @param products The array of products.
     * @param prodMap A map from IDs (as stored in DB) to indices (as stored in DataStore) for products.
     * @return Returns a hashmap from attribute ID in DB to index in the attr table (within ProductData).
     */
    public void fillProductsWithAttributes(List<ProductData> products, TLongIntHashMap prodMap);

    /**
     * Update products with new values of attributes. 
     * @param products The array of products.
     * @param prodMap A map from IDs (as stored in DB) to indices (as stored in DataStore) for products.
     * @param oldProducts Old description of products.
     * @return Returns a hashmap from attribute ID in DB to index in the attr table (within ProductData).
     */
    public void updateProductsWithAttributes(List<ProductData> products, List<ProductData> oldProducts, TLongIntHashMap prodMap, TLongIntHashMap oldMap);
    
    /**
     * @return Returns an array of consumers.
     */
    public void getConsumers(List<ConsumerData> consumerData, TLongIntMap consumerIDs, Map<String, EventsDataDescriptor> eventsDescriptors);

    /**
     * Returns consumers we will recommend to.
     */
    public TIntHashSet getRecommendConsumers(List<ConsumerData> consumers, TLongIntHashMap consMap);

    /**
     * Reads one batch of data.If event_BATCH_SIZE is not set in settings, error will be thrown.  
     * 
     * @param settings
     * @param consumers
     * @param consMap
     * @param prodMap
     * @param eventsDescriptor
     * @return Returns true if the final batch of data was read. 
     */
    public boolean batchFillConsumersWithEvents(final List<ConsumerData> oldConsumers, List<ConsumerData> newConsumers, TLongIntHashMap newConsMap, final TLongIntHashMap prodMap, final Map<String, EventsDataDescriptor> eventsDescriptor);

    /**
     * Returns a consumer object (from DB)
     * @param id
     * @return
     */
//    @Deprecated
//    public Consumer getConsumerById(Long id);

    /**
     * Reads Consumers from DB and creates a new ConsumerData object.
     * TODO: for efficiency reasons work with IDs instead of entity objects; this is why it's deprecated.
     * @param id
     * @param prodMap
     * @return
     */
    public void fillAConsumerWithEvents(ConsumerData consumerData, Map<String,EventsDataDescriptor> eventsDescriptors);

    /**
     * Creates a HashMap from RECOMMENDER_SETTINGS table; (Settings_name --> value)
     * @param recommenderID
     * @return
     */
    public Map<String, String> getSettings(long recommenderID);
    
    /**
     * Creates a list of product ids that were recommended to user with id between start date and end date.
     * @param id
     * @param start
     * @param end
     * @return
     */
    public TLongList getRecommendations(long id, Date start, Date end);
    
    /**
     * Runs a query with the specified query ID and returns a list of IDs as returned by the query.
     * The query must return one field of type long.
     * @param settings
     * @param queryId
     * @return
     */
    public TLongList getIds(Settings settings, String queryId);
    
    /**
     * Runs a specific query and returns a list of IDs as returned by the query.
     * The query must return one field of type long.
     * @param query
     * @return
     */
    public TLongList getIds(String query);

    
    /**
     * Returns a Settings instance configured by this Reader.
     * It is tailored for the recommender that this Reader has been configured with.
     * @return Settings instance configured by this Reader
     */
    public Settings getSettings();

    /**
     * Returns the ID of the recommender that this Reader has been configured with.
     * @return ID of the recommender that this Reader is to be used with
     */
    public Long getRecommenderId();
    
    /**
     * Returns a list of empty attributes used in learning. This method should be called before a list of products is created.
     * @return
     */
    public Domain createDomain();
    
    /**
     * Returns a map between channel_id and its language (spoken and subtitles languages are merged)
     */
    public Map<String,String> getLanguages(final String query);
}
