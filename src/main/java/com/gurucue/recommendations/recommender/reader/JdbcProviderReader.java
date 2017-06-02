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

import com.gurucue.recommendations.data.DataLink;
import com.gurucue.recommendations.data.DataManager;
import com.gurucue.recommendations.data.DataProvider;
import com.gurucue.recommendations.data.jdbc.*;
import com.gurucue.recommendations.entity.Product;

import com.gurucue.recommendations.recommender.Settings;
import com.gurucue.recommendations.recommender.dto.*;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.hash.TIntHashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.recommender.dto.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

public class JdbcProviderReader implements Reader {
    private static final String GET_SETTINGS_SQL = "select setting_name, setting_value from recommender_setting where recommender_id = ?";
    private static final String GET_RECOMMENDATIONS_SQL = "select distinct a.product_id from log_svc_rec_product a inner join log_svc_rec r on r.id = a.log_svc_rec_id where r.consumer_id = ? and r.request_timestamp <= ? and r.request_timestamp >= ?";

    private final Logger log;
    private final Long recommenderId;
    private final Settings settings;

    private final StatementWrapper<List<ProductData>, Domain> getProducts;
    private final StatementWrapper<TIntHashSet, TLongIntHashMap> getRecommendProducts;
    private final StatementWrapper<Void, ArgumentProductsProdMap> fillProductsWithAttributes;
    private final StatementWrapper<Void, ArgumentProductsProdMapUpdate> updateProductsWithAttributes;
    private final StatementWrapper<Void, ArgumentsForGetConsumers> getConsumers;
    private final StatementWrapper<TIntHashSet, TLongIntHashMap> getRecommendConsumers;
    private final PreparedStatementWrapper<Long, ArgumentsForFillConsumersWithEvents> batchFillConsumersWithEvents;
    private final PreparedStatementWrapper<Void, ArgumentsForFillAConsumerWithEvents> fillAConsumerWithEvents;
    private final PreparedStatementWrapper<Map<String, String>, Long> getSettings;
    private final PreparedStatementWrapper<TLongList, ArgumentsForGetRecommendations> getRecommendations;
    private final StatementWrapper<TLongList, Void> getIds;
    private final StatementWrapper<Map<String, String>, Void> getLanguages;
    private final StatementWrapper<Long, Void> getMaxEvent;     

    public JdbcProviderReader(final Long recommenderId) {
        this.recommenderId = recommenderId;
        this.log = LogManager.getLogger(JdbcProviderReader.class.getName() + " [REC " + recommenderId + "]");

        // this one must be created before the settings are retrieved, so settings *can* be retrieved
        getSettings = new PreparedStatementWrapper<Map<String, String>, Long>(
                "[REC " + recommenderId + " getSettings]",
                new PreparedStatementProcessor<Map<String, String>, Long>() {
                    @Override
                    public Map<String, String> process(final PreparedStatement preparedStatement, final Long recommenderId) throws SQLException {
                        final Map<String,String> settings = new HashMap<String,String>();
                        preparedStatement.setLong(1, recommenderId);
                        final ResultSet rs = preparedStatement.executeQuery();
                        try {
                            while (rs.next())
                            {
                                settings.put(rs.getString(1), rs.getString(2));
                            }
                        }
                        finally {
                            rs.close();
                        }
                        return settings;
                    }
                }
        );

        // now create the Settings object
        this.settings = new Settings(getSettings.execute(GET_SETTINGS_SQL, recommenderId), this, recommenderId);

        // and configure the rest of the data fetchers

        getProducts = new StatementWrapper<List<ProductData>, Domain>(
                "[REC " + recommenderId + " getProducts]",
                new ResultProcessor<List<ProductData>, Domain>() {
                    @Override
                    public List<ProductData> process(final ResultSet resultSet, final Domain domain) throws SQLException {
                        List<ProductData> prodArray = new ArrayList<ProductData>();
                        if (resultSet == null)
                        {
                            log.warn("[getProducts]: List of learning products not provided; all products will be used in learning.");
                            return prodArray;
                        }
                        while (resultSet.next())
                        {
                            // values should be: private id, public id, product type
                            prodArray.add(new ProductData(resultSet.getLong(1), resultSet.getLong(2), (int)resultSet.getLong(3), domain));
                        }
                        return prodArray;
                    }
                }
        );

        getRecommendProducts = new StatementWrapper<TIntHashSet, TLongIntHashMap>(
                "[REC " + recommenderId + " getRecommendProducts]",
                new ResultProcessor<TIntHashSet, TLongIntHashMap>() {
                    @Override
                    public TIntHashSet process(final ResultSet resultSet, final TLongIntHashMap prodMap) throws SQLException {
                        final TIntHashSet prodSet = new TIntHashSet();
                        while (resultSet.next())
                        {
                            prodSet.add(prodMap.get(resultSet.getLong(1)));
                        }
                        return prodSet;
                    }
                }
        );

        fillProductsWithAttributes = new StatementWrapper<Void, ArgumentProductsProdMap>(
                "[REC " + recommenderId + " fillProductsWithAttributes]",
                new ResultProcessor<Void, ArgumentProductsProdMap>() {
                    @Override
                    public Void process(final ResultSet resultSet, final ArgumentProductsProdMap arguments) throws SQLException {
                        final List<ProductData> products = arguments.products;
                        final TLongIntHashMap prodMap = arguments.prodMap;
                        final ProductReader reader = arguments.reader;

                        long productID;
                        int productIndex;
                        while (resultSet.next())
                        {
                            productID = resultSet.getLong(1);
                            // read attributes only for relevant products
                            if (prodMap.contains(productID))
                                productIndex = prodMap.get(productID);
                            else
                                continue;

                            reader.readFields(products.get(productIndex), null, resultSet.getString(2), resultSet.getString(3), resultSet.getString(4));
                        }
                        return null;
                    }
                }
        );
        
        updateProductsWithAttributes = new StatementWrapper<Void, ArgumentProductsProdMapUpdate>(
                "[REC " + recommenderId + " updateProductsWithAttributes]",
                new ResultProcessor<Void, ArgumentProductsProdMapUpdate>() {
                    @Override
                    public Void process(final ResultSet resultSet, final ArgumentProductsProdMapUpdate arguments) throws SQLException {
                        final List<ProductData> products = arguments.products;
                        final List<ProductData> oldProducts = arguments.oldProducts;
                        final TLongIntHashMap prodMap = arguments.prodMap;
                        final TLongIntHashMap oldMap = arguments.oldMap;
                        final ProductReader reader = arguments.reader;
                        
                        long productID;
                        int productIndex;
                        while (resultSet.next())
                        {
                            productID = resultSet.getLong(1);
                            // read attributes only for relevant products
                            if (prodMap.contains(productID))
                                productIndex = prodMap.get(productID);
                            else
                                continue;

                            final ProductData oldProduct;
                            if (oldMap.contains(productID) && oldMap.get(productID) >= 0) oldProduct = oldProducts.get(oldMap.get(productID));
                            else oldProduct = null;
                            reader.readFields(products.get(productIndex), oldProduct, resultSet.getString(2), resultSet.getString(3), resultSet.getString(4));
                        }
                        return null;
                    }
                }
        );
        

        getConsumers = new StatementWrapper<Void, ArgumentsForGetConsumers>(
                "[REC " + recommenderId + " getConsumers]",
                new ResultProcessor<Void, ArgumentsForGetConsumers>() {
                    @Override
                    public Void process(final ResultSet resultSet, final ArgumentsForGetConsumers arguments) throws SQLException {
                        if (resultSet == null)
                        {
                            log.warn("[getConsumers]: List of learning consumers not provided; all consumers will be used in learning. ");
                            return null;
                        }
                        final TLongIntMap consumerIDs = arguments.consumerIDs;
                        int consumerIndex = 0;
                        while (resultSet.next())
                        {
                            final long consumerId = resultSet.getLong(1);
                            consumerIDs.put(consumerId, consumerIndex);
                            consumerIndex++;
                        }
                        return null;
                    }
                }
        );
        
        getMaxEvent = new StatementWrapper<Long, Void>(
                "[REC " + recommenderId + " getMaxEvent]",
                new ResultProcessor<Long, Void>() {
                    @Override
                    public Long process(final ResultSet resultSet, final Void arguments) throws SQLException {
                        resultSet.next();
                        return resultSet.getLong(1);
                    }
                }                
        );

        getRecommendConsumers = new StatementWrapper<TIntHashSet, TLongIntHashMap>(
                "[REC " + recommenderId + " getRecommendConsumers]",
                new ResultProcessor<TIntHashSet, TLongIntHashMap>() {
                    @Override
                    public TIntHashSet process(final ResultSet resultSet, final TLongIntHashMap consMap) throws SQLException {
                        final TIntHashSet consSet = new TIntHashSet();
                        while (resultSet.next())
                        {
                            consSet.add(consMap.get(resultSet.getLong(1)));
                        }
                        return consSet;
                    }
                }
        );

        batchFillConsumersWithEvents = new PreparedStatementWrapper<Long, ArgumentsForFillConsumersWithEvents>(
                "[REC " + recommenderId + " batchFillConsumersWithEvents]",
                new PreparedStatementProcessor<Long, ArgumentsForFillConsumersWithEvents>() {
                    @Override
                    public Long process(final PreparedStatement preparedStatement, final ArgumentsForFillConsumersWithEvents arguments) throws SQLException {
                        final String logPrefix = "[batchFillConsumersWithEvents<" + arguments.eventName + ">]: ";
                        final TLongIntHashMap newConsMap = arguments.consMap;
                        final EventsDataDescriptor descriptor = arguments.eventsDescriptors.get(arguments.eventName);
                        List<ConsumerData> newConsumersData = arguments.newConsumersData;
                        final int eventsIndex = descriptor.index;
                        final int eventTypeCount = arguments.eventsDescriptors.size();
                        final int batch_size = descriptor.batch_size;
                        final long last_read_id = descriptor.last_read_id;
                        final long max_event_id = arguments.maxEventID;
                        log.info(logPrefix + "filling data of consumers with event: " + arguments.eventName + ", starting with id = " + last_read_id + ", max event id = " + max_event_id);

                        ConsumerData consumerData = null;
                        ConsumerEventsData eventsData = null;

                        preparedStatement.setLong(1, last_read_id);
                        if (batch_size >= 0)
                            preparedStatement.setLong(2, Math.min(last_read_id + batch_size, max_event_id+1));
                        else
                            preparedStatement.setLong(2, max_event_id);
                        final ResultSet resultSet = preparedStatement.executeQuery();
                        log.info(logPrefix + "Query executed, reading data");
                        long current_id = -1;
                        try {
                            int counter = 0;
                            // query returns: first field is event_id, next is consumer_id, the rest is arbitrary event data
                            final int n = resultSet.getMetaData().getColumnCount();
                            while (resultSet.next()) {
                                current_id = resultSet.getLong(1);
                                if (current_id > max_event_id) // do not read events with id that is higher than max_event_id.
                                    continue;
                                descriptor.last_read_id = current_id;
                                final long consumerId = resultSet.getLong(2);
                                int consumerIndex = newConsMap.get(consumerId);
                                // new consumer?
                                consumerData = new ConsumerData(consumerId, eventTypeCount);
                                if (consumerIndex < 0)
                                {
                                    newConsumersData.add(consumerData);
                                    newConsMap.put(consumerId, newConsMap.size());
                                }
                                // newConsumersData = null
                                else if (newConsumersData.get(consumerIndex) == null)
                                {
                                    consumerData = new ConsumerData(consumerId, eventTypeCount);
                                    newConsumersData.set(consumerIndex, consumerData);
                                }
                                else
                                    consumerData = newConsumersData.get(consumerIndex);

                                eventsData = consumerData.events[eventsIndex];
                                if (eventsData == null)
                                    consumerData.events[eventsIndex] = eventsData = descriptor.createEventsData();

                                // the Object array should contain all columns
                                final Object [] cols = new Object [n-1];
                                cols[0] = consumerId;

                                boolean isEmpty = true;
                                for (int i = 3; i <= n; i++)
                                {
                                    cols[i-2] = resultSet.getObject(i);
                                    isEmpty = isEmpty && resultSet.wasNull();
                                }

                                // consumer must have at least one non-null field, otherwise there is no event data
                                if (isEmpty)
                                    continue;

                                eventsData.addEvent(descriptor, cols);
                                counter++;
                            }
                            // finalize all events
                            for (ConsumerData c : newConsumersData)
                            {
                                // if events null or consumer data null, skip it
                                if (c != null && c.events[eventsIndex] != null)
                                {
                                    c.events[eventsIndex].finalizeReading();
                                }
                            }
                            log.info(logPrefix + "set " + counter + " events of type " + arguments.eventName + " to " + newConsMap.size() + " consumers. ID of the last read event =" + descriptor.last_read_id + ", timestamp of the last event = " + descriptor.last_read_date);
                        }
                        finally {
                            resultSet.close();
                        }

                        if (last_read_id + batch_size < max_event_id)
                        {
                            descriptor.last_read_id = last_read_id + batch_size;
                            return current_id;
                        }
                        else {
                            descriptor.last_read_id = max_event_id;
                            return -1L;
                        }
                        
                    }
                }
        );

        fillAConsumerWithEvents = new PreparedStatementWrapper<Void, ArgumentsForFillAConsumerWithEvents>(
                "[REC " + recommenderId + " fillAConsumerWithEvents]",
                new PreparedStatementProcessor<Void, ArgumentsForFillAConsumerWithEvents>() {
                    @Override
                    public Void process(final PreparedStatement preparedStatement, final ArgumentsForFillAConsumerWithEvents parameters) throws SQLException {
                        final EventsDataDescriptor descriptor = parameters.eventsDescriptors.get(parameters.eventName);
                        ConsumerData consumerData = parameters.consumerData;
                        final ConsumerEventsData eventsData;
                        consumerData.events[descriptor.index] = eventsData = descriptor.createEventsData();

                        preparedStatement.setLong(1, consumerData.consumerId);
                        long counter = 0;
                        final ResultSet rs = preparedStatement.executeQuery();
                        try {
                            final int n = rs.getMetaData().getColumnCount();
                            while (rs.next()) {
                                // consumer's id must be in consMap
                                // the Object array should contain all columns
                                final Object [] cols = new Object [n];
                                for (int i = 0; i < n; i++)
                                    cols[i] = rs.getObject(i+1);
                                eventsData.addEvent(descriptor, cols);
                                counter++;
                            }
                        }
                        finally {
                            rs.close();
                        }
                        eventsData.finalizeReading();
                        log.debug("set " + counter + " events of type " + parameters.eventName + " to consumer " + consumerData.consumerId);
                        return null;
                    }
                }
        );

        getRecommendations = new PreparedStatementWrapper<TLongList, ArgumentsForGetRecommendations>(
                "[REC " + recommenderId + " getRecommendations]",
                new PreparedStatementProcessor<TLongList, ArgumentsForGetRecommendations>() {
                    @Override
                    public TLongList process(final PreparedStatement preparedStatement, final ArgumentsForGetRecommendations parameters) throws SQLException {
                        final TLongList l = new TLongArrayList();
                        preparedStatement.setLong(1, parameters.consumerId);
                        preparedStatement.setTimestamp(2, new Timestamp(parameters.logEnd.getTime()));
                        preparedStatement.setTimestamp(3, new Timestamp(parameters.logStart.getTime()));
                        final ResultSet rs = preparedStatement.executeQuery();
                        try {
                            while (rs.next()) {
                                l.add(rs.getLong(1));
                            }
                        }
                        finally {
                            rs.close();
                        }
                        return l;
                    }
                }
        );

        getIds = new StatementWrapper<TLongList, Void>(
                "[REC " + recommenderId + " getIds]",
                new ResultProcessor<TLongList, Void>() {
                    @Override
                    public TLongList process(final ResultSet resultSet, final Void arguments) throws SQLException {
                        final TLongList ids = new TLongArrayList();
                        while (resultSet.next()) {
                            ids.add(resultSet.getLong(1));
                        }
                        return ids;
                    }
                }
        );
        
        
        getLanguages = new StatementWrapper<Map<String, String>, Void>(
                "[REC " + recommenderId + " getLanguages]",
                new ResultProcessor<Map<String, String>, Void>() {
                    @Override
                    public Map<String, String> process(final ResultSet resultSet, final Void arguments) throws SQLException {
                        final Map<String, String> langs = new HashMap<String, String>();
                        while (resultSet.next()) {
                        	String channel = resultSet.getString(1);
                        	String l1 = resultSet.getString(2);
                        	String l2 = resultSet.getString(3);
                        	langs.put(channel, l1+l2);
                        }
                        return langs;
                    }
                }
        );        

    }

    @Override
    public List<ProductData> getProducts(final Domain domain) {
        log.info("reading products for learning");
        final List<ProductData> prodArray = getProducts.execute(settings.getSetting("LEARN_PRODUCTS_SQL"), domain);
        log.info("returning " + prodArray.size() + " products for learning");
        return prodArray;
    }

    @Override
    public TIntHashSet getRecommendProducts(final List<ProductData> products, final TLongIntHashMap prodMap) {
        log.info("reading products for recommending");
        final TIntHashSet prodSet = getRecommendProducts.execute(settings.getSetting("RECOMMEND_PRODUCTS_SQL"), prodMap);
        log.info("returning " + prodSet.size() + " products for recommending");
        return prodSet;
    }
    
    @Override
    public Domain createDomain()
    {
        final String [] attrTypes = settings.getAsStringArray("ATTRIBUTE_TYPES");
        final String [] attrNames = settings.getAsStringArray("ATTRIBUTE_NAMES");
        if (attrTypes.length != attrNames.length)
            log.error("The number of attributes in ATTRIBUTE_NAMES and in ATTRIBUTE_TYPES does not match");
        final Attr[] attrs = new Attr[attrTypes.length];
        int index = 0;
        for (String type : attrTypes)
        {
            final String identifier = attrNames[index];
            if (type.equalsIgnoreCase("int"))
                attrs[index] = new ContAttr(identifier);
            else if (type.equalsIgnoreCase("multi"))
                attrs[index] = new MultiValAttr(identifier);
            else if (type.equalsIgnoreCase("multisplit"))
                attrs[index] = new MultiValAttr(identifier, true);
            else if (type.startsWith("tfidf"))
            {
            	String[] vals = type.split(":");
            	int ngrams = Integer.parseInt(vals[1]);
            	int nfeatures = Integer.parseInt(vals[2]);
            	int onlyTitle = Integer.parseInt(vals[3]);
                attrs[index] = new TFIDFAttr(identifier, ngrams, nfeatures, onlyTitle>0?true:false);
            }
            else if (type.startsWith("string"))
            {
            	String[] vals = type.split(":");
            	int maxLength = Integer.parseInt(vals[1]);
                attrs[index] = new StringAttr(identifier, maxLength);
            }
            else if (type.equalsIgnoreCase("boolean"))
                attrs[index] = new BooleanAttr(identifier);
            else if (type.equalsIgnoreCase("float"))
                attrs[index] = new FloatAttr(identifier);
            else if (type.equalsIgnoreCase("long"))
                attrs[index] = new LongAttr(identifier);
            else
            {
                log.error("[initializeAttributes]: Unrecognized value type for attribute " + identifier + ": " + type);    
            }
            index += 1;
        }
        return new Domain(attrs);
    }

    @Override
    public void fillProductsWithAttributes(final List<ProductData> products, final TLongIntHashMap prodMap) {
        log.info("filling products with attributes");
        final String [] attrsSQL = settings.getAsStringArray("READ_ATTRIBUTES");
//        final String [] attrsSQLLink = settings.getAsStringArray("SQL_ATTRIBUTE_LINK");
        final String [] attrNames = settings.getAsStringArray("ATTRIBUTE_NAMES");
        final String [] attrFields = settings.getAsStringArray("READ_ATTRIBUTES_FIELDS");
        
        int n = attrsSQL.length;
        final DataProvider provider = DataManager.getProvider();

        for (int i = 0; i < n; i++)
        {
        	log.info("Going into: " + attrsSQL[i]);
            fillProductsWithAttributes.execute(attrsSQL[i], new ArgumentProductsProdMap(products, prodMap, new ProductReader(attrNames, attrFields[i], provider)));
        }
        log.info("filled products with " + attrNames.length + " attributes");
    }
    
    
    @Override
    public void updateProductsWithAttributes(final List<ProductData> products, final List<ProductData> oldProducts, final TLongIntHashMap prodMap, final TLongIntHashMap oldMap) {
        log.info("updating products with attributes");
        final String [] attrsSQL = settings.getAsStringArray("READ_ATTRIBUTES");
        final String [] attrNames = settings.getAsStringArray("ATTRIBUTE_NAMES");
        final String [] attrFields = settings.getAsStringArray("READ_ATTRIBUTES_FIELDS");

        int n = attrsSQL.length;
        final DataProvider provider = DataManager.getProvider();

        for (int i = 0; i < n; i++)
        {
            updateProductsWithAttributes.execute(attrsSQL[i], new ArgumentProductsProdMapUpdate(products, oldProducts, prodMap, oldMap, new ProductReader(attrNames, attrFields[i], provider)));
        }
        log.info("updated products with " + attrNames.length + " attributes");
    }    
    
    @Override
    public void getConsumers(final List<ConsumerData> consumerData, final TLongIntMap consumerIDs, final Map<String, EventsDataDescriptor> eventsDescriptors) {
        log.info("reading consumers for learning");
        getConsumers.execute(settings.getSetting("LEARN_CONSUMERS_SQL"), new ArgumentsForGetConsumers(consumerData, consumerIDs, eventsDescriptors));
        log.info("returning " + consumerData.size() + " consumers for learning");
    }

    @Override
    public TIntHashSet getRecommendConsumers(final List<ConsumerData> consumers, final TLongIntHashMap consMap) {
        log.info("reading recommend consumers");
        final TIntHashSet consSet = getRecommendConsumers.execute(settings.getSetting("RECOMMEND_CONSUMERS_SQL"), consMap);
        log.info("returning " + consSet.size() + " recommend consumers");
        return consSet;
    }

    @Override
    public boolean batchFillConsumersWithEvents(final List<ConsumerData> oldConsumers, final List<ConsumerData> newConsumers, final TLongIntHashMap newConsMap, TLongIntHashMap prodMap, final Map<String, EventsDataDescriptor> eventsDescriptor) {
        ArgumentsForFillConsumersWithEvents arguments = new ArgumentsForFillConsumersWithEvents(newConsMap, oldConsumers, newConsumers, eventsDescriptor);
        boolean allFinished = true;
        final String [] eventNames = settings.getAsStringArray("EVENTS");
        for (int i = 0; i < eventNames.length; i++) {
            final String e = eventNames[i];
        	log.info("Reading event: " + e + ", " + settings.getSetting(e + "_SQL"));
            arguments.eventName = e;
            if (settings.getSettingAsLong(e + "_MAX_EVENT") != null)
                arguments.maxEventID = settings.getSettingAsLong(e + "_MAX_EVENT");
            else
                arguments.maxEventID = getMaxEvent.execute(settings.getSetting(e + "_MAX_EVENT_SQL"), null);
            batchFillConsumersWithEvents.execute(settings.getSetting(e + "_SQL"), arguments);
            allFinished = allFinished && (arguments.eventsDescriptors.get(arguments.eventName).last_read_id == arguments.maxEventID);
        }
        return allFinished;
    }

    @Override
    public void fillAConsumerWithEvents(final ConsumerData consumerData, final Map<String, EventsDataDescriptor> eventsDescriptors) {
        log.info("filling the data of the consumer with ID " + consumerData.consumerId + " with events.");
        final ArgumentsForFillAConsumerWithEvents arguments = new ArgumentsForFillAConsumerWithEvents(eventsDescriptors, consumerData);
        final String [] eventNames = settings.getAsStringArray("EVENTS");
        for (int i = 0; i < eventNames.length; i++) {
            final String e = eventNames[i];
            arguments.eventName = e;
            fillAConsumerWithEvents.execute(settings.getSetting(e + "_CONSUMER_SQL"), arguments);
        }
        log.info("end filling the data of the consumer with ID " + consumerData.consumerId + " with events.");
    }
    


    @Override
    public Map<String, String> getSettings(long recommenderID) {
        log.info("reading settings of recommender " + recommenderID);
        final Map<String, String> settings = getSettings.execute(GET_SETTINGS_SQL, recommenderID);
        log.info("reading settings end");
        return settings;
    }

    @Override
    public TLongList getRecommendations(final long consumerId, final Date logStart, final Date logEnd) {
        return getRecommendations.execute(GET_RECOMMENDATIONS_SQL, new ArgumentsForGetRecommendations(consumerId, logStart, logEnd));
    }

    @Override
    public TLongList getIds(Settings settings, final String queryId) {
        final String sql = settings.getSetting(queryId + "_SQL");
        log.info("executing query: " + sql);
        final TLongList ids = getIds.execute(sql, null);
        log.info("end executing query: " + sql);
        return ids;
    }

    @Override
    public TLongList getIds(final String query) {
        log.info("executing query: " + query);
        final TLongList ids = getIds.execute(query, null);
        log.info("end executing query: " + query);
        return ids;
    }


	@Override
	public Map<String, String> getLanguages(final String query) {
        log.info("executing query: " + query);
        final Map<String,String> langs = getLanguages.execute(query, null);
        log.info("end executing query: " + query);
		return langs;
	}    
    
    @Override
    public Settings getSettings() {
        return settings;
    }

    @Override
    public Long getRecommenderId() {
        return recommenderId;
    }

    // Argument containers for wrappers

    private static class ArgumentsForGetConsumers {
        final List<ConsumerData> consumerData;
        final TLongIntMap consumerIDs;
        final Map<String, EventsDataDescriptor> eventsDescriptors;
        ArgumentsForGetConsumers(final List<ConsumerData> consumerData, final TLongIntMap consumerIDs, final Map<String, EventsDataDescriptor> eventsDescriptors) {
            this.consumerData = consumerData;
            this.consumerIDs = consumerIDs;
            this.eventsDescriptors = eventsDescriptors;
        }
    }

    private static class ArgumentProductsProdMap {
        final List<ProductData> products;
        final TLongIntHashMap prodMap;
        final ProductReader reader;
        ArgumentProductsProdMap(final List<ProductData> products, final TLongIntHashMap prodMap, final ProductReader reader) {
            this.products = products;
            this.prodMap = prodMap;
            this.reader = reader;
        }
    }
    
    private static class ArgumentProductsProdMapUpdate {
        final List<ProductData> products;
        final List<ProductData> oldProducts;
        final TLongIntHashMap prodMap;
        final TLongIntHashMap oldMap;
        final ProductReader reader;
        ArgumentProductsProdMapUpdate(final List<ProductData> products, final List<ProductData> oldProducts, final TLongIntHashMap prodMap, final TLongIntHashMap oldMap, final ProductReader reader) {
            this.products = products;
            this.prodMap = prodMap;
            this.reader = reader;
            this.oldProducts = oldProducts;
            this.oldMap = oldMap;
        }
    }    

    private static class ArgumentsForFillConsumersWithEvents {
        final TLongIntHashMap consMap;
        final List<ConsumerData> oldConsumersData, newConsumersData;
        final Map<String,EventsDataDescriptor> eventsDescriptors;
        Long maxEventID;
        String eventName;
        ArgumentsForFillConsumersWithEvents(final TLongIntHashMap consMap, final List<ConsumerData> oldConsumersData, List<ConsumerData> newConsumersData, final Map<String,EventsDataDescriptor> eventsDescriptors) {
            this.consMap = consMap;
            this.oldConsumersData = oldConsumersData;
            this.newConsumersData = newConsumersData;
            this.eventsDescriptors = eventsDescriptors;
        }
    }

    private static class ArgumentsForFillAConsumerWithEvents {
        final Map<String,EventsDataDescriptor> eventsDescriptors;
        final ConsumerData consumerData;
        String eventName;
        ArgumentsForFillAConsumerWithEvents(final Map<String,EventsDataDescriptor> eventsDescriptors, final ConsumerData consumerData) {
            this.eventsDescriptors = eventsDescriptors;
            this.consumerData = consumerData;
        }
    }

    private static class ArgumentsForGetRecommendations {
        final long consumerId;
        final Date logStart;
        final Date logEnd;
        ArgumentsForGetRecommendations(final long consumerId, final Date logStart, final Date logEnd) {
            this.consumerId = consumerId;
            this.logStart = logStart;
            this.logEnd = logEnd;
        }
    }
}
