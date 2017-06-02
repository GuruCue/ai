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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gurucue.recommendations.recommender.reader.Reader;

/**
 * Stores recommender settings that gets from DB (Reader). 
 * In the current version it is only a simple wrap around the HashMap with some auxiliary methods.
 */
public final class Settings implements Cloneable, Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = -2945487011241563673L;
	
	private final static Logger logger = LogManager.getLogger(Settings.class.getName());
    private final String logPrefix;
    
    private final Map<String,String> settingsMap;
    private final Long recommenderId;

    /**
     * Constructor used for cloning.
     * @param originalSettings the settings to clone
     */
    private Settings(final Settings originalSettings)  {
        this.settingsMap = new HashMap<String,String>(originalSettings.settingsMap);
        this.recommenderId = originalSettings.recommenderId;
        this.logPrefix = originalSettings.logPrefix;
    }

    /**
     * Constructor for the settings holder.
     * @param r the reader that will be used to obtain settings from
     * @param recommenderID the ID of the recommender that will use this instance; used for retrieving settings and for logging purposes
     */
    private Settings(final Reader r, final Long recommenderID) {
        this.settingsMap = r.getSettings(recommenderID);
        this.recommenderId = recommenderID;
        this.logPrefix = "[REC " + recommenderID + "] ";
    }

    /**
     * Constructor for the settings, to be used by Reader instances.
     * @param settingsMap
     * @param reader
     * @param recommenderId
     */
    public Settings(final Map<String,String> settingsMap, final Reader reader, final long recommenderId) {
        this.settingsMap = settingsMap;
        this.logPrefix = "[REC " + recommenderId + "] ";
        this.recommenderId = recommenderId;
    }
    
    public Settings(final Map<String,String> settingsMap, final long recommenderId) {
        this.settingsMap = settingsMap;
        this.logPrefix = "[REC " + recommenderId + "] ";
        this.recommenderId = recommenderId;
    }    
    
    public Settings(final long recommenderId) {
        this.settingsMap = new HashMap<String, String>();
        this.logPrefix = "[REC " + recommenderId + "] ";
        this.recommenderId = recommenderId;
    }        

    public void addSetting(String settingName, String settingValue)
    {
        settingsMap.put(settingName, settingValue);
    }

    public String getSetting(String settingName)
    {
        if (!settingsMap.containsKey(settingName))
        {
            return null;
        }
        return settingsMap.get(settingName);
    }

    public Boolean getSettingAsBoolean(String settingName)
    {
        String val = getSetting(settingName);
        if (null == val) return null;
        if (val.equals("yes") || val.equals("true"))
            return true;
        return false; 
    }

    public Float getSettingAsFloat(String settingName)
    {
        String val = getSetting(settingName);
        if (null == val) return null;
        return Float.valueOf(val); 
    }
    
    public int [] getAsIntArray(String settingName)
    {
        String [] spvals = getAsStringArray(settingName);
        if (null == spvals) return null;
        int [] intvals = new int[spvals.length];
        /* print substrings */
        for(int i =0; i < spvals.length ; i++)
          intvals[i] = Integer.parseInt(spvals[i]); 
        return intvals;
    }
    
    public long [] getAsLongArray(String settingName)
    {
        String [] spvals = getAsStringArray(settingName);
        if (null == spvals) return null;
        long [] longvals = new long[spvals.length];
        /* print substrings */
        for(int i =0; i < spvals.length ; i++)
          longvals[i] = Long.parseLong(spvals[i]); 
        return longvals;
    }    

    public float [] getAsFloatArray(String settingName)
    {
        String [] spvals = getAsStringArray(settingName);
        if (null == spvals) return null;
        float [] floatvals = new float[spvals.length];
        for(int i = 0; i < spvals.length ; i++)
          floatvals[i] = Float.parseFloat(spvals[i]); 
        return floatvals;
    }
    
    public String [] getAsStringArray(String settingName)
    {
        if (!settingsMap.containsKey(settingName))
        {
            return null;
        }
        String vals = settingsMap.get(settingName);
        if (vals.equals(""))
            return new String[0];
        return vals.split(";");
    }    
    
    private void logNotFoundError(String settingName)
    {
        logger.error(logPrefix + "Could not find setting "+settingName+". Are you sure it is specified?");
    }

    public Integer getSettingAsInt(String settingName) {
        String val = getSetting(settingName);
        if (null == val) return null;
        return Integer.valueOf(val);
    }

    public Long getSettingAsLong(String settingName) {
        String val = getSetting(settingName);
        if (null == val) return null;
        return Long.valueOf(val);
    }

    public Long getRecommenderId() {
        return recommenderId;
    }

    /* TODO: clone() is architecturally broken.
     * TODO: refactor into cloneSettings() that explicitly returns a Settings instance.
     */
    @Override
    public Object clone() throws CloneNotSupportedException {
        Settings newSettings = new Settings(this);
        return newSettings;
    }
    
    public Map<String,String> getSettings()
    {
    	return settingsMap;
    }
}
