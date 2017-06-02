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
package com.gurucue.recommendations.misc;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.gurucue.recommendations.recommender.Settings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Miscellaneous class for working with predictor and some other objects.
 */
public class Misc {
    private final static Logger log = LogManager.getLogger(Misc.class);

    /**
     * Creates a java object by using given path. Used to create predictors from settings.
     * 
     * @param path Path to the java class.
     * @param name Name of the predictor (argument 1 in the predictor's constructor).
     * @param settings Settings object (argument 2 in the predictor's constructor).
     * @return
     */
    public static Object createClassObject(String path, String name, Settings settings) {
        try {
	    Class cl = Class.forName(path);
            Class [] classParm = new Class [] {String.class, Settings.class};
            Object [] objectParm = new Object[] {name, settings};            
            Constructor co = cl.getConstructor(classParm);
            return Misc.createObject(co, objectParm);
        } catch (ClassNotFoundException e) {
            System.out.println(String.format("Class %s could not be found!", path));
            final String reason = String.format("Class %s could not be found!", path);
            log.error(reason, e);
            throw new IllegalArgumentException(reason, e);
        } 
        catch (SecurityException e) {
            final String reason = String.format("SecurityException while cloning Class %s: %s", path, e.toString());
            log.error(reason, e);
            throw new IllegalArgumentException(reason, e);
        } catch (NoSuchMethodException e) {
            final String reason = String.format("NoSuchMethodException while cloning Class %s: %s", path, e.toString());
            log.error(reason, e);
            throw new IllegalArgumentException(reason, e);
        }
    }
    
    private static Object createObject(Constructor constructor, Object[] arguments) 
    {
        Object object = null;

        try {
            object = constructor.newInstance(arguments);
            return object;
        } catch (InstantiationException e) {
            final String reason = String.format("Cannot create a new instance of Class %s: %s", constructor.toString(), e.toString());
            log.error(reason, e);
            throw new IllegalArgumentException(reason, e);
        } catch (IllegalAccessException e) {
            final String reason = String.format("Cannot create a new instance of Class %s: %s", constructor.toString(), e.toString());
            log.error(reason, e);
            throw new IllegalArgumentException(reason, e);
        } catch (IllegalArgumentException e) {
            final String reason = String.format("Cannot create a new instance of Class %s: %s", constructor.toString(), e.toString());
            log.error(reason, e);
            throw new IllegalArgumentException(reason, e);
        } catch (InvocationTargetException e) {
            final String reason = String.format("Cannot create a new instance of Class %s: %s", constructor.toString(), e.toString());
            log.error(reason, e);
            throw new IllegalArgumentException(reason, e);
        }
    }
}

