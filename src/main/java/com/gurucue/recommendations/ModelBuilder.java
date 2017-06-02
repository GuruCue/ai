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
package com.gurucue.recommendations;

import com.gurucue.recommendations.data.DataManager;
import com.gurucue.recommendations.data.DataProvider;
import com.gurucue.recommendations.data.postgresql.PostgreSqlDataProvider;
import com.gurucue.recommendations.recommender.MasterRecommender;
import com.gurucue.recommendations.recommender.Recommender;
import com.gurucue.recommendations.recommender.reader.JdbcProviderReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Used for building or updating a model. Invoke it from a console.
 * It expects the following properties:
 * <ul>
 *     <li><code>rec.db.jdbc</code>, required, JDBC connection string for the database</li>
 *     <li><code>rec.db.user</code>, required, database username</li>
 *     <li><code>rec.db.pass</code>, required, database password</li>
 *     <li><code>rec.id</code>, required, ID of the recommender</li>
 *     <li><code>rec.saveFilename</code>, optional, if not specified it defaults to a predefined filename</li>
 *     <li><code>rec.readFilename</code>, optional, if specified the existing model is loaded from the file and an incremental update made</li>
 * </ul>
 *
 * The properties can be specified either directly at command line (with <code>-D switch</code>,
 * or in a properties file. If using a properties file, the specify its filename either as the
 * only argument to the program, or as the <code>rec.propFile</code> property on command-line
 * (<code>-Drec.propFile=&lt;filename&gt;</code>).
 */
public class ModelBuilder {
    public static void main(final String[] args) {
        if (args.length > 1) {
            System.err.println("Exactly one optional argument permitted: the properties filename");
            System.exit(1);
            return;
        }

        String propertiesFilename = null;
        if (args.length == 1) propertiesFilename = args[0];
        else propertiesFilename = System.getProperty("rec.propFile");

        Properties props = null;
        if (propertiesFilename != null) {
            File f = new File(propertiesFilename);
            if (!f.exists()) {
                System.err.println("The properties file does not exists: " + propertiesFilename);
                System.exit(2);
                return;
            }
            props = new Properties();
            try {
                props.load(new FileReader(f));
            }
            catch (IOException|RuntimeException e) {
                System.err.println("Cannot read the properties file \"" + propertiesFilename + "\": caught exception " + e.getClass().getCanonicalName());
                e.printStackTrace();
                System.exit(3);
                return;
            }
        }

        String jdbc, username, password, idString;
        try {
            jdbc = getProperty(props, "rec.db.jdbc", null);
            username = getProperty(props, "rec.db.user", null);
            password = getProperty(props, "rec.db.pass", null);
            idString = getProperty(props, "rec.id", null);
        }
        catch (MissingArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(4);
            return;
        }

        long id;
        try {
            id = Long.parseLong(idString, 10);
        }
        catch (NumberFormatException e) {
            System.err.println("Recommender ID is not an integer: " + idString);
            System.exit(5);
            return;
        }

        final DataProvider provider = PostgreSqlDataProvider.create(jdbc, username, password);
        try {
            DataManager.setProvider(provider);

            String filename = getProperty(props, "rec.saveFilename", "");
            if (filename.length() == 0) {
                filename = "/opt/GuruCue/RecommenderStates/recommender-" + idString + ".serialized";
                System.out.println("Property rec.modelFilename was not set, using filename " + filename);
            }

            String existingStateFile = getProperty(props, "rec.readFilename", "");

            final Recommender recommender;
            if (existingStateFile.length() == 0) {
                recommender = MasterRecommender.getRecommender(new JdbcProviderReader(id), false);
            } else {
                // full update is performed automatically, when instantiating without a file
                recommender = MasterRecommender.getRecommender(existingStateFile, new JdbcProviderReader(id), false);
                try {
                    recommender.updateIncrementalAndProductsUntilFinished(false);
                }
                catch (InterruptedException e) {
                    System.err.println("Interrupted while updating the existing model");
                    e.printStackTrace();
                    recommender.stop();
                    System.exit(6);
                    return;
                }
            }

            try {
                Thread.sleep(1000L); // delay for a second, so any logs and stuff get out in the right order
            }
            catch (InterruptedException e) {}

            try {
                recommender.saveToFile(filename);
            } catch (InterruptedException e) {
                System.out.println("Interrupted while saving the model to " + filename);
                e.printStackTrace();
                recommender.stop();
                System.exit(7);
                return;
            }

            System.out.println("Program run complete, exiting.");
            recommender.stop();
        }
        finally {
            provider.close(); // so any database threads get stopped even in the case an exception is thrown, otherwise the JVM doesn't exit
        }
    }

    private static String getProperty(final Properties props, final String key, final String defaultValue) {
        if (props != null) {
            final String s = props.getProperty(key);
            if (s != null) return s;
        }
        final String s = System.getProperty(key);
        if (s == null) {
            if (defaultValue == null) throw new MissingArgumentException("Missing property: " + key);
            return defaultValue;
        }
        return s;
    }
}

class MissingArgumentException extends RuntimeException {
    public MissingArgumentException(final String message) {
        super(message);
    }
}