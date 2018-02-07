package com.mycompany.collectingdata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.swing.JOptionPane;

import org.apache.log4j.Logger;

import twitter4j.Status;

/**
 * Klasa koja sadrzi metode koje se cesto koriste kroz sistem
 * 
 * @author Srdjan Ristic
 *
 */
public class Utils {

	// Loger klase
	private static final Logger LOGGER = Logger.getLogger(Utils.class);

	/**
	 * Metoda koja uzima hestagove definisane u application.properties fajlu i
	 * kreira set default-nih hestagova
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static Set<String> getDefaultHashtags() throws SQLException {
		Set<String> set = new HashSet<String>();
		String defaultHashtags = Utils.getProperty("DEFAULT-HASHTAGS");
		if (defaultHashtags == null || defaultHashtags.length() < 1) {
			JOptionPane.showMessageDialog(null, "Default hashtags are not defined!");
			throw new InternalError("Default hashtags are not defined!");
		}
		String[] hashtagsArray = defaultHashtags.split(",");
		for (String tag : hashtagsArray) {
			if (tag.charAt(0) != '#') {
				set.add("#" + tag);
			} else {
				set.add(tag);
			}
		}
		return set;
	}

	/**
	 * Metoda koja konvertuje niz u String
	 * 
	 * @param array
	 * @return
	 */
	public static String convertFromArrayToString(Object[] array) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < array.length; i++) {
			if (i == 0) {
				builder.append(array[0]);
			} else {
				builder.append("," + array[i]);
			}
		}
		return builder.toString();
	}

	/**
	 * Metoda koja sortira mapu po vrednostima
	 * 
	 * @param unsortMap
	 * @return
	 */
	public static Map<String, Integer> sortMapByValue(Map<String, Integer> unsortedMap) {
		List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(unsortedMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				return (o2.getValue()).compareTo(o1.getValue());
			}
		});
		Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		for (Map.Entry<String, Integer> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	/**
	 * Metoda koja ispisuje mapu na konzoli
	 * 
	 * @param map
	 * @throws NumberFormatException
	 */
	public static <K, V> void printMap(Map<K, V> map) throws NumberFormatException {
		int count = 0;
		for (Map.Entry<K, V> entry : map.entrySet()) {
			if (count == Integer.parseInt(Utils.getProperty("TOP-RESULTS-OF-DATA"))) {
				break;
			}
			LOGGER.info(entry.getKey() + " : " + entry.getValue());
			count++;
		}
	}

	/**
	 * Metoda koja konvertuje List<Double> u double[]
	 * 
	 * @param doubles
	 * @return
	 */
	public static double[] convertDoubles(List<Double> doubles) {
		double[] ret = new double[doubles.size()];
		Iterator<Double> iterator = doubles.iterator();
		int i = 0;
		while (iterator.hasNext()) {
			ret[i] = iterator.next();
			i++;
		}
		return ret;
	}

	/**
	 * Metoda koja uzima vrednost iz application.properties fajla
	 * 
	 * @param property
	 * @return
	 */
	public static String getProperty(String property) {
		InputStream inputStream = null;
		String result = null;
		try {
			Properties prop = new Properties();
			String propFileName = "application.properties";

			inputStream = Utils.class.getClassLoader().getResourceAsStream(propFileName);

			if (inputStream != null) {
				prop.load(inputStream);
			} else {
				throw new FileNotFoundException("Property file '" + propFileName + "' not found in the classpath!");
			}
			result = prop.getProperty(property);

		} catch (Exception e) {
			LOGGER.error("Exception: " + e);
		} finally {
			try {
				inputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	/**
	 * Metoda koja proverava prikupljeni tvit, da li je retvit ili je quoted
	 * status. Ovo se proverava kada tvit ne sadrzi pocetne hestagove
	 * 
	 * @param s
	 * @return
	 * @throws SQLException
	 */
	public static Status statusCheck(Status s) throws SQLException {
		boolean switching = true;
		String fullHashtags = getTagsFull();
		String[] hashtagsArray = fullHashtags.toLowerCase().split(",");
		for (String element : hashtagsArray) {
			if (s.getText().toLowerCase().contains(element)) {
				switching = false;
				break;
			}
		}

		if (switching == true) {
			if (s.getQuotedStatus() != null) {
				s = s.getQuotedStatus();
				return s;
			} else if (s.getRetweetedStatus() != null) {
				s = s.getRetweetedStatus();
				return s;
			} else {
				return null;
			}
		}
		return s;
	}

	/**
	 * Metoda koja spaja pocetne hestagove i custom hestagove i vraca ih kao
	 * string
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static String getTagsFull() throws SQLException {
		Set<String> defaultHashtags = Utils.getDefaultHashtags();
		Set<String> customHashtags = CustomHashtagsFilter.getCustomHashtags();
		if (!defaultHashtags.isEmpty()) {
			defaultHashtags.addAll(customHashtags);
		}
		String fullTagsAsString = Utils.convertFromArrayToString(defaultHashtags.toArray());
		return fullTagsAsString;
	}

}
