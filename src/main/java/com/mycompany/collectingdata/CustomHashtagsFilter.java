package com.mycompany.collectingdata;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import twitter4j.HashtagEntity;
import twitter4j.Status;

/**
 * Klasa koja se koristi za filterovanje i obradu hestagova pomocu korelacija
 * 
 * @author Srdjan Ristic
 *
 */
public class CustomHashtagsFilter {

	// Loger klase
	private static final Logger LOGGER = Logger.getLogger(CustomHashtagsFilter.class);
	// Mapa za hestagove koji se zajedno pojavljuju
	private static HashMap<String, Integer> cooccurrencesBetweenHashtags = new HashMap<>();
	// Set hestagova koji se privremeno cuvaju
	private static Set<String> customHashtags = new HashSet<String>();
	// Brojac koji racuna koliko tvitova je prikupljeno za 15 minuta
	private static int numberOfTweetsReceived = 0;
	// Brojac koji sluzi da broji koliko puta je proslo po 15 minuta
	private static int customHashtagsListFilledTimes = 0;

	/**
	 * Metoda koja pravi threshold za hestagove koji su se zajedno pojavili
	 * 
	 * @param status
	 * @throws SQLException
	 */
	public static void collectingThreshold(Status status) throws SQLException {
		HashtagEntity[] allTags = status.getHashtagEntities();
		findCooccurrencyBetweenHashtags(allTags);
		numberOfTweetsReceived++;
	}

	/**
	 * Metoda koja pronalazi pojavljivanje pocetnih hestagova sa hestagovima iz
	 * tvita u dodaje ih u mapu
	 * 
	 * @param allTags
	 * @throws SQLException
	 */
	private static void findCooccurrencyBetweenHashtags(HashtagEntity[] allTags) throws SQLException {
		Set<String> defaultHashtags = Utils.getDefaultHashtags();
		for (String defTag : defaultHashtags) {
			defTag = defTag.substring(1).toLowerCase();
			for (HashtagEntity entity : allTags) {
				if (!defTag.equals(entity.getText().toLowerCase())
						&& !defaultHashtags.contains("#" + entity.getText().toLowerCase())
						&& checkIfThisDefaultTagIsInTweet(defTag, allTags) == true) {
					String combined = defTag + "-" + entity.getText().toLowerCase();
					addToHashMap(combined, cooccurrencesBetweenHashtags);
				}
			}
		}
	}

	/**
	 * Metoda koja proverava da li postoji pocetni hestag u listi hestagova iz
	 * tvita
	 * 
	 * @param tag
	 * @param allTags
	 * @return
	 */
	private static boolean checkIfThisDefaultTagIsInTweet(String tag, HashtagEntity[] allTags) {
		for (HashtagEntity entity : allTags) {
			if (tag.equals(entity.getText().toLowerCase())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Metoda koja dodaje 2 povezana hestaga u mapu
	 * 
	 * @param tag
	 * @param hashMapName
	 */
	private static void addToHashMap(String tag, HashMap<String, Integer> hashMapName) {
		Integer count = hashMapName.get(tag);
		if (count == null) {
			count = 1;
		} else {
			++count;
		}
		hashMapName.put(tag, count);
	}

	/**
	 * Metoda koja dodaje popunjava listu privremenih hestagova i resetuje neke
	 * od verijabli
	 * 
	 * @throws SQLException
	 * @throws NumberFormatException
	 */
	public static void fillCustomHashtagsList() throws SQLException, NumberFormatException {
		Map<String, Integer> sortedCooccurrencesBetweenTags = Utils.sortMapByValue(cooccurrencesBetweenHashtags);
		LOGGER.info("Number of tweets received for 15 minutes: " + numberOfTweetsReceived);
		LOGGER.info("Top 5 cooccurrences between hastags:");
		Utils.printMap(sortedCooccurrencesBetweenTags);
		addToCustomHashtagSet(sortedCooccurrencesBetweenTags);
		customHashtagsListFilledTimes++;
		numberOfTweetsReceived = 0;
		cooccurrencesBetweenHashtags.clear();
	}

	/**
	 * Metoda koja dodaje hestagove iz liste frekventnosti hestagova u listu
	 * privremenih hestagova za prikuplje tvitova
	 * 
	 * @param sortedFrequencyOfTags
	 * @throws SQLException
	 * @throws NumberFormatException
	 */
	private static void addToCustomHashtagSet(Map<String, Integer> sortedFrequencyOfTags)
			throws SQLException, NumberFormatException {
		if (customHashtags.size() < Integer.parseInt(Utils.getProperty("TOTAL-NUMBER-OF-FULL-TAGS"))) {
			int count = 0;
			for (Map.Entry<String, Integer> entry : sortedFrequencyOfTags.entrySet()) {
				String hashtag = entry.getKey().substring(entry.getKey().lastIndexOf("-") + 1);
				if (count == Integer.parseInt(Utils.getProperty("TOP-RESULTS-OF-DATA"))) {
					break;
				} else {
					if (Utils.getDefaultHashtags().contains("#" + hashtag)) {
						continue;
					} else {
						customHashtags.add("#" + hashtag);
						count++;
					}
				}
			}
		}
	}

	/**
	 * Metoda koja uzima vrednosti dobijene iz baze i prosledjuje ih za
	 * kalkulaciju korelacije
	 * 
	 * @throws SQLException
	 * @throws ParseException
	 * @throws NumberFormatException
	 */
	public static void checkDominationOfCustomHashtags() throws SQLException, ParseException, NumberFormatException {
		ArrayNode customHashtagsAppearingWithDefault = DBInstance.getInstance()
				.occuranceOfCustomTagsWithDefaultOnes(customHashtags);
		LOGGER.info(customHashtagsAppearingWithDefault);

		ArrayNode customHashtagsAppearingAlone = DBInstance.getInstance()
				.occurranceOfCustomTagsPerFifteenMinues(customHashtags);
		LOGGER.info(customHashtagsAppearingAlone);
		calculatingCorrelation(customHashtagsAppearingWithDefault, customHashtagsAppearingAlone);
	}

	/**
	 * Metoda koja pravi nizove sa decimalnim vrednostima, salje ih na proveru
	 * korelacije za svaki od hestagova i brise hestagove iz liste privremenih
	 * hestagova koji ne zadovoljavaju uslove korelacije
	 * 
	 * @param customHashtagsAppearingWithDefault
	 * @param customHashtagsAppearingAlone
	 */
	private static void calculatingCorrelation(ArrayNode customHashtagsAppearingWithDefault,
			ArrayNode customHashtagsAppearingAlone) {
		ArrayList<Double> array1 = new ArrayList<Double>();
		ArrayList<Double> array2 = new ArrayList<Double>();
		ArrayList<String> deleteFromHashtagsList = new ArrayList<String>();

		for (String customHashtag : customHashtags) {
			for (JsonNode s : customHashtagsAppearingWithDefault) {
				array1.add(Double.parseDouble(s.get(customHashtag).asText()));
			}
			for (JsonNode s : customHashtagsAppearingAlone) {
				array2.add(Double.parseDouble(s.get(customHashtag).asText()));
			}
			deleteFromHashtagsList.add(findCorrelationForHashtag(array1, array2, customHashtag));
		}
		for (String tag : deleteFromHashtagsList) {
			customHashtags.remove(tag);
		}
	}

	/**
	 * Metoda koja odredjuje Pearson i Spearman korelacije za 2 prosledjena
	 * niza, racuna vrednost znacajnosti (p vrednost) i u zavisnosti od tih
	 * rezultat vraca da li hestag treba brisati ili ne
	 * 
	 * @param array1
	 * @param array2
	 * @param customHashtag
	 * @return
	 */
	private static String findCorrelationForHashtag(ArrayList<Double> array1, ArrayList<Double> array2,
			String customHashtag) {
		double[] arrayOneConverted = Utils.convertDoubles(array1);
		double[] arrayTwoConverted = Utils.convertDoubles(array2);
		LOGGER.info("TAG : " + customHashtag);

		if (Utils.getProperty("CORRELATION").toLowerCase().equals("pearson")) {
			double resultCorrelationPearson = PearsonCorrelationCalculation.correlation(arrayOneConverted,
					arrayTwoConverted);
			LOGGER.info("RESULT CORRELATION Pearson: " + resultCorrelationPearson);
			boolean pValueMeetsTheLimit = PSignificanceValueCalculation.calculate(resultCorrelationPearson,
					arrayOneConverted.length);
			if (resultCorrelationPearson < Double.parseDouble(Utils.getProperty("MIN-CORRELATION"))
					&& pValueMeetsTheLimit == false) {
				return customHashtag;
			}
		} else {
			double resultCorrelationSpearman = SpearmanCorrelationCalculation.correlation(arrayOneConverted,
					arrayTwoConverted);
			LOGGER.info("RESULT CORRELATION Spearman: " + resultCorrelationSpearman);
			boolean pValueMeetsTheLimit = PSignificanceValueCalculation.calculate(resultCorrelationSpearman,
					arrayOneConverted.length);
			if (resultCorrelationSpearman < Double.parseDouble(Utils.getProperty("MIN-CORRELATION"))
					&& pValueMeetsTheLimit == false) {
				return customHashtag;
			}
		}
		return null;
	}

	/**
	 * Geter za customHashtags set
	 * 
	 * @return
	 */
	public static Set<String> getCustomHashtags() {
		return customHashtags;
	}

	/**
	 * Seter za customHashtags set
	 * 
	 * @param customHashtags
	 */
	public static void setCustomHashtags(Set<String> customHashtags) {
		CustomHashtagsFilter.customHashtags = customHashtags;
	}

	/**
	 * Geter za customHashtagsListFilledTimes int
	 * 
	 * @return
	 */
	public static int getCustomHashtagsListFilledTimes() {
		return customHashtagsListFilledTimes;
	}

	/**
	 * Seter za customHashtagsListFilledTimes int
	 * 
	 * @param customHashtagsListFilledTimes
	 */
	public static void setCustomHashtagsListFilledTimes(int customHashtagsListFilledTimes) {
		CustomHashtagsFilter.customHashtagsListFilledTimes = customHashtagsListFilledTimes;
	}

}
