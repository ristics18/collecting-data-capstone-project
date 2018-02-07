
package com.mycompany.collectingdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.Statement;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import twitter4j.HashtagEntity;
import twitter4j.Status;

/**
 * Klasa DBInstance koja se koristi za transakcije sa bazom
 * 
 * @author Srdjan Ristic
 *
 */
public class DBInstance {

	// Instanca klase
	private static DBInstance instance = null;
	// URL baze
	private static final String DB_URL = "jdbc:mysql://localhost/" + Utils.getProperty("DATABASE-NAME")
			+ "?characterEncoding=utf-8&autoReconnect=true&useSSL=false";
	// Objekat Connection
	private static Connection connection = null;
	// Username za konekciju sa bazom
	private static final String USER = Utils.getProperty("DATABASE-USER");
	// Password za konekciju sa bazom
	private static final String PASS = Utils.getProperty("DATABASE-PASSWORD");
	private ObjectMapper mapper = new ObjectMapper();

	/**
	 * Single instanca klase
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static DBInstance getInstance() throws SQLException {
		if (instance == null) {
			instance = new DBInstance();
		}
		return instance;
	}

	/**
	 * Konstruktor klase
	 * 
	 * @throws SQLException
	 */
	private DBInstance() throws SQLException {
		connection = (Connection) DriverManager.getConnection(DB_URL, USER, PASS);
		Statement stat = (Statement) connection.createStatement();
		stat.execute("SET NAMES 'utf8mb4'");
	}

	/**
	 * Metoda koja unosi informacije o tvitu u tweets tabelu
	 * 
	 * @param status
	 * @throws SQLException
	 */
	public void insertTweet(Status status) throws SQLException {
		PreparedStatement pstmt = (PreparedStatement) connection.prepareStatement(
				"INSERT INTO tweets (text, lang, user, create_date, tweet_id, retweet_count, is_retweeted, is_retweet, user_location, user_lang, geolocation, tweet_url, friends_count, followers_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
				Statement.RETURN_GENERATED_KEYS);

		pstmt.setString(1, status.getText());
		pstmt.setString(2, status.getLang());
		pstmt.setString(3, "@" + status.getUser().getScreenName());
		Timestamp timestamp = new Timestamp(status.getCreatedAt().getTime());
		pstmt.setTimestamp(4, timestamp);
		pstmt.setLong(5, status.getId());
		pstmt.setInt(6, status.getRetweetCount());
		pstmt.setInt(7, (status.isRetweeted()) ? 1 : 0);
		pstmt.setInt(8, (status.isRetweet() ? 1 : 0));
		pstmt.setString(9, status.getUser().getLocation());
		pstmt.setString(10, status.getUser().getLang());
		String geolocation = null;
		if (status.getGeoLocation() != null) {
			geolocation = status.getGeoLocation().getLatitude() + "," + status.getGeoLocation().getLongitude();
		}
		pstmt.setString(11, geolocation);
		if (status.getURLEntities() != null) {
			if (status.getURLEntities().length > 0) {
				pstmt.setString(12, status.getURLEntities()[0].getDisplayURL());
			} else {
				pstmt.setString(12, null);
			}
		} else {
			pstmt.setString(12, null);
		}
		pstmt.setInt(13, status.getUser().getFriendsCount());
		pstmt.setInt(14, status.getUser().getFollowersCount());
		pstmt.executeUpdate();
		ResultSet rs = pstmt.getGeneratedKeys();
		if (rs.next()) {
			long last_inserted_id = rs.getInt(1);
			insertHashTags(status.getHashtagEntities(), last_inserted_id);
		}
	}

	/**
	 * Metoda koja iterura kroz sve hestagove jednog tvita i prosledjuje ih na
	 * proveru
	 * 
	 * @param hashtagEntities
	 * @param last_inserted_id
	 * @throws SQLException
	 */
	private void insertHashTags(HashtagEntity[] hashtagEntities, long last_inserted_id) throws SQLException {
		for (HashtagEntity hashTag : hashtagEntities) {
			checkTag(hashTag.getText(), last_inserted_id);
		}
	}

	/**
	 * MMetoda koja vraca ukupan broj hestagova sa prosledjenim imenom hestaga
	 * 
	 * @param tag
	 * @return
	 * @throws SQLException
	 */
	private int getTagNum(String tag) throws SQLException {
		PreparedStatement pstmt = (PreparedStatement) connection
				.prepareStatement("SELECT COUNT(*) FROM tags WHERE name=?");
		pstmt.setString(1, tag);
		ResultSet rs = pstmt.executeQuery();
		int numberOfRows = 0;
		if (rs.next()) {
			numberOfRows = rs.getInt(1);
		}
		return numberOfRows;
	}

	/**
	 * Metoda koja vraca ID hestaga u zavisnosti od prosledjenog imena hestaga
	 * 
	 * @param tag
	 * @return
	 * @throws SQLException
	 */
	private long getTagId(String tag) throws SQLException {
		PreparedStatement pstmt = (PreparedStatement) connection.prepareStatement("SELECT id FROM tags WHERE name=?");
		pstmt.setString(1, tag);
		ResultSet rs = pstmt.executeQuery();
		long id = 0;
		if (rs.next()) {
			id = rs.getInt(1);
		}
		return id;
	}

	/**
	 * Metoda koja unosi ID tvita i ID hestaga u medjutabelu tweets_tags
	 * 
	 * @param tweet_id
	 * @param tag_id
	 * @throws SQLException
	 */
	public void joinHashTagAndTweet(long tweet_id, long tag_id) throws SQLException {
		PreparedStatement pstmt = (PreparedStatement) connection
				.prepareStatement("INSERT INTO tweets_tags (tweet_id, tag_id) VALUES (?,?)");
		pstmt.setLong(1, tweet_id);
		pstmt.setLong(2, tag_id);
		pstmt.execute();
	}

	/**
	 * 
	 * Metoda koja unosi hestag u tabelu tags u zavisnosti od dobijenog
	 * GENERATED_KEYS
	 * 
	 * @param tag
	 * @param last_id
	 * @throws SQLException
	 */
	private void checkTag(String tag, long last_id) throws SQLException {
		if (getTagNum(tag) == 1) {
			long tagId = getTagId(tag);
			joinHashTagAndTweet(last_id, tagId);

		} else {
			PreparedStatement pstmt = (PreparedStatement) connection
					.prepareStatement("INSERT INTO tags (name) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
			pstmt.setString(1, tag);
			pstmt.executeUpdate();
			ResultSet rs = pstmt.getGeneratedKeys();
			long last_id_tags = 0;
			if (rs.next()) {
				last_id_tags = rs.getInt(1);
			}
			if (last_id_tags != 0) {
				joinHashTagAndTweet(last_id, last_id_tags);
			}
		}

	}

	/**
	 * Metoda koja uzima vrednosti iz baze koliko puta su se pojavili hestagovi
	 * iz customHashtags liste. Deli se u 4 sektora, pocevsi od vremena sat
	 * vremena unazad (dodaje se 15 minuta na ovo vreme kako bi se dobile 4
	 * vrednosti)
	 * 
	 * @param customHashtags
	 * @return
	 * @throws SQLException
	 * @throws ParseException
	 * @throws NumberFormatException
	 */
	public ArrayNode occuranceOfCustomTagsWithDefaultOnes(Set<String> customHashtags)
			throws SQLException, ParseException, NumberFormatException {
		int[] iteration = new int[4];

		Date date = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.HOUR, Integer.parseInt(Utils.getProperty("HOURS-BACK-FOR-CUSTOM-TAGS-CHECK")));
		Date dateOneHourBack = calendar.getTime();
		SimpleDateFormat formatDateOneHourBehind = new SimpleDateFormat("HH:mm:ss");
		String timeOneHourPast = formatDateOneHourBehind.format(dateOneHourBack);

		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.HOUR, Integer.parseInt(Utils.getProperty("HOURS-BACK-FOR-CUSTOM-TAGS-CHECK")));
		cal.add(Calendar.MINUTE, Integer.parseInt(Utils.getProperty("MINUTES-FOR-DATA-COLLECTING")));
		Date dateFifteenMinutes = cal.getTime();
		SimpleDateFormat formatFifteenMinutes = new SimpleDateFormat("HH:mm:ss");
		String fifteenMinutesInPastHour = formatFifteenMinutes.format(dateFifteenMinutes);

		ArrayNode arrayNode = mapper.createArrayNode();
		for (int i = 0; i < iteration.length; i++) {
			ObjectNode node = mapper.createObjectNode();
			for (String customHashtag : customHashtags) {
				PreparedStatement pstmt = (PreparedStatement) connection.prepareStatement(
						"select text from tweets where (CAST(create_date as time) BETWEEN ? AND ?) AND text LIKE ?");
				pstmt.setString(1, timeOneHourPast);
				pstmt.setString(2, fifteenMinutesInPastHour);
				pstmt.setString(3, "%" + customHashtag + "%");
				ResultSet rs = pstmt.executeQuery();
				ArrayList<String> textsWithThisTag = new ArrayList<String>();
				while (rs.next()) {
					textsWithThisTag.add(rs.getString("text"));
				}
				if (textsWithThisTag.size() != 0) {
					int numberOfAppearanceOfThisTagWithDT = customTagAppearingWithDefault(customHashtag,
							textsWithThisTag);
					node.put(customHashtag, numberOfAppearanceOfThisTagWithDT);
				} else {
					node.put(customHashtag, 0);
				}
			}
			arrayNode.add(node);
			timeOneHourPast = addFifteenMinutesToTime(timeOneHourPast);
			fifteenMinutesInPastHour = addFifteenMinutesToTime(fifteenMinutesInPastHour);
		}
		return arrayNode;
	}

	/**
	 * Metoda koja iterira kroz listu tekstova i listu pocetnih hestagova u
	 * cilju dobijanja rezultata koliko puta se prosledjeni customHashtag
	 * pojavio sa nekim hestagom iz pocetne liste
	 * 
	 * @param customHashtag
	 * @param textsWithThisTag
	 * @return
	 * @throws SQLException
	 */
	private int customTagAppearingWithDefault(String customHashtag, ArrayList<String> textsWithThisTag)
			throws SQLException {
		Set<String> defaultHashtags = Utils.getDefaultHashtags();
		int count = 0;
		for (String text : textsWithThisTag) {
			defaultTagLoop: for (String defaultTag : defaultHashtags) {
				if (text.toLowerCase().contains(defaultTag.toLowerCase())) {
					count++;
					break defaultTagLoop;
				}
			}
		}
		return count;
	}

	/**
	 * Metoda koja uzima vrednosti iz baze koliko puta su se pojavili hestagovi
	 * iz customHashtags liste sa pocetnim hestagovima. Deli se u 4 sektora,
	 * pocevsi od vremena sat vremena unazad (dodaje se 15 minuta na ovo vreme
	 * kako bi se dobile 4 vrednosti)
	 * 
	 * @param customHashtags
	 * @return
	 * @throws SQLException
	 * @throws ParseException
	 * @throws NumberFormatException
	 */
	public ArrayNode occurranceOfCustomTagsPerFifteenMinues(Set<String> customHashtags)
			throws SQLException, ParseException, NumberFormatException {
		int[] iteration = new int[4];

		Date date = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.HOUR, Integer.parseInt(Utils.getProperty("HOURS-BACK-FOR-CUSTOM-TAGS-CHECK")));
		Date dateOneHourBack = calendar.getTime();
		SimpleDateFormat formatDateOneHourBehind = new SimpleDateFormat("HH:mm:ss");
		String timeOneHourPast = formatDateOneHourBehind.format(dateOneHourBack);

		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		cal.add(Calendar.HOUR, Integer.parseInt(Utils.getProperty("HOURS-BACK-FOR-CUSTOM-TAGS-CHECK")));
		cal.add(Calendar.MINUTE, Integer.parseInt(Utils.getProperty("MINUTES-FOR-DATA-COLLECTING")));
		Date dateFifteenMinutes = cal.getTime();
		SimpleDateFormat formatFifteenMinutes = new SimpleDateFormat("HH:mm:ss");
		String fifteenMinutesInPastHour = formatFifteenMinutes.format(dateFifteenMinutes);

		ArrayNode arrayNode = mapper.createArrayNode();
		for (int i = 0; i < iteration.length; i++) {
			ObjectNode node = mapper.createObjectNode();
			for (String customHashtag : customHashtags) {
				PreparedStatement pstmt = (PreparedStatement) connection.prepareStatement(
						"select text from tweets where (CAST(create_date as time) BETWEEN ? AND ?) AND text LIKE ?");
				pstmt.setString(1, timeOneHourPast);
				pstmt.setString(2, fifteenMinutesInPastHour);
				pstmt.setString(3, "%" + customHashtag + "%");
				ResultSet rs = pstmt.executeQuery();
				ArrayList<String> textsWithThisTag = new ArrayList<String>();
				while (rs.next()) {
					textsWithThisTag.add(rs.getString("text"));
				}
				if (textsWithThisTag.size() != 0) {
					int numberOfAppearanceOfThisTagWithDT = customTagAppearingRangeFifteenMinutes(customHashtag,
							textsWithThisTag);
					node.put(customHashtag, numberOfAppearanceOfThisTagWithDT);
				} else {
					node.put(customHashtag, 0);
				}
			}
			arrayNode.add(node);
			timeOneHourPast = addFifteenMinutesToTime(timeOneHourPast);
			fifteenMinutesInPastHour = addFifteenMinutesToTime(fifteenMinutesInPastHour);
		}
		return arrayNode;
	}

	/**
	 * Metoda koja koja iterira kroz listu tekstova i proverava da koliko
	 * tekstova sadrzi prosledjeni hestag i na osnovu toga vraca ukupnu vrednost
	 * count
	 * 
	 * @param customHashtag
	 * @param textsWithThisTag
	 * @return
	 * @throws SQLException
	 */
	private int customTagAppearingRangeFifteenMinutes(String customHashtag, ArrayList<String> textsWithThisTag)
			throws SQLException {
		int count = 0;
		for (String text : textsWithThisTag) {
			if (text.toLowerCase().contains(customHashtag)) {
				count++;
			}
		}
		return count;
	}

	/**
	 * Metoda koja dodaje 15 minuta na prosledjeni String time
	 * 
	 * @param time
	 * @return
	 * @throws ParseException
	 */
	private String addFifteenMinutesToTime(String time) throws ParseException {
		DateFormat formatter = new SimpleDateFormat("HH:mm:ss");
		Date date = formatter.parse(time);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.MINUTE, 15);
		Date dateFifteenMinutes = calendar.getTime();
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		String result = sdf.format(dateFifteenMinutes);
		return result;
	}
}
