
package com.mycompany.collectingdata;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Klasa ThreadTwitter koja sluzi za interakciju sa Twitter Streaming API
 * slanjem liste hestagova pri cemu se prokupljaju tvitovi
 * 
 * @author Srdjan Ristic
 *
 */
public class ThreadTwitter extends Thread {

	// Loger klase
	private static final Logger LOGGER = Logger.getLogger(ThreadTwitter.class);
	// Twitter Streaming API konfiguracioni parametri za autentikaciju
	private ConfigurationBuilder cb = new ConfigurationBuilder();
	// Twitter stream objekat
	private TwitterStream twitterStream = null;
	// Poslednji filter hestagova
	private String lastFilter = null;
	// Filter hestagova
	private String filter = null;
	// Tajmer
	private Timer timer = new Timer();

	/**
	 * Main pokretacka metoda koja ispisuje datum pokretanja sistema, koja baza
	 * se koristi i pokrece nit ThreadTwitter
	 * 
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		LOGGER.info("DATE - " + dateFormat.format(date));
		LOGGER.info("DATABASE - " + Utils.getProperty("DATABASE-NAME"));
		new ThreadTwitter().start();
	}

	/**
	 * Override run metoda koja proverava autentifikacione parametre sza Twitter
	 * API-jem postavlja odredjene parametre za koriscenje API-ja i postavlja
	 * StatusListener koji ima niz Override-ovanih metoda, od kojih se koristi
	 * samo onStatus koja prima tvit i vrsi njegovu dalju obradu
	 * 
	 */
	@Override
	public void run() {
		cb.setIncludeEntitiesEnabled(true);
		cb.setDebugEnabled(true).setOAuthConsumerKey(Utils.getProperty("OAuthConsumerKey"))
				.setOAuthConsumerSecret(Utils.getProperty("OAuthConsumerSecret"))
				.setOAuthAccessToken(Utils.getProperty("OAuthAccessToken"))
				.setOAuthAccessTokenSecret(Utils.getProperty("OAuthAccessTokenSecret"));
		cb.setIncludeExtAltTextEnabled(true);
		cb.setIncludeEntitiesEnabled(true);
		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		try {
			lastFilter = getFilter();
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
		StatusListener listener = new StatusListener() {

			@Override
			public void onException(Exception arg0) {

			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {

			}

			@Override
			public void onScrubGeo(long arg0, long arg1) {

			}

			@Override
			public void onStatus(Status status) {

				try {
					status = Utils.statusCheck(status);
					if (status != null) {
						DBInstance.getInstance().insertTweet(status);
						CustomHashtagsFilter.collectingThreshold(status);
					}

				} catch (SQLException e) {
					e.printStackTrace();
				}

				if (!lastFilter.equals(filter)) {
					lastFilter = filter;
					reloadStream(this);
				}
			}

			@Override
			public void onTrackLimitationNotice(int arg0) {

			}

			@Override
			public void onStallWarning(StallWarning sw) {
			}

		};

		reloadStream(listener);
	}

	/**
	 * Metoda koja cisti twitterStream, sklanja listener, uzima listu novih
	 * hestagova, kreira novi listener i postavlja listu novih hestagova na novo
	 * kreirani listener za prikupljanje tvitova
	 * 
	 * @param listener
	 */
	public void reloadStream(StatusListener listener) {

		twitterStream.cleanUp();
		twitterStream.removeListener(listener);
		FilterQuery fq = new FilterQuery();

		String keywords[] = { lastFilter };

		fq.track(keywords);

		twitterStream.addListener(listener);
		twitterStream.filter(fq);
	}

	/**
	 * Metoda koja na svakih 15 minuta puni listu customListHashtags, nakon sat
	 * vremena proverava koji hestagovi se trebaju koristit za prikupljanje a
	 * koji ne i na kraju pravi finalnu listu za prikupljanje tvitova i
	 * postavlja filter sa njom
	 * 
	 * @return
	 * @throws SQLException
	 */
	private String getFilter() throws SQLException {
		filter = Utils.getTagsFull();
		LOGGER.info("Full tags: " + filter);
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				try {
					LOGGER.info("Filtering custom hashtags that should to be added...");
					CustomHashtagsFilter.fillCustomHashtagsList();

					if (CustomHashtagsFilter.getCustomHashtagsListFilledTimes() >= 4) {
						LOGGER.info("Checking if custom hashtags are still dominating with default ones...");
						CustomHashtagsFilter.checkDominationOfCustomHashtags();
					}

					LOGGER.info("Adding new hashtags to filter...");
					filter = Utils.getTagsFull();
					LOGGER.info("Full tags: " + filter);
				} catch (SQLException | NumberFormatException | ParseException ex) {
					ex.printStackTrace();
				}
			}
		}, 15 * 60 * 1000, 15 * 60 * 1000); // Na svakih 15 minuta (zbog limita
											// api-ja)
		return filter;
	}
}
