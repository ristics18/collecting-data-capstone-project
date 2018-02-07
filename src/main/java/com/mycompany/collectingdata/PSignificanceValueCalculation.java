package com.mycompany.collectingdata;

import org.apache.commons.math3.distribution.TDistribution;
import org.apache.log4j.Logger;

/**
 * Klasa koja racuna vrednost znacajnosti (p value)
 * 
 * @author Srdjan Ristic
 *
 */
public class PSignificanceValueCalculation {

	// Loger klase
	private static final Logger LOGGER = Logger.getLogger(PSignificanceValueCalculation.class);

	/**
	 * Metoda za racunanje i kalkilaciju p vrednosti
	 * 
	 * @param correlation
	 * @param n
	 * @return
	 * @throws NumberFormatException
	 */
	public static boolean calculate(double correlation, int n) throws NumberFormatException {
		// Studentova t vrednost (Student's t-distribution)
		double t = correlation * Math.sqrt((n - 2) / (1 - correlation * correlation));
		// Vrednost p
		double p = 1 - new TDistribution(n - 1).cumulativeProbability(t);
		LOGGER.info("Significance value P: " + p);
		if (Integer.parseInt(Utils.getProperty("NUMBER-OF-TAILS")) == 1) {
			return p <= Double.parseDouble(Utils.getProperty("SIGNIFICANCE-LEVEL"));
		} else {
			return 2 * p <= Double.parseDouble(Utils.getProperty("SIGNIFICANCE-LEVEL"));
		}
	}
}
