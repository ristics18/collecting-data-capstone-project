package com.mycompany.collectingdata;

/**
 * Klasa koja sluzi za kalkulaciju Pearson korelacije
 * 
 * @author Srdjan Ristic
 *
 */
public class PearsonCorrelationCalculation {

	/**
	 * Metoda koja vrsi racunanja i kalkilaciju Pearson korelacije
	 * 
	 * @param xs
	 * @param ys
	 * @return
	 */
	public static double correlation(double[] xs, double[] ys) {
		double sx = 0.0;
		double sy = 0.0;
		double sxx = 0.0;
		double syy = 0.0;
		double sxy = 0.0;

		int n = xs.length;

		// Iteracija kroz niz i dobijanje vrednosti potrebne za kalkulaciju
		for (int i = 0; i < n; ++i) {
			double x = xs[i];
			double y = ys[i];

			sx += x;
			sy += y;
			sxx += x * x; // x na kvadrat
			syy += y * y; // y na kvadrat
			sxy += x * y; // x puta y
		}

		// Kovarijansa
		double cov = (sxy / n) - (sx * (sy / n) / n);
		// Sigma x
		double sigmax = Math.sqrt((sxx / n) - (sx * sx / n / n));
		// Sigma y
		double sigmay = Math.sqrt((syy / n) - (sy * sy / n / n));
		// Rezultat
		double result = cov / sigmax / sigmay;

		return result;
	}
}
