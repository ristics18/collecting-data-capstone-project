package com.mycompany.collectingdata;

/**
 * Klasa koja sluzi za kalkulaciju Spearman korelacije
 * 
 * @author Srdjan Ristic
 *
 */
public class SpearmanCorrelationCalculation {

	/**
	 * Metoda za racunanje i kalkulaciju Spearman korelacije
	 * 
	 * @param xs
	 * @param ys
	 * @return
	 */
	public static double correlation(double[] xs, double[] ys) {
		double[] rankX = new double[xs.length];
		double[] rankY = new double[xs.length];
		double[] difference = new double[xs.length];
		double[] differenceSquared = new double[xs.length];
		int n = xs.length;

		// Kreiranje ranka xs niza
		for (int i = 0; i < n; i++) {
			rankX[i] = fillRank(xs, xs[i], i);
		}

		// Kreiranje ranka ys niza
		for (int i = 0; i < n; i++) {
			rankY[i] = fillRank(ys, ys[i], i);
		}

		// Kreiranje niza razlike za razliku izmedju svakog elemenata niza x i
		// niza y medjusobno
		for (int i = 0; i < n; i++) {
			difference[i] = rankX[i] - rankY[i];
		}

		// Kreiranje niza differenceSquared kvadriranjem svih vrednosti niza
		// razlike
		for (int i = 0; i < n; i++) {
			differenceSquared[i] = difference[i] * difference[i];
		}

		// Kreiranje niza sabiranjem svih vrednosti niza differenceSquared
		double dSquared = 0.0;
		for (int i = 0; i < n; i++) {
			dSquared += differenceSquared[i];
		}

		// Rezultat
		double result = 1 - ((6 * dSquared) / ((n * n * n) - n));
		return result;
	}

	/**
	 * Metoda za postavljanje ranka za svaku od vrednosti u prosledjenom nizu
	 * 
	 * @param ys
	 * @param element
	 * @param position
	 * @return
	 */
	private static int fillRank(double[] ys, double element, int position) {
		int count = 1;
		for (int i = 0; i < ys.length; i++) {
			if (i != position) {
				if (element > ys[i]) {
					count++;
				}
			}
		}
		return count;
	}
}
