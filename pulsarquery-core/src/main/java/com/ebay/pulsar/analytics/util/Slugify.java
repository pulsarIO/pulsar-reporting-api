/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.util;

import java.text.Normalizer;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author xinxu1
 * 
 **/
public class Slugify {

	private Map<String, String> customReplacements;
	private boolean lowerCase;
	
	public Slugify()  {
		this(true);
	}

	public Slugify(final boolean lowerCase)  {
		setLowerCase(lowerCase);
	}

	public String slugify(String input) {
		if (input == null) {
			return "";
		}

		input = input.trim();

		Map<String, String> customReplacements = getCustomReplacements();
		if (customReplacements != null) {
			for (Entry<String, String> entry : customReplacements.entrySet()) {
				input = input.replace(entry.getKey(), entry.getValue());
			}
		}

		input = Normalizer.normalize(input, Normalizer.Form.NFD)
				.replaceAll("[^0-9a-zA-Z\\s]+", "")
				.replaceAll(" +", "_");

		if (getLowerCase()) {
			input = input.toLowerCase();
		}

		return input;
	}

	public Map<String, String> getCustomReplacements() {
		return customReplacements;
	}

	public void setCustomReplacements(Map<String, String> customReplacements) {
		this.customReplacements = customReplacements;
	}

	public boolean getLowerCase() {
		return lowerCase;
	}

	public void setLowerCase(boolean lowerCase) {
		this.lowerCase = lowerCase;
	}
}
