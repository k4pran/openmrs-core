/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.xsd;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.w3c.dom.Document;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.openmrs.module.xsd.ConfigXmlBuilder.withMinimalTags;
import static org.openmrs.module.xsd.ConfigXmlBuilder.writeToInputStream;
import static org.openmrs.module.xsd.XsdTestValidator.isValidConfigXml;

public class ModuleConfigXSDV1_3Test {

	private static final String[] compatibleVersions = new String[] { "1.3", "1.4", "1.5", "1.6", "1.7" };

	@ParameterizedTest
	@MethodSource("getCompatibleVersions")
	public void validXmlWhenMandatoryIsSet(String version) throws ParserConfigurationException, TransformerException, IOException, URISyntaxException {
		Document configXml = withMinimalTags(version)
				.withMandatory("true")
				.build();

		try (InputStream inputStream = writeToInputStream(configXml)) {
			assertTrue(isValidConfigXml(inputStream, version));
		}
	}

	@ParameterizedTest
	@MethodSource("getCompatibleVersions")
	public void validXmlWhenMandatoryIsNotSet(String version) throws ParserConfigurationException, TransformerException, IOException, URISyntaxException {
		Document configXml = withMinimalTags(version)
				.build();

		try (InputStream inputStream = writeToInputStream(configXml)) {
			assertTrue(isValidConfigXml(inputStream, version));
		}
	}

	private static Stream<Arguments> getCompatibleVersions() {
		return Arrays.stream(compatibleVersions).map(Arguments::of);
	}
}
