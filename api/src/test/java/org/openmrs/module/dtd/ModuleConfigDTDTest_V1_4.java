package org.openmrs.module.dtd;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.w3c.dom.Document;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.openmrs.module.dtd.ConfigXmlBuilder.withMinimalTags;
import static org.openmrs.module.dtd.ConfigXmlBuilder.writeToInputStream;
import static org.openmrs.module.dtd.DtdTestValidator.isValidConfigXml;

public class ModuleConfigDTDTest_V1_4 {
	
	private static final String[] compatibleVersions = new String[] {"1.4", "1.5", "1.6" };
	
	@ParameterizedTest
	@MethodSource("getCompatibleVersions")
	public void validXmlWithMultipleAwareOfModules(String version) throws ParserConfigurationException, TransformerException, IOException {
		List<ConfigXmlBuilder.AwareOfModule> awareOfModules = new ArrayList<>();
		awareOfModules.add(new ConfigXmlBuilder.AwareOfModule(Optional.of("mod1"), Optional.of("1.2.3")));
		awareOfModules.add(new ConfigXmlBuilder.AwareOfModule(Optional.of("mod2"), Optional.of("1.2.4")));
		awareOfModules.add(new ConfigXmlBuilder.AwareOfModule(Optional.of("mod3"), Optional.of("1.2.5")));
		
		Document configXml = withMinimalTags(version)
				.withAwareOfModules(awareOfModules)
				.build();
		
		try (InputStream inputStream = writeToInputStream(configXml)) {
			assertTrue(isValidConfigXml(inputStream));
		}
	}
	
	public static String toString(Document doc) {
		try {
			StringWriter sw = new StringWriter();
			TransformerFactory tf = TransformerFactory.newInstance();
			Transformer transformer = tf.newTransformer();
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
			transformer.setOutputProperty(OutputKeys.METHOD, "xml");
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
			
			transformer.transform(new DOMSource(doc), new StreamResult(sw));
			return sw.toString();
		} catch (Exception ex) {
			throw new RuntimeException("Error converting to String", ex);
		}
	}
	
	@ParameterizedTest
	@MethodSource("getCompatibleVersions")
	public void validXmlWithMissingVersion(String version) throws ParserConfigurationException, TransformerException, IOException {
		List<ConfigXmlBuilder.AwareOfModule> awareOfModules = new ArrayList<>();
		awareOfModules.add(new ConfigXmlBuilder.AwareOfModule(Optional.of("mod1"), Optional.empty()));
		awareOfModules.add(new ConfigXmlBuilder.AwareOfModule(Optional.of("mod2"), Optional.of("1.2.4")));
		awareOfModules.add(new ConfigXmlBuilder.AwareOfModule(Optional.of("mod3"), Optional.of("1.2.5")));
		
		Document configXml = withMinimalTags(version)
				.withAwareOfModules(awareOfModules)
				.build();
		
		try (InputStream inputStream = writeToInputStream(configXml)) {
			assertTrue(isValidConfigXml(inputStream));
		}
	}
	
	@ParameterizedTest
	@MethodSource("getCompatibleVersions")
	public void xmlFailsWithNoModules(String version) throws ParserConfigurationException, TransformerException, IOException {
		List<ConfigXmlBuilder.AwareOfModule> awareOfModules = new ArrayList<>();
		
		Document configXml = withMinimalTags(version)
				.withAwareOfModules(awareOfModules)
				.build();
		
		try (InputStream inputStream = writeToInputStream(configXml)) {
			assertFalse(isValidConfigXml(inputStream));
		}
	}
	
	private static Stream<Arguments> getCompatibleVersions() {
		return Arrays.stream(compatibleVersions).map(Arguments::of);
	}
}
