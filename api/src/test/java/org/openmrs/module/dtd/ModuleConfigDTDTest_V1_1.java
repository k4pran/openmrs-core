package org.openmrs.module.dtd;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.w3c.dom.Document;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import java.io.IOException;
import java.io.InputStream;
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

public class ModuleConfigDTDTest_V1_1 {
	
	private static final String[] compatibleVersions = new String[] { "1.1", "1.2", "1.3", "1.4", "1.5", "1.6" };
	
	@ParameterizedTest
	@MethodSource("getCompatibleVersions")
	public void requireModulesWithVersionsAttributeSet(String version) throws ParserConfigurationException, TransformerException, IOException {
		
		List<String> modules = new ArrayList<>();
		modules.add("module1");
		modules.add("module2");
		
		List<Optional<String>> versions = new ArrayList<>();
		versions.add(Optional.of("1.2.3"));
		versions.add(Optional.of("1.2.4"));
		
		Document configXml = withMinimalTags(version)
				.withRequireModules(modules, versions)
				.build();
		
		try (InputStream inputStream = writeToInputStream(configXml)) {
			assertTrue(isValidConfigXml(inputStream));
		}
	}
	
	@ParameterizedTest
	@MethodSource("getCompatibleVersions")
	public void requireModulesWithVersionsAttributeNotSet(String version) throws ParserConfigurationException, TransformerException, IOException {
		
		List<String> modules = new ArrayList<>();
		modules.add("module1");
		modules.add("module2");
		
		List<Optional<String>> versions = new ArrayList<>();
		versions.add(Optional.empty());
		versions.add(Optional.empty());
		
		Document configXml = withMinimalTags(version)
				.withRequireModules(modules, versions)
				.build();
		
		try (InputStream inputStream = writeToInputStream(configXml)) {
			assertTrue(isValidConfigXml(inputStream));
		}
	}
	
	private static Stream<Arguments> getCompatibleVersions() {
		return Arrays.stream(compatibleVersions).map(Arguments::of);
	}
}
