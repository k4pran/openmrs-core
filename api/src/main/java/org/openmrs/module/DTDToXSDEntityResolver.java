package org.openmrs.module;

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.IOException;

public class DTDToXSDEntityResolver implements EntityResolver {
	
	@Override
	public InputSource resolveEntity(String publicId, String systemId) throws SAXException, IOException {
		if (systemId.contains("config-1.3.dtd")) {
			// Redirect to an XSD as a String or from a file
			String xsdLocation = "/path/to/config-1.3.xsd";
			return new InputSource(xsdLocation);
		}
		return null; // Use default behavior for other entities
	}
}
