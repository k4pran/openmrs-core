package org.openmrs.module.dtd;

import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;

public class DtdTestValidator {
	
	private DtdTestValidator() {}
	
	public static boolean isValidConfigXml(InputStream xml) {
		try {
			DocumentBuilderFactory domFactory;
			DocumentBuilder builder;
			domFactory = DocumentBuilderFactory.newInstance();
			domFactory.setValidating(true);
			builder = domFactory.newDocumentBuilder();
			final boolean[] isValidConfig = { true };
			
			builder.setErrorHandler(new ErrorHandler() {
				
				@Override
				public void warning(SAXParseException e) {
					isValidConfig[0] = true;
					
				}
				
				@Override
				public void error(SAXParseException e) {
					e.printStackTrace();
					isValidConfig[0] = false;
				}
				
				@Override
				public void fatalError(SAXParseException e) {
					e.printStackTrace();
					isValidConfig[0] = false;
				}
			});
			builder.parse(xml);
			return isValidConfig[0];
		}
		catch (SAXException | IOException | ParserConfigurationException e) {
			System.out.println("Failure reason: " + e.getMessage());
			return false;
		}
	}
}
