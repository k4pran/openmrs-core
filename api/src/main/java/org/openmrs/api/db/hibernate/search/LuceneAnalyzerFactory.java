/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.api.db.hibernate.search;

import static org.openmrs.api.db.hibernate.search.LuceneAnalyzers.CONCEPT_NAME_ANALYZER;

import org.apache.lucene.analysis.classic.ClassicFilterFactory;
import org.apache.lucene.analysis.core.KeywordTokenizerFactory;
import org.apache.lucene.analysis.core.LowerCaseFilterFactory;
import org.apache.lucene.analysis.core.WhitespaceTokenizerFactory;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilterFactory;
import org.apache.lucene.analysis.ngram.EdgeNGramFilterFactory;
import org.apache.lucene.analysis.ngram.NGramFilterFactory;
import org.apache.lucene.analysis.phonetic.PhoneticFilterFactory;
import org.apache.lucene.analysis.standard.StandardTokenizerFactory;
import org.hibernate.search.backend.lucene.analysis.LuceneAnalysisConfigurationContext;
import org.hibernate.search.backend.lucene.analysis.LuceneAnalysisConfigurer;

/**
 * Provides a Lucene SearchMapping for any objects in openmrs-core.
 * 
 * Objects such as PersonName can use the analyzers provided by this mapping to make their fields searchable.
 * This class defines some default analyzers:
 * 	phraseAnalyzer, which allows searching for an entire phrase, including whitespace
 * 	startAnalyzer, which allows searching for tokens that match at the beginning
 * 	exactAnalyzer, which allows searching for tokens that are identical
 * 	anywhereAnalyzer, which allows searching for text within tokens
 *
 * @since 2.4.0
 */
public class LuceneAnalyzerFactory implements LuceneAnalysisConfigurer {

	@Override
	public void configure(LuceneAnalysisConfigurationContext context) {
		context.analyzer(LuceneAnalyzers.PHRASE_ANALYZER).custom()
			.tokenizer(KeywordTokenizerFactory.class)
			.tokenFilter(ClassicFilterFactory.class)
			.tokenFilter(LowerCaseFilterFactory.class)
			.tokenFilter(ASCIIFoldingFilterFactory.class);

		context.analyzer(LuceneAnalyzers.EXACT_ANALYZER).custom()
			.tokenizer(WhitespaceTokenizerFactory.class)
			.tokenFilter(ClassicFilterFactory.class)
			.tokenFilter(LowerCaseFilterFactory.class)
			.tokenFilter(ASCIIFoldingFilterFactory.class);

		context.analyzer(LuceneAnalyzers.START_ANALYZER).custom()
			.tokenizer(WhitespaceTokenizerFactory.class)
			.tokenFilter(ClassicFilterFactory.class)
			.tokenFilter(LowerCaseFilterFactory.class)
			.tokenFilter(ASCIIFoldingFilterFactory.class)
			.tokenFilter(EdgeNGramFilterFactory.class)
			.param("minGramSize", "2")
			.param("maxGramSize", "20");

		context.analyzer(LuceneAnalyzers.ANYWHERE_ANALYZER).custom()
			.tokenizer(WhitespaceTokenizerFactory.class)
			.tokenFilter(ClassicFilterFactory.class)
			.tokenFilter(LowerCaseFilterFactory.class)
			.tokenFilter(ASCIIFoldingFilterFactory.class)
			.tokenFilter(NGramFilterFactory.class)
			.param("minGramSize", "2")
			.param("maxGramSize", "20");

		context.analyzer(LuceneAnalyzers.SOUNDEX_ANALYZER).custom()
			.tokenizer(StandardTokenizerFactory.class)
			.tokenFilter(ClassicFilterFactory.class)
			.tokenFilter(LowerCaseFilterFactory.class)
			.tokenFilter(PhoneticFilterFactory.class)
			.param("encoder", "Soundex");

		context.analyzer(CONCEPT_NAME_ANALYZER).custom()
			.tokenizer(StandardTokenizerFactory.class)
			.tokenFilter(LowerCaseFilterFactory.class)
			.tokenFilter(ASCIIFoldingFilterFactory.class);
	}
}

