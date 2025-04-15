/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.api.cache;

import org.ehcache.jsr107.EhcacheCachingProvider;
import org.springframework.cache.CacheManager;
import org.springframework.cache.jcache.JCacheCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Objects;

/**
 * CacheConfig provides a cache manager for the @Cacheable annotation and uses ehCache under the hood.
 * The config of ehCache is loaded from ehcache-api.xml and can be extended by modules through apiCacheConfig.properties.
 * For more details see the wiki page at <a href="https://wiki.openmrs.org/x/IYaEBg">https://wiki.openmrs.org/x/IYaEBg</a>
 */
@Configuration
public class CacheConfig {

//    @Bean(name = "apiCacheManagerFactoryBean")
//    public EhCacheManagerFactoryBean apiCacheManagerFactoryBean() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
//		CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
//			.build(); 
//		
//		OpenmrsCacheManagerFactoryBean cacheManagerFactoryBean = new OpenmrsCacheManagerFactoryBean();
//        cacheManagerFactoryBean.setConfigLocation(new ClassPathResource("ehcache-api.xml"));
//        cacheManagerFactoryBean.setShared(false);
//        cacheManagerFactoryBean.setAcceptExisting(true);
//
//        return cacheManagerFactoryBean;
//    }

	@Bean
	public CacheManager cacheManager() throws URISyntaxException {
		CachingProvider provider = Caching.getCachingProvider(EhcacheCachingProvider.class.getName());

		URL configXmlUrl = getClass().getResource("/ehcache-api.xml");
		Objects.requireNonNull(configXmlUrl, "Ehcache configuration file '/ehcache-api.xml' not found in classpath");
		URI configUri = configXmlUrl.toURI();
		
		javax.cache.CacheManager jCacheManager = provider.getCacheManager(
			configUri,
			getClass().getClassLoader()
		);
		return new JCacheCacheManager(jCacheManager);
	}


}
