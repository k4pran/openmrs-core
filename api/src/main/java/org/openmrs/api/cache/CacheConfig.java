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

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.jsr107.Eh107Configuration;
import org.ehcache.jsr107.EhcacheCachingProvider;
import org.hibernate.cache.spi.RegionFactory;
import org.springframework.cache.CacheManager;
import org.springframework.cache.jcache.JCacheCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * CacheConfig provides a cache manager for the @Cacheable annotation and uses ehCache under the hood.
 * The config of ehCache is loaded from ehcache-api.xml and can be extended by modules through apiCacheConfig.properties.
 * For more details see the wiki page at <a href="https://wiki.openmrs.org/x/IYaEBg">https://wiki.openmrs.org/x/IYaEBg</a>
 */
@Configuration
public class CacheConfig {

	@Bean(name = "apiCacheManager")
	public CacheManager cacheManager() throws URISyntaxException {
		CachingProvider provider = Caching.getCachingProvider(EhcacheCachingProvider.class.getName());

//		List<CacheConfiguration> cacheConfigurations = CachePropertiesUtil.getCacheConfigurations();

		URL configXmlUrl = getClass().getResource("/ehcache-api.xml");
		Objects.requireNonNull(configXmlUrl, "Ehcache configuration file '/ehcache-api.xml' not found in classpath");
		URI configUri = configXmlUrl.toURI();
		
		javax.cache.CacheManager jCacheManager = provider.getCacheManager(
			configUri,
			getClass().getClassLoader()
		);

		JCacheCacheManager springCacheManager = new JCacheCacheManager(jCacheManager);
		
		registerLegacyCaches(jCacheManager);
		
		return springCacheManager;
	}

	private void registerLegacyCaches(javax.cache.CacheManager jCacheManager) {
		List<OpenmrsCacheConfiguration> props = CachePropertiesUtil.getCacheConfigurations();

		for (OpenmrsCacheConfiguration cfg : props) {
			String cacheName = cfg.getProperty("name");

			// 3. Build an Ehcache-3 cache config from your properties

			String maxEntries = cfg.getProperty("maxEntriesLocalHeap");
			
			CacheConfigurationBuilder<Object,Object> builder =
				CacheConfigurationBuilder
					.newCacheConfigurationBuilder(
						Object.class, Object.class,
						ResourcePoolsBuilder.heap(Long.parseLong(maxEntries == null ? "1000" : maxEntries))
					);

			// if you have TTL settings
			if (cfg.getProperty("timeToLiveSeconds") != null) {
				long ttl = Long.parseLong(cfg.getProperty("timeToLiveSeconds"));
				builder = builder
					.withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofSeconds(ttl)));
			}

			// …handle other settings similarly…

			org.ehcache.config.CacheConfiguration<Object,Object> eh3Config = builder.build();

			// 4. Wrap it for JCache…
			javax.cache.configuration.Configuration<Object,Object> jcacheConfig =
				Eh107Configuration.fromEhcacheCacheConfiguration(eh3Config);

			// 5. And create it
			jCacheManager.createCache(cacheName, jcacheConfig);
		}
	}
}
