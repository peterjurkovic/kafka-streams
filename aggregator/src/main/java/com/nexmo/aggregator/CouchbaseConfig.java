package com.nexmo.aggregator;

import java.util.List;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@ConditionalOnProperty(prefix = "couchbaase", name = "enabled", havingValue = "true")
@Configuration
public class CouchbaseConfig implements DisposableBean {
	
	@Value("${couchbase.host:localhost}") 
	List<String> hosts;
	
	@Value("${couchbase.bucket:chatapp}") 
	String bucket;
	
	@Value("${couchbase.password:chatapp}") 
	String password;
	
	Cluster cluster;
	@Bean
	public Cluster cluster() {
		log.info("Couchabase hosts {}", hosts);
		cluster =  CouchbaseCluster.create(hosts);
		return cluster;
	}
	
	@Bean
	public Bucket bucker(Cluster cluster) {
		return cluster.openBucket(bucket, password);
	}

	@Override
	public void destroy() throws Exception {
		if (cluster != null) cluster.disconnect();
	}
	
}
