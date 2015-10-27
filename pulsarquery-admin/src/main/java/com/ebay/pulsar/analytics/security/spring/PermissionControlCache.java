/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.security.spring;


import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.ebay.pulsar.analytics.dao.model.DBUser;
import com.ebay.pulsar.analytics.service.PermissionConst;
import com.ebay.pulsar.analytics.service.UserPermissionControl;
import com.ebay.pulsar.analytics.service.UserService;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
/**
 * Cache the permissions for users to re-authentication.
 * 
 * @author qxing
 *
 */
public class PermissionControlCache {
	private static final Logger logger = LoggerFactory.getLogger(PermissionControlCache.class);
	
	private ListeningExecutorService EXECUTOR = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

	private static volatile PermissionControlCache instance = null;
	
	private final LoadingCache<String, PulsarSession> cache;
	private final UserPermissionControl permissionControl;
	private final UserService userService;
	
	
	// Cache Size
	private static int SIZE_CACHE = 10000;
	// Expire after 1 hours = 60 min
	private static final int TIME_EXPIRE = 30;
	// Refresh after 1 hours = 60 min
	private static final int TIME_REFRESH = 5;

	public static PermissionControlCache getInstance() {
		if (instance == null) {
			synchronized (PermissionControlCache.class) {
				if (instance == null) {
					instance = new PermissionControlCache();
				}
			}
		}
		return instance;
	}


	private PermissionControlCache() {
		permissionControl=new UserPermissionControl();
		userService=new UserService();
		cache = CacheBuilder.newBuilder()
					.maximumSize(SIZE_CACHE)
					.expireAfterWrite(TIME_EXPIRE, TimeUnit.MINUTES)
					.refreshAfterWrite(TIME_REFRESH, TimeUnit.MINUTES)
					.build( new CacheLoader<String, PulsarSession>() {
						@Override
						public PulsarSession load(String userName) throws Exception {
							return loadFromUnderlying(userName);
						}

						@Override
						public Map<String, PulsarSession> loadAll(Iterable<? extends String> keys) {
							Map<String, PulsarSession> m= Maps.newHashMap();
						    for(String key: keys){
						    	try{
						    	PulsarSession ps=load(key);
						    	m.put(key, ps);
						    	}catch(Exception e){};
						    }
						    return m;
						}

						@Override
						public ListenableFuture<PulsarSession> reload(final String userName, PulsarSession prev) {
							// asynchronous!
							ListenableFutureTask<PulsarSession> task = ListenableFutureTask.create(new Callable<PulsarSession>() {
																		public PulsarSession call() {
																			return loadFromUnderlying(userName);
																		}
																	});
							EXECUTOR.submit(task);
							return task;
						}
					});
	}
	private PulsarSession loadFromUnderlying(String userName){
		DBUser user=userService.getUserByName(userName);
		if(user==null){
			user=new DBUser();
			user.setName(userName);
			user.setPassword(PermissionConst.THIRD_PARTY_AUTHENTICATION_PASSWORD);
			user.setEnabled(true);
		}
		Set<SimpleGrantedAuthority>  auths=permissionControl.getAllRightsForValidUser(user.getName());
		return new PulsarSession(user,auths);
	}
	public PulsarSession getSessions(String userName){
		try {
			return cache.get(userName);
		} catch(Exception t){
			logger.warn("getSessions Exception:",t.getMessage());
		}
		return null;
	}
	public void expireSessions(String userName){
		cache.invalidate(userName);
	}
	public void expireSessionsAll(){
		cache.invalidateAll();
	}
	public void expireSessions(List<String> userNames){
		cache.invalidateAll(userNames);
	}
	public void refresh(String key){
		cache.refresh(key);
	}
	public ImmutableMap<String,PulsarSession> getAll(Iterable<String> keys) throws ExecutionException{
		return cache.getAll(keys);
	}
	
}
