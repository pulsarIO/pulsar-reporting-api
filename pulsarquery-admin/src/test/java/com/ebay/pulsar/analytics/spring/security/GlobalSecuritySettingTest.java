/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.spring.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.security.access.intercept.aopalliance.MethodSecurityInterceptor;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import com.ebay.pulsar.analytics.dao.model.DBUser;
import com.ebay.pulsar.analytics.security.spring.GlobalSecuritySetting;
import com.ebay.pulsar.analytics.security.spring.PermissionControlCache;
import com.ebay.pulsar.analytics.security.spring.PlainTextBasicAuthenticationEntryPoint;
import com.ebay.pulsar.analytics.security.spring.PulsarSession;
import com.ebay.pulsar.analytics.security.spring.PulsarUserDetailService;
import com.ebay.pulsar.analytics.service.PermissionConst;
import com.ebay.pulsar.analytics.service.ReflectFieldUtil;
import com.ebay.pulsar.analytics.service.UserPermissionControl;
import com.ebay.pulsar.analytics.service.UserService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
@RunWith(PowerMockRunner.class)  //1
public class GlobalSecuritySettingTest {
    @Test
    public void testUtils() throws Exception{
    	
    	MethodSecurityInterceptor interceptor = Mockito.mock(MethodSecurityInterceptor.class);		
    	when(interceptor.isAlwaysReauthenticate()).thenReturn(false).thenReturn(true);
    	GlobalSecuritySetting gss=new GlobalSecuritySetting();
    	ReflectFieldUtil.setField(gss, "interceptor", interceptor);
    	gss.afterPropertiesSet();
    	Assert.assertTrue(interceptor.isAlwaysReauthenticate());
    }
    
    @Test
    public void testSecuritySpringPackage(){
    	
    }
    
    @Test
    public void testPulsarSession(){
    	DBUser user=new DBUser();
    	Set<SimpleGrantedAuthority> authorities=Sets.newHashSet();
    	PulsarSession ps=new PulsarSession(user,authorities);
    	assertEquals(ps.getUser(),user);
    	assertEquals(ps.getAuthorities(),authorities);
    	
    }
    @Test
    public void testUserDetailsService() throws Exception{
    	UserPermissionControl permissionControl=Mockito.mock(UserPermissionControl.class);
    	UserService userService=Mockito.mock(UserService.class);
    	Set<SimpleGrantedAuthority> auths=Sets.newHashSet(
				new SimpleGrantedAuthority("D1_VIEW"),
				new SimpleGrantedAuthority("D2_VIEW"),
				new SimpleGrantedAuthority("D2_MANAGE"),
				new SimpleGrantedAuthority("ADD_DATASOURCE")
				);
    	when(permissionControl.getAllRightsForValidUser("qxing"))
    	.thenReturn(auths);
    	
    	DBUser user=new DBUser();
    	user.setCreateTime(new Date());
    	user.setName("qxing");
    	user.setPassword("test");
    	when(userService.getUserByName("qxing"))
    	.thenReturn(user);
    	PermissionControlCache instance=PermissionControlCache.getInstance();
    	ReflectFieldUtil.setField(instance, "userService", userService);
    	ReflectFieldUtil.setField(instance, "permissionControl", permissionControl);
    	ReflectFieldUtil.setField(instance, "instance", instance);
    	
    	PulsarUserDetailService detailService=new PulsarUserDetailService();
    	UserDetails ud=detailService.loadUserByUsername("qxing");
    	assertNotNull(ud);
    	assertEquals(ud.getPassword(),"test");
    	assertEquals(ud.getUsername(),"qxing");
    	assertTrue(ud.isAccountNonExpired());
    	assertTrue(ud.isAccountNonLocked());
    	assertTrue(ud.isCredentialsNonExpired());
    	assertTrue(ud.isEnabled());
    	assertEquals(ud.getAuthorities(),auths);
    	
    	ud=detailService.loadUserByUsername("qxing2");
    	assertNotNull(ud);
    	assertEquals(ud.getPassword(),PermissionConst.THIRD_PARTY_AUTHENTICATION_PASSWORD);
    	assertEquals(ud.getUsername(),"qxing2");
    	assertTrue(ud.isAccountNonExpired());
    	assertTrue(ud.isAccountNonLocked());
    	assertTrue(ud.isCredentialsNonExpired());
    	assertTrue(ud.isEnabled());
    	assertEquals(ud.getAuthorities(),Sets.newHashSet());
    	
    	instance.getAll(Lists.newArrayList("qxing"));
    	instance.refresh("qxing");
    	instance.getAll(Lists.newArrayList("qxing","qxing3"));
    }
    @Test
    public void testPlainTextBasicAuthenticationEntryPoint() throws IOException, ServletException{
    	HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    	HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
    	when(response.getWriter()).thenReturn(new PrintWriter(System.out));
    	PlainTextBasicAuthenticationEntryPoint point=new PlainTextBasicAuthenticationEntryPoint();
    	point.setRealmName("Test query");
    	point.commence(request, response, new AuthenticationException("Test Exception message"){

			/**
			 * 
			 */
			private static final long serialVersionUID = 8643273901705238777L;
    		
    	});
    	
    }
}
