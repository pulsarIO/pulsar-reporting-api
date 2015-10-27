/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.security.spring;

import java.util.Collection;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import com.ebay.pulsar.analytics.dao.model.DBUser;
import com.google.common.collect.ImmutableSet;
/**
 * UserDetailService used by Spring Security to load user permissions and other user details.
 * 
 * @author qxing
 *
 */
public class PulsarUserDetailService implements UserDetailsService {
	@SuppressWarnings("serial")
	@Override
	public UserDetails loadUserByUsername(final String userName)
			throws UsernameNotFoundException, DataAccessException {
				final PulsarSession session=PermissionControlCache.getInstance().getSessions(userName);
				final DBUser user=session==null?null:session.getUser();
				return new UserDetails(){
					@Override
					public Collection<? extends GrantedAuthority> getAuthorities() {
					    Set<SimpleGrantedAuthority> authRights=session==null?ImmutableSet.<SimpleGrantedAuthority>of():session.getAuthorities();//permissionControl.getAllRightsForValidUser(userName);
						return authRights;
					}
					@Override
					public String getPassword() {
						return user.getPassword();
					}

					@Override
					public String getUsername() {
						return user!=null ? user.getName():userName;
					}

					@Override
					public boolean isAccountNonExpired() {
						return true;
					}

					@Override
					public boolean isAccountNonLocked() {
						return true;
					}

					@Override
					public boolean isCredentialsNonExpired() {
						return true;
					}

					@Override
					public boolean isEnabled() {
						return true;
					}
					
				};

	}

}
