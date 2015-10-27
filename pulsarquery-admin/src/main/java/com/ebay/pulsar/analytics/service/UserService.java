/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.service;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Date;
import java.util.List;

import org.springframework.security.authentication.encoding.Md5PasswordEncoder;
import org.springframework.stereotype.Service;

import com.ebay.pulsar.analytics.dao.model.DBUser;
import com.ebay.pulsar.analytics.dao.service.DBUserService;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

/**
 * @author qxing
 * 
 **/
@Service
public class UserService {
	private DBUserService userService;
	private Md5PasswordEncoder encoder = new Md5PasswordEncoder();
	public UserService() {
		userService = new DBUserService();
	}

	public long addUser(DBUser user) {
		checkNotNull(user);
		checkArgument(user.getName() != null && !"".equals(user.getName()),
				"user name could not be empty.");
		if (user.getCreateTime() == null)
			user.setCreateTime(new Date());
		DBUser condition = new DBUser();
		condition.setName(user.getName());
		user.setPassword(encoder.encodePassword(user.getPassword(), null));
		List<DBUser> list = userService.get(condition);
		checkState(list == null || list.size() == 0,
				"[%s] user name already exists.", user.getName());
		long id = userService.inser(user);
		checkState(id > 0, "add user to db failed.menu=%s", user.getName());
		user.setId(id);
		return id;
	}

	public int addUsers(List<DBUser> DBUsers) {
		int rows=0;
		for (DBUser DBUser : DBUsers) {
			try{
				long id=addUser(DBUser);
				if(id>0) {
					rows++;
				}
			}catch(Exception e){
				
			}
		}
		return rows;
	}

	public int deleteUser(String userName) {
		checkNotNull(userName);
		checkArgument(!"".equals(userName),"userName name could not be empty.");
		DBUser condition = new DBUser();
		condition.setName(userName);
		int id = userService.deleteBatch(condition);
		return id;
	}

	public int deleteUsers(List<String> userNames) {
		int rows = 0;
		for (String userName : userNames) {
			rows += deleteUser(userName);
		}
		return rows;
	}

	public List<String> getAllUsers() {

		return FluentIterable.from(userService.getAll())
				.transform(new Function<DBUser, String>() {
					public String apply(DBUser input) {
						return input.getName();
					}
				}).toList();

	}

	public boolean isValidUser(String name, String info){
		DBUser condition=new DBUser();
		condition.setName(name);
		List<DBUser> list=userService.get(condition);
		checkState(list.size()==1,"[%s] user name dosen't exists.", name);
		if(info.equals(list.get(0).getPassword())) 
			return true;
		else 
			return false;
 	}

	public DBUser getUserByName(String name) {
		DBUser condition = new DBUser();
		condition.setName(name);
		List<DBUser> list = userService.get(condition);
		if(list!=null && list.size()>0)
			return list.get(0);
		return null;
	}
}
