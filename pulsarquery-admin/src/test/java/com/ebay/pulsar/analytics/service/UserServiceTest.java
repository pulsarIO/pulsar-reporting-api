/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/
package com.ebay.pulsar.analytics.service;

import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.security.authentication.encoding.Md5PasswordEncoder;

import com.ebay.pulsar.analytics.dao.RDBMS;
import com.ebay.pulsar.analytics.dao.mapper.DBUserMapper;
import com.ebay.pulsar.analytics.dao.model.DBUser;
import com.ebay.pulsar.analytics.dao.service.BaseDBService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
@RunWith(PowerMockRunner.class)  //1
@PrepareForTest({GeneratedKeyHolder.class,BaseDBService.class})
public class UserServiceTest {
	private UserService userService=new UserService();
	
    @SuppressWarnings("unchecked")
	@Test
    public void testDb() throws Exception{
    	DBUser user=new DBUser();
    	user.setName("userTest");
    	user.setPassword("test");
    	user.setEmail("testMail");
    	user.setComment("testComment");
    	user.setImage("image");
    	
    	DBUser user2=new DBUser();
    	user2.setName("userTest2");
    	user2.setPassword("test");
    	user2.setEmail("testMail");
    	user2.setComment("testComment");
    	user2.setImage("image");
    	
    	RDBMS db = Mockito.mock(RDBMS.class);		
		final GeneratedKeyHolder keyHolder = PowerMockito.mock(GeneratedKeyHolder.class);	
		PowerMockito.whenNew(GeneratedKeyHolder.class).withNoArguments()
		.thenReturn(keyHolder);
		when(keyHolder.getKey()).thenReturn(1L).thenReturn(2L);
		//GroupService groupService = new GroupService();
		BaseDBService<?> uss=(BaseDBService<?>)ReflectFieldUtil.getField(userService, "userService");
		ReflectFieldUtil.setField(BaseDBService.class,uss, "db", db);
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", "userTest")), Matchers.eq(-1),Matchers.any(DBUserMapper.class)))
		.thenReturn(Lists.<DBUser>newArrayList());
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", "userTest2")), Matchers.eq(-1),Matchers.any(DBUserMapper.class)))
		.thenReturn(Lists.<DBUser>newArrayList());
		when(db.insert(Mockito.anyString(), Matchers.anyMap(), Matchers.any(GeneratedKeyHolder.class)))
		.thenReturn(1);
		//userService.addUser(user);
		int r1=userService.addUsers(Lists.newArrayList(user,user2));
		Assert.assertTrue(r1==2);
		Assert.assertEquals(new Long(1L), user.getId());
		Assert.assertEquals(new Long(2L), user2.getId());
		
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBUserMapper.class)))
		.thenReturn(Lists.newArrayList(user,user2));
		List<String> users=userService.getAllUsers();
		Assert.assertEquals(Lists.newArrayList("userTest","userTest2"), users);
		
		when(db.query(Matchers.anyString(),Matchers.eq(ImmutableMap.of("name", "userTest")), Matchers.eq(-1),Matchers.any(DBUserMapper.class)))
		.thenReturn(Lists.<DBUser>newArrayList(user));
		DBUser u1=userService.getUserByName("userTest");
		Assert.assertEquals(user, u1);

		when(db.delete(Mockito.anyString(), Matchers.eq(ImmutableMap.of("name","userTest"))))
		.thenReturn(1);
		when(db.delete(Mockito.anyString(), Matchers.eq(ImmutableMap.of("name","userTest2"))))
		.thenReturn(1);
		int rows=userService.deleteUsers(Lists.newArrayList("userTest","userTest2"));
		Assert.assertEquals(2, rows);
		
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBUserMapper.class)))
		.thenReturn(Lists.newArrayList());
		try{
			userService.isValidUser("tt", "tt");
		}catch(Exception notexist){
			Assert.assertTrue(true);
		}
		when(db.query(Matchers.anyString(),Matchers.anyMap(), Matchers.eq(-1),Matchers.any(DBUserMapper.class)))
		.thenReturn(Lists.newArrayList(user));
		Md5PasswordEncoder encoder = new Md5PasswordEncoder();
		Assert.assertTrue(userService.isValidUser("userTest", encoder.encodePassword("test",null)));
		Assert.assertTrue(!userService.isValidUser("userTest", "test2"));
    }
    
    
}
