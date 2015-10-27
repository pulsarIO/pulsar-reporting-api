/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.resources;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.DataTruncation;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;




import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.www.BasicAuthenticationEntryPoint;
import org.springframework.stereotype.Component;

import com.ebay.pulsar.analytics.Dashboard;
import com.ebay.pulsar.analytics.auth.exceptions.InvalidSessionException;
import com.ebay.pulsar.analytics.dao.model.DBDashboard;
import com.ebay.pulsar.analytics.dao.model.DBDataSource;
import com.ebay.pulsar.analytics.dao.model.DBGroup;
import com.ebay.pulsar.analytics.dao.model.DBRightGroup;
import com.ebay.pulsar.analytics.dao.model.DBUser;
import com.ebay.pulsar.analytics.datasource.DataSourceTypeEnum;
import com.ebay.pulsar.analytics.exception.DataSourceConfigurationException;
import com.ebay.pulsar.analytics.exception.DataSourceException;
import com.ebay.pulsar.analytics.service.DashboardService;
import com.ebay.pulsar.analytics.service.DataSourceService;
import com.ebay.pulsar.analytics.service.GroupService;
import com.ebay.pulsar.analytics.service.PermissionConst;
import com.ebay.pulsar.analytics.service.UserPermissionControl;
import com.ebay.pulsar.analytics.service.UserService;
import com.ebay.pulsar.analytics.util.JsonUtil;
import com.ebay.pulsar.analytics.util.Slugify;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * @author xinxu1
 * 
 **/
@Component
@Scope("request")
@Path("/")
public class PermissionControlResource {
	private static final Logger logger = LoggerFactory
			.getLogger(PermissionControlResource.class);

	@Autowired
	private DashboardService dashboardService;
	@Autowired
	private GroupService groupService;
	@Autowired
	private DataSourceService datasourceService;
	@Autowired
	private UserService userService;
	@Autowired
	private UserPermissionControl userPermissions;
	@Autowired
	private BasicAuthenticationEntryPoint basicAuthEntryPoint;

	private Slugify slg = new Slugify();

	private List<DBRightGroup> addList = new ArrayList<DBRightGroup>();
	private List<DBRightGroup> rightsInGroup = new ArrayList<DBRightGroup>();

	public String getUserName() {
		try {
			Authentication auth = SecurityContextHolder.getContext()
					.getAuthentication();
			if (auth instanceof UsernamePasswordAuthenticationToken) {
				return ((UserDetails) auth.getPrincipal()).getUsername();
			} else if (auth instanceof AnonymousAuthenticationToken) {
				throw new BadCredentialsException("Bad credentials");
			} else {
				throw new BadCredentialsException("Bad credentials");
			}
		} catch (Exception e) {
			throw new BadCredentialsException("Bad credentials");
		}
	}

	public boolean isValidDisplayName(String displayName) {
		if (Strings.isNullOrEmpty(displayName) || displayName.length() > 64) {
			return false;
		}
	    Matcher matcher=Pattern.compile("^[0-9a-zA-Z\\s]+$").matcher(displayName);
		if(!matcher.find()){
			return false;
		}
		return true;
	}

	public boolean isAnonymous() throws BadCredentialsException {
		String userName = getUserName();
		if (Strings.isNullOrEmpty(userName))
			return true;
		return false;
	}

	public Response handleException(Throwable ex) {
		Status status = Status.BAD_REQUEST;
		Map<String, String> errorMap = new HashMap<String, String>();

		if (ex instanceof SQLException || ex instanceof DataTruncation
				|| ex instanceof DataIntegrityViolationException) {
			errorMap.put("error", "Sql Error!");
			return Response.status(status).entity(errorMap).build();
		}
		if (ex instanceof BadCredentialsException
				|| ex instanceof InvalidSessionException) {
			status = Status.UNAUTHORIZED;
			errorMap.put("error", ex.getMessage());
		} else if (ex instanceof AccessDeniedException) {
			status = Status.FORBIDDEN;
			errorMap.put("error", ex.getMessage());
		} else if (ex instanceof DataSourceException
				|| ex instanceof DataSourceConfigurationException) {
			status = Status.SERVICE_UNAVAILABLE;
			errorMap.put("error", ex.getMessage());
		} else if (ex instanceof IllegalArgumentException
				|| ex instanceof UnsupportedOperationException
				|| ex instanceof IllegalStateException) {
			status = Status.BAD_REQUEST;
			errorMap.put("error", ex.getMessage());
		} else {
			errorMap.put("error", "Operation failed.");
		}

		if (status.equals(Status.UNAUTHORIZED)) {
			return Response
					.status(status)
					.header("WWW-Authenticate",
							"Basic realm=\""
									+ basicAuthEntryPoint.getRealmName() + "\"")
					.entity(errorMap).build();
		}
		return Response.status(status).entity(errorMap).build();

	}

	public List<DBRightGroup> getDiffInLists(List<DBRightGroup> list1,
			List<DBRightGroup> list2) {
		final List<String> rights = FluentIterable.from(list2)
				.transform(new Function<DBRightGroup, String>() {
					@Override
					public String apply(DBRightGroup input) {
						return input.getRightName();
					}
				}).toList();
		addList = FluentIterable.from(list1)
				.filter(new Predicate<DBRightGroup>() {
					@Override
					public boolean apply(DBRightGroup input) {
						return !rights.contains(input.getRightName());
					}
				}).toList();

		return Lists.newArrayList(addList);

	}

	@POST
	@Path("datasources")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addDatasource(@Context HttpServletRequest request,
			DBDataSource datasource) {
		logger.info("Add DataSource API called from IP: "
				+ request.getRemoteAddr());

		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (!this.validateDataSourceType(datasource.getType()))
				throw new IllegalArgumentException("Invalid DataSource Type ["
						+ datasource.getType() + "]");
			if (!this.validateDataSourceEndPoint(datasource.getEndpoint())) {
				throw new IllegalArgumentException(
						"Invalid DataSource Endpoint ["
								+ datasource.getEndpoint() + "]");
			}
			if (!isValidDisplayName(datasource.getDisplayName())) {
				throw new IllegalArgumentException(
						"DataSource DisplayName is Invalid!");
			}
			if (datasource.getType() == null) {
				throw new IllegalArgumentException("DataSource Type is Empty!");
			}
			datasource.setName(slg.slugify(datasource.getDisplayName()));
			datasource.setOwner(getUserName());
			long id = datasourceService.addDataSource(datasource);

			if (id > 0) {
				return Response.ok(datasource).build();
			} else {
				return Response.status(Status.BAD_REQUEST).build();
			}

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}
	}

	@DELETE
	@Path("datasources/{datasourceName}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteDatasource(@Context HttpServletRequest request,
			@PathParam("datasourceName") String datasourceName) {
		logger.info("Delete DataSources API called from IP: "
				+ request.getRemoteAddr());

		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (datasourceName == null) {
				throw new IllegalArgumentException(
						"No DataSource Name to Delete!");
			}
			int id = datasourceService.deleteDataSource(datasourceName);

			if (id >= 0) {
				return Response.ok(ImmutableMap.of("deleted", id)).build();
			} else {
				return Response.status(Status.BAD_REQUEST).build();
			}


		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@DELETE
	@Path("datasources")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response batchDeleteDatasource(@Context HttpServletRequest request,
			@QueryParam("batch") String datasourceNames) {
		logger.info("Delete DataSources API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (datasourceNames == null) {
				throw new IllegalArgumentException("No DataSources to Delete!");
			}
			int id = datasourceService.deleteDataSources(Lists
					.newArrayList(datasourceNames.split(",")));
			if (id >= 0) {
				return Response.ok(ImmutableMap.of("deleted", id)).build();
			} else {
				return Response.status(Status.BAD_REQUEST).build();
			}

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@PUT
	@Path("datasources")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response updateDatasource(@Context HttpServletRequest request,
			DBDataSource datasource) {
		logger.info("Update DataSource API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (!this.validateDataSourceType(datasource.getType()))
				throw new IllegalArgumentException("Invalid DataSource Type ["
						+ datasource.getType() + "]");
			if (!this.validateDataSourceEndPoint(datasource.getEndpoint())) {
				throw new IllegalArgumentException(
						"Invalid DataSource Endpoint ["
								+ datasource.getEndpoint() + "]");
			}
			if (datasource.getName() == null) {
				throw new IllegalArgumentException("DataSource Name is Empty!");
			}
			if (!isValidDisplayName(datasource.getDisplayName())) {
				throw new IllegalArgumentException(
						"DataSource DisplayName is Invalid!");
			}

			datasource.setName(datasource.getName().toLowerCase());
			long id = datasourceService.updateDataSource(datasource);
			if (id > 0) {
				return Response.ok(ImmutableMap.of("updated", id)).build();
			} else {
				return Response.status(Status.BAD_REQUEST).build();
			}

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@GET
	@Path("datasources")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAllDataSourceByUser(@Context HttpServletRequest request,
			@QueryParam("right") String right) {
		logger.info("List DataSources API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			Set<DBDataSource> datasources = null;
			if ("view".equalsIgnoreCase(right)) {
				datasources = datasourceService.getAllUserViewedDatasource();
				return Response.ok("get all datasources succeed!")
						.entity(datasources).build();
			}
			if (right == null || "manage".equalsIgnoreCase(right)) {
				datasources = datasourceService.getAllUserManagedDatasource();
				return Response.ok("get all datasources succeed!")
						.entity(datasources).build();
			}
			throw new IllegalArgumentException("Invalid Query Parameter!");

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@GET
	@Path("datasources/{datasourceName}/groups")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAllGroupsByDataSource(
			@Context HttpServletRequest request,
			@PathParam("datasourceName") String datasourceName,
			@QueryParam("right") String right) {
		logger.info("List DataSources API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			List<DBGroup> groups = groupService.getAllUserManagedGroups();
			if ("view".equalsIgnoreCase(right)) {
				return Response
						.ok("get all datasources succeed!")
						.entity(groupService.getAllGroupsForDataSource(
								datasourceName, groups, String.format(
										PermissionConst.VIEW_RIGHT_TEMPLATE,
										datasourceName))).build();
			}
			if (right == null || "manage".equalsIgnoreCase(right)) {
				return Response
						.ok("get all datasources succeed!")
						.entity(groupService.getAllGroupsForDataSource(
								datasourceName, groups, String.format(
										PermissionConst.MANAGE_RIGHT_TEMPLATE,
										datasourceName))).build();
			}
			throw new IllegalArgumentException("Invalid Query Parameter!");

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@POST
	@Path("dashboards")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addDashboard(@Context HttpServletRequest request,
			Dashboard d) {
		logger.info("Add Dashboard API called from IP: "
				+ request.getRemoteAddr());
		try {
			DBDashboard dashboard = d.toDBDashboard();
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (!this.validateDashboardConfig(dashboard.getConfig())) {
				throw new IllegalArgumentException("Invalid Dashboard config");
			}
			if (!isValidDisplayName(dashboard.getDisplayName())) {
				throw new IllegalArgumentException(
						"Dashboard DisplayName is Invalid!");
			}
			dashboard.setName(slg.slugify(dashboard.getDisplayName()));
			dashboard.setOwner(getUserName());

			long id = dashboardService.addDashboard(dashboard);

			if (id > 0) {
				return Response.ok(this.converDBDashboard2Map(dashboard))
						.build();
			} else {
				return Response.status(Status.BAD_REQUEST).build();
			}

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@DELETE
	@Path("dashboards/{dashboardName}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteDashboard(@Context HttpServletRequest request,
			@PathParam("dashboardName") String dashboardName) {
		logger.info("Delete Dashboard API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (dashboardName == null) {
				throw new IllegalArgumentException("No Dashboard to Delete!");
			}
			int id = dashboardService.deleteDashboard(dashboardName);
			if (id >= 0) {
				return Response.ok(ImmutableMap.of("deleted", id)).build();
			} else {
				return Response.status(Status.BAD_REQUEST).build();
			}

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}
	}

	@DELETE
	@Path("dashboards")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteDashboards(@Context HttpServletRequest request,
			@QueryParam("batch") String dashboardNames) {
		logger.info("Delete Dashboard API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (dashboardNames == null) {
				throw new IllegalArgumentException("No Dashboards to Delete!");
			}
			int id = dashboardService.deleteDashboards(Lists
					.newArrayList(dashboardNames.split(",")));
			if (id >= 0) {
				return Response.ok(ImmutableMap.of("deleted", id)).build();
			} else {
				return Response.status(Status.BAD_REQUEST).build();
			}

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@PUT
	@Path("dashboards")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response saveDashboard(@Context HttpServletRequest request,
			Dashboard d) {
		logger.info("Update Dashboard API called from IP: "
				+ request.getRemoteAddr());
		try {
			DBDashboard dashboard = d.toDBDashboard();
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (dashboard.getName() == null) {
				throw new IllegalArgumentException("Dashboard Name is Empty!");
			}
			if (!this.validateDashboardConfig(dashboard.getConfig())) {
				throw new IllegalArgumentException("Invalid Dashboard config");
			}
			if (!isValidDisplayName(dashboard.getDisplayName())) {
				throw new IllegalArgumentException(
						"Dashboard DisplayName is Invalid!");
			}
			dashboard.setName(dashboard.getName().toLowerCase());
			long id = dashboardService.updateDashboard(dashboard.getName(),
					dashboard.getDisplayName(), dashboard.getConfig());
			if (id > 0) {
				return Response.ok(ImmutableMap.of("updated", id)).build();
			} else {
				return Response.status(Status.BAD_REQUEST).build();
			}

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@GET
	@Path("dashboards/{dashboardName}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getDashboard(@Context HttpServletRequest request,
			@PathParam("dashboardName") String dashboardName) {
		logger.info("List dashboards API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (dashboardName == null) {
				throw new IllegalArgumentException("Dashboard Name is Empty!");
			}
			return Response
					.ok("get dashboard succeed!")
					.entity(this.converDBDashboard2Map(dashboardService
							.getDashboardByName(dashboardName))).build();

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@GET
	@Path("dashboards")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAllDashboardByUser(@Context HttpServletRequest request,
			@QueryParam("right") String right) {
		logger.info("List dashboards API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			List<DBDashboard> databoards = null;
			if ("view".equalsIgnoreCase(right)) {
				databoards = dashboardService.getUserViewedDashboard();
				return Response.ok("get all viewed dashboards succeed!")
						.entity(this.converList2Map(databoards)).build();
			}
			if (right == null || "manage".equalsIgnoreCase(right)) {
				databoards = dashboardService.getAllUserManagedDashboard();

				return Response.ok("get all managed dashboards succeed!")
						.entity(this.converList2Map(databoards)).build();
			}
			throw new IllegalArgumentException("Invalid Query Parameter!");

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@POST
	@Path("groups")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addGroup(@Context HttpServletRequest request, DBGroup group) {
		logger.info("Add Group API called from IP: " + request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (!isValidDisplayName(group.getDisplayName())) {
				throw new IllegalArgumentException(
						"Group DisplayName is Invalid!");
			}
			group.setName(slg.slugify(group.getDisplayName()));
			group.setOwner(getUserName());

			long id = groupService.addGroup(group);
			if (id > 0) {
				return Response.ok(group).build();
			} else {
				return Response.status(Status.BAD_REQUEST).build();
			}

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@PUT
	@Path("groups")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response saveDashboard(@Context HttpServletRequest request,
			DBGroup group) {
		logger.info("Update group API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (group.getName() == null) {
				throw new IllegalArgumentException("Group Name is Empty!");
			}
			if (!isValidDisplayName(group.getDisplayName())) {
				throw new IllegalArgumentException(
						"Group DisplayName is Invalid!");
			}
			long id = groupService.updateGroup(group.getName(),
					group.getDisplayName());
			if (id > 0) {
				return Response.ok(ImmutableMap.of("updated", id)).build();
			} else {
				return Response.status(Status.BAD_REQUEST).build();
			}

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@DELETE
	@Path("groups/{groupName}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteGroup(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName) {
		logger.info("Delete Group API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (groupName == null) {
				throw new IllegalArgumentException("No Group to Delete!");
			}
			int id = groupService.deleteGroup(groupName);

			if (id >= 0) {
				return Response.ok(ImmutableMap.of("deleted", id)).build();
			} else {
				return Response.status(Status.BAD_REQUEST).build();
			}

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@DELETE
	@Path("groups")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteGroups(@Context HttpServletRequest request,
			@QueryParam("batch") String groupNames) {
		logger.info("Delete Group API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (groupNames == null) {
				throw new IllegalArgumentException("No Groups to Delete!");
			}
			int id = groupService.deleteGroups(Lists.newArrayList(groupNames
					.split(",")));
			if (id >= 0) {
				return Response.ok(ImmutableMap.of("deleted", id)).build();
			} else {
				return Response.status(Status.BAD_REQUEST).build();
			}

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@GET
	@Path("groups")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAllGroupsByUser(@Context HttpServletRequest request,
			@QueryParam("right") String right) {
		logger.info("List Groups API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			List<DBGroup> groups = null;
			if ("view".equalsIgnoreCase(right)) {
				groups = groupService.getAllUserViewedGroups();
				return Response.ok("get all groups succeed!").entity(groups)
						.build();
			}
			if (right == null || "manage".equalsIgnoreCase(right)) {
				groups = groupService.getAllUserManagedGroups();
				return Response.ok("get all groups succeed!").entity(groups)
						.build();
			}
			throw new IllegalArgumentException("Invalid QueryParm!");

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	// @POST
	// @Path("/groups/{groupName}/users/{userName}")
	// @Consumes({ "application/json", "application/xml" })
	// @Produces({ "application/json", "application/xml" })
	// public Response addUserToGroup(@Context HttpServletRequest request,
	// @PathParam("groupName") String groupName,
	// @PathParam("userName") String userName) {
	// logger.info("update usergroup API called from IP: "
	// + request.getRemoteAddr());
	// try {
	// if (isAnonymous()) {
	// throw new BadCredentialsException("Bad credentials");
	// }
	// if (groupName == null) {
	// throw new IllegalArgumentException("GroupName is Empty!");
	// }
	// if (userName == null) {
	// throw new IllegalArgumentException("No Users for Add!");
	// }
	//
	// boolean addId = groupService.addUserToGroup(groupName, userName);
	// if (addId) {
	// return Response.ok("add user to group succeed!")
	// .entity(ImmutableMap.of("added", userName)).build();
	// }
	//
	// return Response.status(Status.BAD_REQUEST).build();
	//
	// } catch (Exception ex) {
	// logger.warn("Response Error: " + ex.getMessage());
	// return handleException(ex);
	// }
	//
	// }
	//
	@DELETE
	@Path("/groups/{groupName}/users/{userName}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteUserFromGroup(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName,
			@PathParam("userName") String userName) {
		logger.info("update usergroup API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (groupName == null) {
				throw new IllegalArgumentException("GroupName is Empty!");
			}
			if (userName == null) {
				throw new IllegalArgumentException("No Users for Add!");
			}

			int id = groupService.removeUserFromGroup(groupName, userName);
			if (id >= 0) {
				return Response.ok("delete user from group succeed!")
						.entity(ImmutableMap.of("deleted", id)).build();
			}

			return Response.status(Status.BAD_REQUEST).build();

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@POST
	@Path("/groups/{groupName}/users")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addUsersToGroup(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName, List<String> userNames) {
		logger.info("add users to group API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (groupName == null) {
				throw new IllegalArgumentException("GroupName is Empty!");
			}
			if (userNames == null) {
				throw new IllegalArgumentException("No Users for Update!");
			}

			int addId = groupService.addUsersToGroup(groupName, userNames);
			if (addId >= 0) {
				return Response.ok("add users to group succeed!")
						.entity(ImmutableMap.of("success", addId)).build();
			}

			return Response.status(Status.BAD_REQUEST).build();


		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@PUT
	@Path("/groups/{groupName}/users")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response UpdateUsersInGroup(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName, List<String> userNames) {
		logger.info("update usergroup API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (groupName == null) {
				throw new IllegalArgumentException("GroupName is Empty!");
			}
			if (userNames == null) {
				throw new IllegalArgumentException("No Users for Update!");
			}
			int id = groupService.removeUsersFromGroup(groupName,
					groupService.getAllUsersInGroup(groupName));
			if (id >= 0) {
				int addId = groupService.addUsersToGroup(groupName, userNames);
				if (addId >= 0) {
					return Response.ok("update users in group succeed!")
							.entity(groupService.getAllUsersInGroup(groupName))
							.build();
				}
			}

			return Response.status(Status.BAD_REQUEST).build();

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@DELETE
	@Path("/groups/{groupName}/users")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteUsersFromGroup(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName,
			@QueryParam("batch") String userNames) {
		logger.info("update usergroup API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (groupName == null) {
				throw new IllegalArgumentException("GroupName is Empty!");
			}
			if (userNames == null) {
				throw new IllegalArgumentException("No Users for Add!");
			}

			int id = groupService.removeUsersFromGroup(groupName,
					Lists.newArrayList(userNames.split(",")));
			if (id >= 0) {
				return Response.ok("delete user from group succeed!")
						.entity(ImmutableMap.of("deleted", id)).build();
			}

			return Response.status(Status.BAD_REQUEST).build();

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@GET
	@Path("/groups/{groupName}/users")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getGroupAllUsers(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName) {
		logger.info("List users API called from IP: " + request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (groupName == null) {
				throw new IllegalArgumentException("GroupName is Empty!");
			}
			return Response.ok("get all groups user belong to succeed!")
					.entity(groupService.getAllUsersInGroup(groupName)).build();
		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	// @POST
	// @Path("/groups/{groupName}/rights/{rightName}")
	// @Consumes({ "application/json", "application/xml" })
	// @Produces({ "application/json", "application/xml" })
	// public Response addRightToGroup(@Context HttpServletRequest request,
	// @PathParam("groupName") String groupName,
	// @PathParam("rightName") String rightName, DBRightGroup dbRightGroup) {
	// logger.info("update usergroup API called from IP: "
	// + request.getRemoteAddr());
	// try {
	// if (isAnonymous()) {
	// throw new BadCredentialsException("Bad credentials");
	// }
	// if (groupName == null) {
	// throw new IllegalArgumentException("GroupName is Empty!");
	// }
	// if (rightName == null) {
	// throw new IllegalArgumentException("No Right for Add!");
	// }
	//
	// boolean addId = groupService.addRightToGroup(groupName,
	// dbRightGroup.getRightName(), dbRightGroup.getRightType());
	// if (addId) {
	// return Response.ok("add right to group succeed!")
	// .entity(ImmutableMap.of("added", addId)).build();
	// }
	//
	// return Response.status(Status.BAD_REQUEST).build();
	//
	// } catch (Exception ex) {
	// logger.warn("Response Error: " + ex.getMessage());
	// return handleException(ex);
	// }
	//
	// }

	@DELETE
	@Path("/groups/{groupName}/rights/{rightName}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteRightFromGroup(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName,
			@PathParam("rightName") String rightName) {
		logger.info("update usergroup API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (groupName == null) {
				throw new IllegalArgumentException("GroupName is Empty!");
			}
			if (rightName == null) {
				throw new IllegalArgumentException("No RightName for Add!");
			}

			int id = groupService.removeRightFromGroup(groupName, rightName);
			if (id >= 0) {
				return Response.ok("delete right from group succeed!")
						.entity(ImmutableMap.of("deleted", id)).build();
			}

			return Response.status(Status.BAD_REQUEST).build();

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@POST
	@Path("/groups/{groupName}/rights")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addRightsToGroup(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName,
			List<DBRightGroup> dbRightGroups) {
		logger.info("update usergroup API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (groupName == null) {
				throw new IllegalArgumentException("GroupName is Empty!");
			}
			if (dbRightGroups == null) {
				throw new IllegalArgumentException("No Rights for Add!");
			}

			int addId = groupService.addRightsToGroup(dbRightGroups, groupName);
			if (addId >= 0) {
				return Response.ok("add right to group succeed!")
						.entity(ImmutableMap.of("success", addId)).build();
			}

			return Response.status(Status.BAD_REQUEST).build();

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}
	}

	@DELETE
	@Path("/groups/{groupName}/rights")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteRightsFromGroup(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName,
			@QueryParam("batch") String rightNames) {
		logger.info("update usergroup API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (groupName == null) {
				throw new IllegalArgumentException("GroupName is Empty!");
			}
			if (rightNames == null) {
				throw new IllegalArgumentException("No Rights for Add!");
			}

			int id = groupService.removeRightNamesFromGroup(groupName,
					Lists.newArrayList(rightNames.split(",")));
			if (id >= 0) {
				return Response.ok("delete right from group succeed!")
						.entity(ImmutableMap.of("deleted", id)).build();
			}

			return Response.status(Status.BAD_REQUEST).build();

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@PUT
	@Path("/groups/{groupName}/rights")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response UpdateRightsToGroup(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName,
			List<DBRightGroup> dbRightGroups) {
		logger.info("update rights of a group API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (groupName == null) {
				throw new IllegalArgumentException("GroupName is Empty!");
			}
			if (dbRightGroups == null) {
				throw new IllegalArgumentException("No Rights for Update!");
			}
			rightsInGroup = groupService.getRightsByGroupName(groupName);
			if (getDiffInLists(dbRightGroups, rightsInGroup).size() > 0) {
				int addId = groupService
						.addRightsToGroup(
								getDiffInLists(dbRightGroups, rightsInGroup),
								groupName);
				if (addId <= 0) {
					return Response.status(Status.BAD_REQUEST).build();
				}
			}
			if (getDiffInLists(rightsInGroup, dbRightGroups).size() > 0) {
				int id = groupService.removeRightsFromGroup(groupName,
						getDiffInLists(rightsInGroup, dbRightGroups));
				if (id < 0) {
					return Response.status(Status.BAD_REQUEST).build();
				}

			}
			return Response.ok("updates rights in a group succeed!")
					.entity(groupService.getRightsByGroupName(groupName))
					.build();
		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@GET
	@Path("/groups/{groupName}/rights")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getGroupAllRights(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName) {
		logger.info("List users API called from IP: " + request.getRemoteAddr());

		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (groupName == null) {
				throw new IllegalArgumentException("GroupName is Empty!");
			}
			return Response.ok("get all groups user belong to succeed!")
					.entity(groupService.getRightsByGroupName(groupName))
					.build();
		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@POST
	@Path("user")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addUser(@Context HttpServletRequest request, DBUser user) {
		logger.info("Add User API called from IP: " + request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (user == null) {
				throw new IllegalArgumentException("User List is Empty!");
			}
			if (userService.addUser(user) > 0) {
				return Response.ok(user).build();
			} else {
				return Response.status(Status.BAD_REQUEST).build();
			}
		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@DELETE
	@Path("users")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteUser(@Context HttpServletRequest request,
			@QueryParam("batch") String userNames) {
		logger.info("Delete user API called from IP: "
				+ request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			if (userNames == null) {
				throw new IllegalArgumentException("No Users to Delete!");
			}
			long id = userService.deleteUsers(Lists.newArrayList(userNames
					.split(",")));
			if (id >= 0) {
				return Response.ok(ImmutableMap.of("deleted", id)).build();
			} else {
				return Response.status(Status.BAD_REQUEST).build();
			}
		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@GET
	@Path("users")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAllUsers(@Context HttpServletRequest request) {
		logger.info("List users API called from IP: " + request.getRemoteAddr());
		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			return Response.ok("get all users succeed!")
					.entity(userService.getAllUsers()).build();
		} catch (Exception ex) {
				logger.warn("Response Error: " + ex.getMessage());
				return handleException(ex);
			}

	}

	@GET
	@Path("users/autocomplete")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getuserFields(@Context HttpServletRequest request,
			@QueryParam("name") String name, @QueryParam("row") Integer row) {
		logger.info("List users API called from IP: " + request.getRemoteAddr());

		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			List<String> userNames = userService.getAllUsers();
			List<String> result = new ArrayList<String>();
			for (String user : userNames) {
				if (row > 0 && user.startsWith(name)) {
					result.add(user);
					row--;
				}
			}
			return Response.ok(result).build();

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}

	}

	@GET
	@Path("rights")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getUserRights(@Context HttpServletRequest request) {
		logger.info("List users API called from IP: " + request.getRemoteAddr());

		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			List<String> allrights = userPermissions
					.getAllRightsForUser(getUserName());
			Set<String> result = Sets.newHashSet(allrights);
			return Response.ok(result).build();

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}
	}

	@GET
	@Path("sysrights")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getUserSysRights(@Context HttpServletRequest request) {
		logger.info("List users API called from IP: " + request.getRemoteAddr());

		try {
			if (isAnonymous()) {
				throw new BadCredentialsException("Bad credentials");
			}
			List<String> allrights = userPermissions
					.getAllRightsForUser(getUserName());
			Set<String> result = Sets.newHashSet();
			for (String right : allrights) {
				if (PermissionConst.isSysPermission(right)) {
					result.add(right);
				}
			}
			return Response.ok(result).build();

		} catch (Exception ex) {
			logger.warn("Response Error: " + ex.getMessage());
			return handleException(ex);
		}
	}

	private boolean validateDataSourceType(String type) {
		return DataSourceTypeEnum.DRUID.toString().toLowerCase().equals(type);
	}

	private boolean validateDataSourceEndPoint(String endpoints) {
		List<String> eps = Splitter.on(",").omitEmptyStrings()
				.splitToList(endpoints);
		for (String e : eps) {
			try {
				URI uri=new URI(e);
				if(uri!=null){
					logger.warn("data source endpoint: "+uri.toString());
				}
			} catch (URISyntaxException e1) {
				return false;
			}
		}
		return true;
	}

	private boolean validateDashboardConfig(String config) {
		try {
			Map<?, ?> obj = JsonUtil.readValue(config, Map.class);
			return obj != null;
		} catch (Exception e) {
			return false;
		}
	}

	private Dashboard converDBDashboard2Map(DBDashboard dd) {
		if (dd == null)
			return null;
		return Dashboard.from(dd);
	}

	private List<Dashboard> converList2Map(List<DBDashboard> dds) {
		List<Dashboard> list = Lists.newArrayList();
		for (DBDashboard dd : dds) {
			Dashboard d = converDBDashboard2Map(dd);
			if (d != null)
				list.add(d);
		}
		return list;
	}
}
