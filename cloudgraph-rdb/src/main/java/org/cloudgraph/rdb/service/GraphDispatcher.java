/**
 *        CloudGraph Community Edition (CE) License
 * 
 * This is a community release of CloudGraph, a dual-license suite of
 * Service Data Object (SDO) 2.1 services designed for relational and 
 * big-table style "cloud" databases, such as HBase and others. 
 * This particular copy of the software is released under the 
 * version 2 of the GNU General Public License. CloudGraph was developed by 
 * TerraMeta Software, Inc.
 * 
 * Copyright (c) 2013, TerraMeta Software, Inc. All rights reserved.
 * 
 * General License information can be found below.
 * 
 * This distribution may include materials developed by third
 * parties. For license and attribution notices for these
 * materials, please refer to the documentation that accompanies
 * this distribution (see the "Licenses for Third-Party Components"
 * appendix) or view the online documentation at 
 * <http://cloudgraph.org/licenses/>. 
 */
package org.cloudgraph.rdb.service;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudgraph.rdb.filter.RDBStatementExecutor;
import org.cloudgraph.rdb.filter.RDBStatementFactory;
import org.cloudgraph.store.lang.StatementExecutor;
import org.cloudgraph.store.lang.StatementFactory;
import org.cloudgraph.store.service.CreatedCommitComparator;
import org.cloudgraph.store.service.DeletedCommitComparator;
import org.cloudgraph.store.service.GraphServiceException;
import org.plasma.config.DataAccessProvider;
import org.plasma.config.DataAccessProviderName;
import org.plasma.config.PlasmaConfig;
import org.plasma.sdo.DataFlavor;
import org.plasma.sdo.PlasmaChangeSummary;
import org.plasma.sdo.PlasmaDataGraph;
import org.plasma.sdo.PlasmaDataGraphVisitor;
import org.plasma.sdo.PlasmaDataObject;
import org.plasma.sdo.PlasmaNode;
import org.plasma.sdo.PlasmaProperty;
import org.plasma.sdo.PlasmaType;
import org.plasma.sdo.access.DataAccessException;
import org.plasma.sdo.access.DataGraphDispatcher;
import org.plasma.sdo.access.InvalidSnapshotException;
import org.plasma.sdo.access.LockedEntityException;
import org.plasma.sdo.access.RequiredPropertyException;
import org.plasma.sdo.access.SequenceGenerator;
import org.plasma.sdo.access.provider.common.ModifiedObjectCollector;
import org.plasma.sdo.access.provider.common.PropertyPair;
import org.plasma.sdo.core.CoreConstants;
import org.plasma.sdo.core.CoreDataObject;
import org.plasma.sdo.core.CoreHelper;
import org.plasma.sdo.core.NullValue;
import org.plasma.sdo.core.SnapshotMap;
import org.plasma.sdo.profile.ConcurrencyType;
import org.plasma.sdo.profile.ConcurrentDataFlavor;
import org.plasma.sdo.profile.KeyType;

import sorts.InsertionSort;
import commonj.sdo.ChangeSummary.Setting;
import commonj.sdo.DataGraph;
import commonj.sdo.DataObject;
import commonj.sdo.Property;
import commonj.sdo.Type;

public class GraphDispatcher implements DataGraphDispatcher {
	private static Log log = LogFactory.getLog(GraphDispatcher.class);
	private SnapshotMap snapshotMap;
	private SequenceGenerator sequenceGenerator;
	private String username;
	private StatementFactory statementFactory;
	private StatementExecutor statementExecutor;

	@SuppressWarnings("unused")
	private GraphDispatcher() {
	}

	public GraphDispatcher(SnapshotMap snapshotMap, String username,
			Connection con) {
		this.snapshotMap = snapshotMap;
		this.username = username;
		this.statementFactory = new RDBStatementFactory();
		this.statementExecutor = new RDBStatementExecutor(con);
	}

	public void close() {
		if (sequenceGenerator != null)
			sequenceGenerator.close();
	}

	@Override
	public SnapshotMap commit(DataGraph[] dataGraphs) {
		for (DataGraph dataGraph : dataGraphs)
			commit(dataGraph);
		return this.snapshotMap;
	}

	@Override
	public SnapshotMap commit(DataGraph dataGraph) {

		if (username == null || username.length() == 0)
			throw new IllegalArgumentException("expected username param not, '"
					+ String.valueOf(username) + "'");
		else if (log.isDebugEnabled()) {
			log.debug("current user is '" + username + "'");
		}

		if (log.isDebugEnabled()) {
			log.debug(dataGraph.getChangeSummary().toString());
			log.debug(((PlasmaDataGraph) dataGraph).dump());
		}

		PlasmaChangeSummary changeSummary = (PlasmaChangeSummary) dataGraph
				.getChangeSummary();

		List<DataObject> list = changeSummary.getChangedDataObjects();
		DataObject[] changed = new DataObject[list.size()];
		list.toArray(changed);

		if (log.isDebugEnabled()) {
			StringBuffer buf = new StringBuffer();
			buf.append('\n');
			for (int i = 0; i < changed.length; i++) {
				DataObject dataObject = changed[i];
				if (changeSummary.isCreated(dataObject))
					buf.append("created: ");
				else if (changeSummary.isModified(dataObject))
					buf.append("modified: ");
				else if (changeSummary.isDeleted(dataObject))
					buf.append("deleted: ");
				buf.append(dataObject.getType().getName() + " ("
						+ dataObject.toString() + ")");
				buf.append(" depth: " + changeSummary.getPathDepth(dataObject));

				buf.append('\n');
			}
			log.debug("commit list: " + buf.toString());
		}

		List<CoreDataObject> createdList = new ArrayList<CoreDataObject>();
		for (int i = 0; i < changed.length; i++) {
			DataObject dataObject = changed[i];
			if (changeSummary.isCreated(dataObject))
				createdList.add((CoreDataObject) dataObject);
		}
		CoreDataObject[] createdArray = new CoreDataObject[createdList.size()];
		createdList.toArray(createdArray);

		if (log.isDebugEnabled()) {
			int createdIndex = 0;
			for (DataObject dataObject : createdArray) {
				log.debug("created before sort " + createdIndex + ": "
						+ dataObject.toString());
				createdIndex++;
			}
		}
		Comparator<CoreDataObject> comparator = new CreatedCommitComparator();
		InsertionSort sort = new InsertionSort();
		sort.sort(createdArray, comparator);

		if (log.isDebugEnabled()) {
			int createdIndex = 0;
			for (DataObject dataObject : createdArray) {
				log.debug("created after sort " + createdIndex + ": "
						+ dataObject.toString());
				createdIndex++;
			}
		}

		List<CoreDataObject> deletedList = new ArrayList<CoreDataObject>();
		for (int i = 0; i < changed.length; i++) {
			DataObject dataObject = changed[i];
			if (changeSummary.isDeleted(dataObject))
				deletedList.add((CoreDataObject) dataObject);
		}
		CoreDataObject[] deletedArray = new CoreDataObject[deletedList.size()];
		deletedList.toArray(deletedArray);

		if (log.isDebugEnabled()) {
			int deletedIndex = 0;
			for (DataObject dataObject : deletedArray) {
				log.debug("deleted before sort " + deletedIndex + ": "
						+ dataObject.toString());
				deletedIndex++;
			}
		}
		comparator = new DeletedCommitComparator();
		sort = new InsertionSort();
		sort.sort(deletedArray, comparator);

		if (log.isDebugEnabled()) {
			int deletedIndex = 0;
			for (int i = deletedArray.length - 1; i >= 0; i--) {
				DataObject dataObject = deletedArray[i];
				log.debug("deleted after sort " + deletedIndex + ": "
						+ dataObject.toString());
				deletedIndex++;
			}
		}

		ModifiedObjectCollector modified = new ModifiedObjectCollector(
				dataGraph);
		try {
			for (PlasmaDataObject dataObject : createdArray)
				create(dataGraph, dataObject);

			for (PlasmaDataObject dataObject : modified.getResult())
				update(dataGraph, dataObject);

			// for (int i = deletedArray.length-1; i >= 0; i--) {
			for (PlasmaDataObject dataObject : deletedArray) {
				// DataObject dataObject = deletedArray[i];
				delete(dataGraph, dataObject);
			}

			// FIXME: lock flags/values not found in change summary, must
			// traverse non-changed
			// nodes in graph to catch any concurrency flags
			new UpdatePessimisticVisitor(dataGraph);

			return snapshotMap;
		} catch (IllegalAccessException e) {
			throw new RDBServiceException(e);
		} catch (IllegalArgumentException e) {
			throw new RDBServiceException(e);
		} catch (InvocationTargetException e) {
			throw new RDBServiceException(e);
		} catch (SQLException e) {
			throw new RDBServiceException(e);
		} catch (RuntimeException e) {
			throw e;
		}
	}

	private void create(DataGraph dataGraph, PlasmaDataObject dataObject)
			throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException {
		PlasmaType type = (PlasmaType) dataObject.getType();
		UUID uuid = ((CoreDataObject) dataObject).getUUID();
		if (uuid == null)
			throw new DataAccessException("expected UUID for inserted entity '"
					+ type.getName() + "'");
		if (log.isDebugEnabled())
			log.debug("creating " + type.getName() + " '"
					+ ((PlasmaDataObject) dataObject).getUUIDAsString() + "'");

		Map<String, PropertyPair> entity = new HashMap<String, PropertyPair>();

		List<Property> pkList = type.findProperties(KeyType.primary);
		if (pkList == null || pkList.size() == 0)
			throw new DataAccessException(
					"no pri-key properties found for type '"
							+ dataObject.getType().getName() + "'");

		for (Property pkp : pkList) {
			PlasmaProperty priKeyProperty = (PlasmaProperty) pkp;

			Object pk = dataObject.get(priKeyProperty.getName());
			if (priKeyProperty.getType().isDataType()) {
				if (pk == null) {
					if (this.hasSequenceGenerator()) {
						DataFlavor dataFlavor = priKeyProperty.getDataFlavor();
						switch (dataFlavor) {
							case integral :
								if (sequenceGenerator == null) {
									sequenceGenerator = this
											.newSequenceGenerator();
									sequenceGenerator.initialize();
								}
								if (log.isDebugEnabled()) {
									log.debug("getting seq-num for "
											+ type.getName());
								}
								pk = sequenceGenerator.get(dataObject);
								PropertyPair pair = new PropertyPair(
										priKeyProperty, pk);
								entity.put(priKeyProperty.getName(), pair);
								// entity.set(targetPriKeyProperty.getName(),
								// pk);
								((CoreDataObject) dataObject).setValue(
										priKeyProperty.getName(), pk); // FIXME:
																		// bypassing
																		// modification
																		// detection
																		// on
																		// pri-key
								break;
							default :
								throw new DataAccessException(
										"found null primary key property '"
												+ priKeyProperty.getName()
												+ "' for type, "
												+ type.getURI() + "#"
												+ type.getName());
						}
					}
				} else {
					PropertyPair pair = new PropertyPair(priKeyProperty, pk);
					entity.put(priKeyProperty.getName(), pair);
				}
			} else // ref type
			{
				if (pk == null)
					throw new DataAccessException(
							"found null primary key value for property '"
									+ priKeyProperty.toString()
									+ " on property supplier chain");

				PlasmaProperty priKeyValueProperty = priKeyProperty;
				DataObject priKeyDataObject = dataObject;
				// traverse to the datatype pk property for this reference
				while (!priKeyValueProperty.getType().isDataType()) {
					priKeyValueProperty = this.statementFactory
							.getOppositePriKeyProperty(priKeyValueProperty);
					priKeyDataObject = (DataObject) pk;
					pk = priKeyDataObject.get(priKeyValueProperty.getName());
					if (pk == null)
						throw new DataAccessException(
								"found null primary key value for property '"
										+ priKeyValueProperty.toString()
										+ " on property supplier chain");
				}

				PropertyPair pair = new PropertyPair(priKeyProperty, pk);
				entity.put(priKeyProperty.getName(), pair);
				pair.setValueProp(priKeyValueProperty);
			}

			if (pk != null) {
				if (priKeyProperty.getType().isDataType()) {
					if (log.isDebugEnabled()) {
						log.debug("mapping UUID '" + uuid + "' to pk ("
								+ String.valueOf(pk) + ")");
					}
					PropertyPair pkPair = new PropertyPair(priKeyProperty, pk);
					snapshotMap.put(uuid, pkPair); // map new PK back to UUID
				} else {
					log.warn("ignoring FK pk property, "
							+ priKeyProperty.toString());
				}
			}
		}

		// FIXME - could be a reference to a user
		Property originationUserProperty = type.findProperty(
				ConcurrencyType.origination, ConcurrentDataFlavor.user);
		if (originationUserProperty != null)
			entity.put(originationUserProperty.getName(), new PropertyPair(
					(PlasmaProperty) originationUserProperty, username));
		// entity.set(originationUserProperty.getName(), username);
		else if (log.isDebugEnabled())
			log.debug("could not find origination (username) property for type, "
					+ type.getURI() + "#" + type.getName());

		Property originationTimestampProperty = type.findProperty(
				ConcurrencyType.origination, ConcurrentDataFlavor.time);
		if (originationTimestampProperty != null)
			entity.put(originationTimestampProperty.getName(),
					new PropertyPair(
							(PlasmaProperty) originationTimestampProperty,
							this.snapshotMap.getSnapshotDate()));
		// entity.set(originationTimestampProperty.getName(),
		// this.snapshotMap.getSnapshotDate());
		else if (log.isDebugEnabled())
			log.debug("could not find origination date property for type, "
					+ type + "#" + type.getName());

		Property concurrencyUserProperty = type.findProperty(
				ConcurrencyType.optimistic, ConcurrentDataFlavor.user);
		if (concurrencyUserProperty != null)
			entity.put(concurrencyUserProperty.getName(), new PropertyPair(
					(PlasmaProperty) concurrencyUserProperty, username));
		// entity.set(concurrencyUserProperty.getName(), username);
		else if (log.isDebugEnabled())
			log.debug("could not find optimistic concurrency (username) property for type, "
					+ type.getURI() + "#" + dataObject.getType().getName());

		Property concurrencyVersionProperty = type.findProperty(
				ConcurrencyType.optimistic, ConcurrentDataFlavor.time);
		if (concurrencyVersionProperty != null)
			entity.put(concurrencyVersionProperty.getName(), new PropertyPair(
					(PlasmaProperty) concurrencyVersionProperty,
					this.snapshotMap.getSnapshotDate()));
		// entity.set(concurrencyVersionProperty.getName(),
		// this.snapshotMap.getSnapshotDate());
		else if (log.isDebugEnabled())
			log.debug("could not find optimistic concurrency version property for type, "
					+ type.getURI() + "#" + type.getName());

		List<Property> properties = type.getProperties();
		for (Property p : properties) {
			PlasmaProperty property = (PlasmaProperty) p;
			if (property.isMany())
				continue;

			if (property.isKey(KeyType.primary))
				continue; // handled above

			if (property.getConcurrent() != null)
				continue;

			Object value = dataObject.get(property);
			if (value != null) {
				PropertyPair pair = createValue(dataObject, value, property);
				entity.put(property.getName(), pair);
			}
		}

		StringBuilder insert = this.statementFactory.createInsert(type, entity);
		if (log.isDebugEnabled()) {
			log.debug("inserting " + dataObject.getType().getName());
		}
		if (!this.hasSequenceGenerator()) {
			List<PropertyPair> keys = this.statementExecutor
					.executeInsertWithGeneratedKeys(type, insert, entity);

			for (Property pkp : pkList) {
				PlasmaProperty targetPriKeyProperty = (PlasmaProperty) pkp;
				for (PropertyPair key : keys) {
					if (targetPriKeyProperty.getName().equals(
							key.getProp().getName())) {
						if (log.isDebugEnabled()) {
							log.debug("mapping UUID '" + uuid + "' to pk ("
									+ String.valueOf(key.getValue()) + ")");
						}
						snapshotMap.put(uuid, key); // map new PK back to UUID
					}
				}
			}
		} else {
			this.statementExecutor.executeInsert(type, insert, entity);
		}

	}

	private void update(DataGraph dataGraph, PlasmaDataObject dataObject)
			throws IllegalArgumentException, IllegalAccessException,
			InvocationTargetException, SQLException {

		PlasmaType type = (PlasmaType) dataObject.getType();
		if (log.isDebugEnabled())
			log.debug("updating " + type.getName() + " '"
					+ ((PlasmaDataObject) dataObject).getUUIDAsString() + "'");

		List<Property> pkList = type.findProperties(KeyType.primary);
		if (pkList == null || pkList.size() == 0)
			throw new DataAccessException(
					"no pri-key properties found for type '"
							+ dataObject.getType().getName() + "'");

		List<PropertyPair> pkPairs = new ArrayList<PropertyPair>();
		for (Property pkp : pkList) {
			PlasmaProperty pkProperty = (PlasmaProperty) pkp;
			Object pk = dataObject.get(pkProperty);
			if (pk == null)
				throw new DataAccessException(
						"found null primary key property '"
								+ pkProperty.getName() + "' for type, "
								+ type.getURI() + "#" + type.getName());
			Setting setting = dataGraph.getChangeSummary().getOldValue(
					dataObject, pkProperty);
			if (setting != null) { // it's been modified
				// pk has been modified, yet we need it to lookup record, use
				// the old value
				if (!pkProperty.isReadOnly()) {
					Object oldPk = setting.getValue();
					PropertyPair pair = this.createValue(dataObject, pk, oldPk,
							pkProperty);
					pkPairs.add(pair);
				} else
					throw new IllegalAccessException(
							"attempt to modify read-only property, "
									+ type.getURI() + "#" + type.getName()
									+ "." + pkProperty.getName());
			} else {
				PropertyPair pair = this
						.createValue(dataObject, pk, pkProperty);
				pkPairs.add(pair);
			}
		}

		// FIXME: get rid of cast - define instance properties in 'base type'
		Timestamp snapshotDate = (Timestamp) ((CoreDataObject) dataObject)
				.getValue(CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP);
		if (snapshotDate == null)
			throw new RequiredPropertyException("instance property '"
					+ CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP
					+ "' is required to update entity '" + type.getURI() + "#"
					+ type.getName() + "'");
		else if (log.isDebugEnabled())
			log.debug("snapshot date: " + String.valueOf(snapshotDate));
		PlasmaProperty lockingUserProperty = (PlasmaProperty) type
				.findProperty(ConcurrencyType.pessimistic,
						ConcurrentDataFlavor.user);
		if (lockingUserProperty == null)
			if (log.isDebugEnabled())
				log.debug("could not find locking user property for type, "
						+ type.getURI() + "#" + type.getName());

		PlasmaProperty lockingTimestampProperty = (PlasmaProperty) type
				.findProperty(ConcurrencyType.pessimistic,
						ConcurrentDataFlavor.time);
		if (lockingTimestampProperty == null)
			if (log.isDebugEnabled())
				log.debug("could not find locking timestamp property for type, "
						+ type.getURI() + "#" + type.getName());

		PlasmaProperty concurrencyUserProperty = (PlasmaProperty) type
				.findProperty(ConcurrencyType.optimistic,
						ConcurrentDataFlavor.user);
		if (concurrencyUserProperty == null)
			if (log.isDebugEnabled())
				log.debug("could not find optimistic concurrency (username) property for type, "
						+ type.getURI() + "#" + type.getName());

		PlasmaProperty concurrencyTimestampProperty = (PlasmaProperty) type
				.findProperty(ConcurrencyType.optimistic,
						ConcurrentDataFlavor.time);
		if (concurrencyTimestampProperty == null)
			if (log.isDebugEnabled())
				log.debug("could not find optimistic concurrency timestamp property for type, "
						+ type.getURI() + "#" + type.getName());

		List<Object> params = new ArrayList<Object>();
		StringBuilder select = this.statementFactory.createSelectConcurrent(
				type, pkPairs, 5, params);
		Map<String, PropertyPair> entity = this.statementExecutor.fetchRowMap(
				type, select);
		if (entity.size() == 0)
			throw new GraphServiceException("could not lock record of type, "
					+ type.toString());

		// overwrite with original PK pairs as these may contain old/new value
		// info
		for (PropertyPair pair : pkPairs)
			entity.put(pair.getProp().getName(), pair);

		if (concurrencyTimestampProperty != null
				&& concurrencyUserProperty != null)
			checkAndRefreshConcurrencyFields(type, entity,
					concurrencyTimestampProperty, concurrencyUserProperty,
					snapshotDate);

		if (CoreHelper.isFlaggedLocked(dataObject))
			lock(dataObject, entity, lockingTimestampProperty,
					lockingUserProperty, snapshotDate);
		else if (CoreHelper.isFlaggedUnlocked(dataObject))
			unlock(dataObject, entity, lockingTimestampProperty,
					lockingUserProperty, snapshotDate);

		List<Property> properties = type.getProperties();
		for (Property p : properties) {
			PlasmaProperty property = (PlasmaProperty) p;
			if (property.isMany())
				continue; // do/could we invoke a "marshaler" here?
			if (property.isKey(KeyType.primary))
				continue; // handled above

			if (property.getConcurrent() != null)
				continue;

			Object oldValue = dataGraph.getChangeSummary().getOldValue(
					dataObject, property);
			if (oldValue != null) { // it's been modified
				if (!property.isReadOnly()) {
					Object value = dataObject.get(property);
					if (value != null) {
						PropertyPair pair = createValue(dataObject, value,
								property);
						entity.put(property.getName(), pair);
					}
					// setEntityValue(entity, value, property);
				} else
					throw new IllegalAccessException(
							"attempt to modify read-only property, "
									+ type.getURI() + "#" + type.getName()
									+ "." + property.getName());
			}
		}

		if (this.statementFactory.hasUpdatableProperties(entity)) {
			StringBuilder update = this.statementFactory.createUpdate(type,
					entity);
			if (log.isDebugEnabled()) {
				log.debug("updating " + dataObject.getType().getName());
			}
			this.statementExecutor.execute(type, update, entity);
		}
	}

	private void delete(DataGraph dataGraph, DataObject dataObject)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, SQLException {
		PlasmaType type = (PlasmaType) dataObject.getType();
		if (log.isDebugEnabled())
			log.debug("deleting " + type.getName() + " '"
					+ ((PlasmaDataObject) dataObject).getUUIDAsString() + "'");
		List<Property> pkList = type.findProperties(KeyType.primary);
		if (pkList == null || pkList.size() == 0)
			throw new DataAccessException(
					"no pri-key properties found for type '"
							+ dataObject.getType().getName() + "'");

		List<PropertyPair> pkPairs = new ArrayList<PropertyPair>();
		for (Property pkp : pkList) {
			PlasmaProperty pkProperty = (PlasmaProperty) pkp;
			PlasmaProperty priKeyValueProperty = pkProperty;
			DataObject priKeyDataObject = dataObject;
			Object pk = priKeyDataObject.get(pkProperty.getName());
			if (pk == null) { // check the change summary - delete removes
								// references
				Setting setting = dataGraph.getChangeSummary().getOldValue(
						dataObject, pkProperty);
				if (setting != null) {
					pk = setting.getValue();
				}
				if (pk == null)
					throw new DataAccessException(
							"found null primary key property '"
									+ pkProperty.toString());
			}
			while (!priKeyValueProperty.getType().isDataType()) {
				priKeyValueProperty = this.statementFactory
						.getOppositePriKeyProperty(priKeyValueProperty);
				priKeyDataObject = (DataObject) pk;
				pk = priKeyDataObject.get(priKeyValueProperty.getName());
				if (pk == null) { // check the change summary - delete removes
									// references
					Setting setting = dataGraph.getChangeSummary().getOldValue(
							priKeyDataObject, priKeyValueProperty);
					if (setting != null) {
						pk = setting.getValue();
					}
					if (pk == null)
						throw new DataAccessException(
								"found null primary key property '"
										+ priKeyValueProperty.toString());
				}
			}
			PropertyPair pair = new PropertyPair(pkProperty, pk);
			if (!priKeyValueProperty.equals(pkProperty))
				pair.setValueProp(priKeyValueProperty);
			pkPairs.add(pair);
		}

		// FIXME: get rid of cast - define instance properties in 'base type'
		Timestamp snapshotDate = (Timestamp) ((CoreDataObject) dataObject)
				.getValue(CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP);
		if (snapshotDate == null)
			throw new RequiredPropertyException("property '"
					+ CoreConstants.PROPERTY_NAME_SNAPSHOT_TIMESTAMP
					+ "' is required to update entity '" + type.getName() + "'");

		List<Object> params = new ArrayList<Object>();
		StringBuilder select = this.statementFactory.createSelectConcurrent(
				type, pkPairs, 5, params);
		Map<String, PropertyPair> entity = this.statementExecutor.fetchRowMap(
				type, select);

		PlasmaProperty lockingUserProperty = (PlasmaProperty) type
				.findProperty(ConcurrencyType.pessimistic,
						ConcurrentDataFlavor.user);
		if (lockingUserProperty == null)
			if (log.isDebugEnabled())
				log.debug("could not find locking user property for type, "
						+ type.getURI() + "#" + type.getName());

		PlasmaProperty lockingTimestampProperty = (PlasmaProperty) type
				.findProperty(ConcurrencyType.pessimistic,
						ConcurrentDataFlavor.time);
		if (lockingTimestampProperty == null)
			if (log.isDebugEnabled())
				log.debug("could not find locking timestamp property for type, "
						+ type.getURI() + "#" + type.getName());

		PlasmaProperty concurrencyUserProperty = (PlasmaProperty) type
				.findProperty(ConcurrencyType.optimistic,
						ConcurrentDataFlavor.user);
		if (concurrencyUserProperty == null)
			if (log.isDebugEnabled())
				log.debug("could not find optimistic concurrency (username) property for type, "
						+ type.getURI() + "#" + type.getName());

		PlasmaProperty concurrencyTimestampProperty = (PlasmaProperty) type
				.findProperty(ConcurrencyType.optimistic,
						ConcurrentDataFlavor.time);
		if (concurrencyTimestampProperty == null)
			if (log.isDebugEnabled())
				log.debug("could not find optimistic concurrency timestamp property for type, "
						+ dataObject.getType().getURI()
						+ "#"
						+ dataObject.getType().getName());

		if (concurrencyTimestampProperty != null
				&& concurrencyUserProperty != null) {
			checkConcurrencyFields(type, entity, concurrencyTimestampProperty,
					concurrencyUserProperty, snapshotDate);
		} else if (log.isDebugEnabled())
			log.debug("could not find concurrency version or user fields for "
					+ dataObject.getType().getURI() + "#"
					+ dataObject.getType().getName());

		entity.clear(); // no concurrency fields needed
		for (PropertyPair pair : pkPairs)
			entity.put(pair.getProp().getName(), pair);

		StringBuilder delete = this.statementFactory.createDelete(type, entity);
		if (log.isDebugEnabled()) {
			log.debug("deleting " + dataObject.getType().getName());
		}
		this.statementExecutor.execute(type, delete, entity);
	}

	/**
	 * Compares the application-level concurrency state of the given datastore
	 * entity against the query snapshot date. If the snapshot date is current,
	 * the concurrency data for the entity is refreshed. Otherwise a shapshot
	 * concurrency exception is thrown.
	 * 
	 * @param type
	 *            - the type definition
	 * @param entity
	 *            - the map representing datastore entity
	 * @param lastUpdatedDateProperty
	 *            - the last updated date property definition metadata
	 * @param lastUpdatedByNameProperty
	 *            - the last updated by name property definition metadata
	 * @param snapshotDate
	 *            - the query snapshot date
	 */
	private void checkConcurrencyFields(Type type,
			Map<String, PropertyPair> entity, Property lastUpdatedDateProperty,
			Property lastUpdatedByNameProperty, Timestamp snapshotDate)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		PropertyPair lastUpdatedDatePair = entity.get(lastUpdatedDateProperty
				.getName());
		PropertyPair lastUpdatedByPair = entity.get(lastUpdatedByNameProperty
				.getName());

		String entityName = type.getName();

		if (lastUpdatedDatePair != null) {
			Date lastUpdatedDate = (Date) lastUpdatedDatePair.getValue();
			if (log.isDebugEnabled())
				log.debug("comparing " + lastUpdatedDate
						+ "greater than snapshot: " + snapshotDate);
			if (lastUpdatedDate.getTime() > snapshotDate.getTime()) {
				if (lastUpdatedByPair != null) {
					String lastUpdatedBy = (String) lastUpdatedByPair
							.getValue();
					throw new InvalidSnapshotException(entityName, username,
							snapshotDate, lastUpdatedBy, lastUpdatedDate);
				} else
					throw new InvalidSnapshotException(entityName, username,
							snapshotDate, "unknown", lastUpdatedDate);
			}
		}
	}

	/**
	 * Compares the application-level concurrency state of the given datastore
	 * entity against the query snapshot date. If the snapshot date is current,
	 * the concurrency data for the entity is refreshed. Otherwise a shapshot
	 * concurrency exception is thrown.
	 * 
	 * @param type
	 *            - the type definition
	 * @param entity
	 *            - the map representing datastore entity
	 * @param lastUpdatedDateProperty
	 *            - the last updated date property definition metadata
	 * @param lastUpdatedByNameProperty
	 *            - the last updated by name property definition metadata
	 * @param snapshotDate
	 *            - the query snapshot date
	 */
	private void checkAndRefreshConcurrencyFields(Type type,
			Map<String, PropertyPair> entity, Property lastUpdatedDateProperty,
			Property lastUpdatedByNameProperty, Timestamp snapshotDate)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		PropertyPair lastUpdatedDatePair = entity.get(lastUpdatedDateProperty
				.getName());
		PropertyPair lastUpdatedByPair = entity.get(lastUpdatedByNameProperty
				.getName());

		String entityName = type.getName();

		if (lastUpdatedDatePair != null) {
			Date lastUpdatedDate = (Date) lastUpdatedDatePair.getValue();
			if (log.isDebugEnabled())
				log.debug("comparing " + lastUpdatedDate
						+ "greater than snapshot: " + snapshotDate);
			if (lastUpdatedDate.getTime() > snapshotDate.getTime()) {
				if (lastUpdatedByPair != null) {
					String lastUpdatedBy = (String) lastUpdatedByPair
							.getValue();
					throw new InvalidSnapshotException(entityName, username,
							snapshotDate, lastUpdatedBy, lastUpdatedDate);
				} else
					throw new InvalidSnapshotException(entityName, username,
							snapshotDate, "unknown", lastUpdatedDate);
			}
			PropertyPair updatedDatePair = new PropertyPair(
					lastUpdatedDatePair.getProp(),
					this.snapshotMap.getSnapshotDate());
			entity.put(lastUpdatedDatePair.getProp().getName(), updatedDatePair);

			if (lastUpdatedByPair != null) {
				PropertyPair updatedByPair = new PropertyPair(
						lastUpdatedByPair.getProp(), username);
				entity.put(updatedByPair.getProp().getName(), updatedByPair);
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("reset updated-date "
					+ entity.getClass().getSimpleName()
					+ " ("
					+ entityName
					+ " - "
					+ this.snapshotMap.getSnapshotDate()
					+ "("
					+ String.valueOf(this.snapshotMap.getSnapshotDate()
							.getTime()) + ")");
		}
	}

	/**
	 * Attempts to lock the given datastore entity. If the given entity has no
	 * locking data or the entity is already locked by the given user, the lock
	 * is refreshed. Otherwise the lock is overwritten (slammed) if another user
	 * has an expired lock. If another user has a current lock on the given
	 * datastore entity, a LockedEntityException is thrown.
	 * 
	 * @param entity
	 *            - the datastore entity
	 * @param dataObject
	 *            - the value object
	 * @param lockedDateProperty
	 *            - the last locked date property definition metadata
	 * @param lockedByNameProperty
	 *            - the last locked by name property definition metadata
	 * @param snapshotDate
	 *            - the query snapshot date
	 */
	private void lock(PlasmaDataObject dataObject,
			Map<String, PropertyPair> entity,
			PlasmaProperty lockedDateProperty,
			PlasmaProperty lockedByNameProperty, Timestamp snapshotDate)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		if (lockedDateProperty != null && lockedByNameProperty != null) {
			PropertyPair lockedDatePair = entity.get(lockedDateProperty
					.getName());
			PropertyPair lockedByNamePair = entity.get(lockedByNameProperty
					.getName());
			String lockedByName = (String) lockedByNamePair.getValue();
			Date lockedDate = (Date) lockedDatePair.getValue();
			CoreHelper.unflagLocked(dataObject);
			// log.info("flag locked");
			if (lockedByName == null || username.equals(lockedByName)) // no
																		// lock
																		// or
																		// same
																		// user
			{
				if (log.isDebugEnabled()) {
					log.debug("locking " + entity.getClass().getSimpleName()
							+ " (" + dataObject.getUUIDAsString() + ")");
				}
				entity.put(lockedByNameProperty.getName(), new PropertyPair(
						lockedByNameProperty, username));
				entity.put(lockedDateProperty.getName(), new PropertyPair(
						lockedDateProperty, this.snapshotMap.getSnapshotDate()));
			} else // another user has existing or expired lock
			{
				long timeout = 300000L;
				DataAccessProvider providerConf = PlasmaConfig.getInstance()
						.getDataAccessProvider(DataAccessProviderName.JDBC);
				if (providerConf.getConcurrency() != null)
					if (providerConf.getConcurrency()
							.getPessimisticLockTimeoutMillis() > 0)
						timeout = providerConf.getConcurrency()
								.getPessimisticLockTimeoutMillis();
				if (snapshotDate.getTime() - lockedDate.getTime() > timeout) // existing
																				// lock
																				// expired
				{
					if (log.isDebugEnabled()) {
						log.debug("locking "
								+ entity.getClass().getSimpleName() + " ("
								+ dataObject.getUUIDAsString()
								+ ") - existing lock by '" + lockedByName
								+ "' expired");
					}
					entity.put(lockedByNameProperty.getName(),
							new PropertyPair(lockedByNameProperty, username));
					entity.put(lockedDateProperty.getName(),
							new PropertyPair(lockedDateProperty,
									this.snapshotMap.getSnapshotDate()));
				} else {
					if (log.isWarnEnabled()) {
						log.warn("could not issue lock for user '"
								+ String.valueOf(username)
								+ "' for snapshot date "
								+ String.valueOf(snapshotDate));
					}
					throw new LockedEntityException(entity.getClass()
							.getSimpleName(), lockedByName, lockedDate);
				}
			}
		}
	}

	/**
	 * Attempts to unlock the given datastore entity, first checking if the
	 * given user has an existing or expired lock. Otherwise a warning is
	 * logged.
	 * 
	 * @param entity
	 *            - the datastore entity
	 * @param dataObject
	 *            - the value object
	 * @param lockedDateProperty
	 *            - the last locked date property definition metadata
	 * @param lockedByNameProperty
	 *            - the last locked by name property definition metadata
	 * @param snapshotDate
	 *            - the query snapshot date
	 */
	private void unlock(PlasmaDataObject dataObject,
			Map<String, PropertyPair> entity,
			PlasmaProperty lockedDateProperty,
			PlasmaProperty lockedByNameProperty, Timestamp snapshotDate)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		if (lockedDateProperty != null && lockedByNameProperty != null) {
			PropertyPair lockedDatePair = entity.get(lockedDateProperty
					.getName());
			PropertyPair lockedByNamePair = entity.get(lockedByNameProperty
					.getName());
			String lockedByName = (String) lockedByNamePair.getValue();
			Date lockedDate = (Date) lockedDatePair.getValue();
			if (username.equals(lockedByName)) {
				if (log.isDebugEnabled()) {
					log.debug("unlocking " + entity.getClass().getSimpleName()
							+ " (" + dataObject.getUUIDAsString() + ")");
				}
				entity.put(lockedByNameProperty.getName(), new PropertyPair(
						lockedByNameProperty, username));
				entity.put(lockedDateProperty.getName(), new PropertyPair(
						lockedDateProperty, this.snapshotMap.getSnapshotDate()));
			} else
				log.warn("could not unlock entity "
						+ entity.getClass().getSimpleName() + " ("
						+ dataObject.getUUIDAsString() + ") - current user '"
						+ username + "' has no existing or expired lock");
		}
	}

	protected PropertyPair createValue(PlasmaDataObject dataObject,
			Object value, Property property) throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		return createValue(dataObject, value, null, property);
	}

	protected PropertyPair createValue(PlasmaDataObject dataObject,
			Object value, Object oldValue, Property property)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		if (log.isDebugEnabled()) {
			log.debug("setting " + dataObject.toString() + "."
					+ property.getName());
		}

		PropertyPair resultValue = findResultValue(dataObject, value, property);
		if (oldValue != null) {
			PropertyPair oldResultValue = findResultValue(dataObject, oldValue,
					property);
			resultValue.setOldValue(oldResultValue.getValue());
		}

		return resultValue;
	}

	protected PropertyPair findResultValue(PlasmaDataObject dataObject,
			Object value, Property property) throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		Object resultValue = value;

		// pull pk from value object target and use to find existing entity
		PlasmaProperty valueProperty = null;
		if (!property.getType().isDataType()
				&& !(resultValue instanceof NullValue)) {
			if (!(resultValue instanceof DataObject))
				throw new DataAccessException(
						"expected data object for singular reference property "
								+ property.getType().getName() + "."
								+ property.getName() + " but found "
								+ resultValue.getClass().getName());

			DataObject resultDataObject = (DataObject) resultValue;
			CoreDataObject resultCoreObject = (CoreDataObject) resultValue;
			PlasmaType resultType = (PlasmaType) resultDataObject.getType();

			List<Property> pkList = resultType.findProperties(KeyType.primary);
			if (pkList == null)
				throw new DataAccessException(
						"found no pri-key properties found for type '"
								+ property.getType().getName() + "'");
			if (pkList.size() > 1)
				throw new DataAccessException(
						"multiple pri-key properties found for type '"
								+ property.getType().getName()
								+ "' - not yet supported");
			Object pk = null;
			valueProperty = (PlasmaProperty) pkList.get(0);
			while (!valueProperty.getType().isDataType()) {
				resultDataObject = (CoreDataObject) resultDataObject
						.get(valueProperty.getName());
				valueProperty = this.statementFactory
						.getOppositePriKeyProperty(valueProperty);
			}

			pk = resultDataObject.get(valueProperty.getName());
			if (pk == null) {
				UUID uuid = resultCoreObject.getUUID();
				if (uuid == null)
					throw new DataAccessException(
							"found no UUID value for entity '"
									+ property.getType().getName()
									+ "' when setting property "
									+ dataObject.getType().toString() + "."
									+ property.getName());
				PropertyPair pkPair = this.snapshotMap.get(uuid, valueProperty);
				if (pkPair == null)
					throw new DataAccessException(
							"found no pri-key value found in entity or mapped to UUID '"
									+ uuid + "' for entity '"
									+ property.getType().getName()
									+ "' when setting property "
									+ dataObject.getType().toString() + "."
									+ property.getName());
				pk = pkPair.getValue();
			}
			resultValue = pk;
			if (log.isDebugEnabled()) {
				log.debug("set " + dataObject.toString() + "."
						+ property.getName() + " ("
						+ String.valueOf(resultValue) + ")");
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("set " + dataObject.toString() + "."
						+ property.getName() + " ("
						+ String.valueOf(resultValue) + ")");
			}
		}

		PropertyPair result = null;
		if (!(value instanceof NullValue)) {
			result = new PropertyPair((PlasmaProperty) property, resultValue);
			if (valueProperty != null)
				result.setValueProp(valueProperty);
		} else {
			result = new PropertyPair((PlasmaProperty) property, null);
			if (valueProperty != null)
				result.setValueProp(valueProperty);
		}

		return result;
	}

	private Property findCachedProperty(PlasmaType type, Property instanceProp) {
		List<Object> result = type.search(instanceProp);
		if (result != null && result.size() > 0) {
			if (result.size() > 1)
				log.warn("expected single value for instance property '"
						+ instanceProp.getName() + "' withing type '"
						+ type.getURI() + "#" + type.getName()
						+ "' and all its base types");
			Object obj = result.get(0);
			if (obj instanceof Property) {
				return (Property) obj;
			} else
				log.warn("expected value for instance property '"
						+ instanceProp.getName()
						+ "' for type '"
						+ type.getURI()
						+ "#"
						+ type.getName()
						+ "' or one of its base types to be a instnace of class, "
						+ Property.class.getName());
		}
		return null;
	}

	private boolean hasSequenceGenerator() {
		DataAccessProvider provider = PlasmaConfig.getInstance()
				.getDataAccessProvider(DataAccessProviderName.JDBC);
		return provider.getSequenceConfiguration() != null
				&& provider.getSequenceConfiguration().getGeneratorClassName() != null;
	}

	private SequenceGenerator newSequenceGenerator() {
		try {
			DataAccessProvider provider = PlasmaConfig.getInstance()
					.getDataAccessProvider(DataAccessProviderName.JDBC);
			String qualifiedName = provider.getSequenceConfiguration()
					.getGeneratorClassName();

			Class<?> entityClass = Class.forName(qualifiedName);
			Class<?>[] argClasses = {};
			Object[] args = {};
			Constructor<?> constructor = entityClass.getConstructor(argClasses);
			return (SequenceGenerator) constructor.newInstance(args);
		} catch (ClassNotFoundException e) {
			throw new DataAccessException(e);
		} catch (NoSuchMethodException e) {
			throw new DataAccessException(e);
		} catch (InstantiationException e) {
			throw new DataAccessException(e);
		} catch (IllegalAccessException e) {
			throw new DataAccessException(e);
		} catch (InvocationTargetException e) {
			throw new DataAccessException(e);
		}
	}

	protected String printDataObjectInfo(DataObject vo) {

		return vo.getType().getName();
	}

	class UpdatePessimisticVisitor implements PlasmaDataGraphVisitor {
		private DataGraph dataGraph;

		public UpdatePessimisticVisitor(DataGraph dataGraph) {
			this.dataGraph = dataGraph;
			((PlasmaNode) this.dataGraph.getRootObject()).getDataObject()
					.accept(this);
		}

		public void visit(DataObject target, DataObject source,
				String sourceKey, int level) {
			try {
				if (CoreHelper.isFlaggedLocked(target)
						|| CoreHelper.isFlaggedUnlocked(target))
					try {
						update(dataGraph, (PlasmaDataObject) target);
					} catch (SQLException e) {
						throw new RDBServiceException(e);
					}
			} catch (IllegalArgumentException e) {
				throw new DataAccessException(e);
			} catch (IllegalAccessException e) {
				throw new DataAccessException(e);
			} catch (InvocationTargetException e) {
				throw new DataAccessException(e);
			}
		}

	}

}
