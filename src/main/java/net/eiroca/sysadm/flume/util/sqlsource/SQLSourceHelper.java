package net.eiroca.sysadm.flume.util.sqlsource;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.conf.ConfigurationException;
import org.hibernate.CacheMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.transform.Transformers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.eiroca.library.parameter.BooleanParameter;
import net.eiroca.library.parameter.IntegerParameter;
import net.eiroca.library.parameter.Parameters;
import net.eiroca.library.parameter.PathParameter;
import net.eiroca.library.parameter.StringParameter;
import net.eiroca.library.system.LibFile;
import net.eiroca.sysadm.flume.core.util.Flume;

/**
 * Helper to manage configuration parameters and utility methods
 * <p>
 *
 * Configuration parameters readed from flume configuration file: <tt>type: </tt>
 * org.keedio.flume.source.SQLSource
 * <p>
 * <tt>table: </tt> table to read from
 * <p>
 * <tt>columns.to.select: </tt> columns to select for import data (* will import all)
 * <p>
 * <tt>run.query.delay: </tt> delay time to execute each query to database
 * <p>
 * <tt>status.file.path: </tt> Directory to save status file
 * <p>
 * <tt>status.file.name: </tt> Name for status file (saves last row index processed)
 * <p>
 * <tt>batch.size: </tt> Batch size to send events from flume source to flume channel
 * <p>
 * <tt>max.rows: </tt> Max rows to import from DB in one query
 * <p>
 * <tt>custom.query: </tt> Custom query to execute to database (be careful)
 * <p>
 *
 * @author <a href="mailto:mvalle@keedio.com">Marcelo Valle</a>
 * @author <a href="mailto:lalazaro@keedio.com">Luis Lazaro</a>
 */

public class SQLSourceHelper {

  private static final Logger logger = LoggerFactory.getLogger(SQLSourceHelper.class);

  private static final String PREFIX_HIBERNATE = "hibernate.";
  private static final String CURRENTINDEX_PLACEHOLDER = "$@$";

  final Parameters params = new Parameters();
  final PathParameter pStatusFile = new PathParameter(params, "status-filepath", "~/SQLSource.status");
  final StringParameter pTable = new StringParameter(params, "table", null);
  final StringParameter pColumns = new StringParameter(params, "table", "*");
  final StringParameter pQuery = new StringParameter(params, "query", null);
  final IntegerParameter pQueryMaxRows = new IntegerParameter(params, "query-max-rows", -1);
  final BooleanParameter pQueryReadOnly = new BooleanParameter(params, "query-read-only", false);
  final StringParameter pQueryStartFrom = new StringParameter(params, "query-start-from", "0");

  private static SessionFactory factory;
  private Session session;
  private ServiceRegistry serviceRegistry;

  private final Configuration config;
  private final Path statusFileName;
  private final String table;
  private final String columnsToSelect;
  private final String customQuery;
  private final String query;
  private final int maxRows;
  private final String startFrom;
  private final boolean readOnlySession;

  private String currentIndex;

  /**
   * Builds an SQLSourceHelper containing the configuration parameters and usefull utils for SQL
   * Source
   * @param context Flume source context, contains the properties from configuration file
   * @param sourceName source file name for store status
   */
  public SQLSourceHelper(final Context context, final String sourceName) {
    Flume.laodConfig(params, context);
    statusFileName = pStatusFile.get();
    table = pTable.get();
    customQuery = pQuery.get();
    columnsToSelect = pColumns.get();
    maxRows = pQueryMaxRows.get();
    readOnlySession = pQueryReadOnly.get();
    startFrom = pQueryStartFrom.get();
    //
    if ((table == null) && (customQuery == null)) { throw new ConfigurationException("property table not set"); }
    if (context.getString("hibernate.connection.url") == null) { throw new ConfigurationException("hibernate.connection.url property not set"); }
    if (context.getString("hibernate.connection.user") == null) { throw new ConfigurationException("hibernate.connection.user property not set"); }
    if (context.getString("hibernate.connection.password") == null) { throw new ConfigurationException("hibernate.connection.password property not set"); }
    //
    if (!isStatusFileCreated()) {
      currentIndex = startFrom;
      updateStatusFile();
    }
    else {
      currentIndex = getStatusFileIndex(startFrom);
    }
    query = buildQuery();
    /* Establish connection with database */
    final Map<String, String> hibernateProperties = context.getSubProperties(SQLSourceHelper.PREFIX_HIBERNATE);
    final Iterator<Map.Entry<String, String>> it = hibernateProperties.entrySet().iterator();
    config = new Configuration();
    Map.Entry<String, String> e;
    while (it.hasNext()) {
      e = it.next();
      config.setProperty(SQLSourceHelper.PREFIX_HIBERNATE + e.getKey(), e.getValue());
    }
    establishSession();
  }

  public String buildQuery() {
    String result;
    if (customQuery == null) {
      result = String.format("SELECT %s FROM %s ", columnsToSelect, table);
    }
    else {
      if (customQuery.contains(SQLSourceHelper.CURRENTINDEX_PLACEHOLDER)) {
        result = customQuery.replace(SQLSourceHelper.CURRENTINDEX_PLACEHOLDER, currentIndex);
      }
      else {
        result = customQuery;
      }
    }
    return result;
  }

  private boolean isStatusFileCreated() {
    final File status = statusFileName.toFile();
    return status.exists() && !status.isDirectory() ? true : false;
  }

  /**
   * Update status file with last read row index
   */
  public void updateStatusFile() {
    if (!LibFile.writeString(statusFileName.toString(), currentIndex)) {
      SQLSourceHelper.logger.error("Error updating status file!!!", LibFile.lastError);
    }
  }

  private String getStatusFileIndex(final String configuredStartValue) {
    String result = configuredStartValue;
    result = LibFile.readString(statusFileName.toString());
    if (result == null) {
      SQLSourceHelper.logger.error("Exception reading status file, doing back up and creating new status file");
      backupStatusFile();
      result = configuredStartValue;
    }
    return result;
  }

  private void backupStatusFile() {
    statusFileName.toFile().renameTo(new File(statusFileName.toString() + ".bak." + System.currentTimeMillis()));
  }

  public String getCurrentIndex() {
    return currentIndex;
  }

  public void setCurrentIndex(final String newValue) {
    currentIndex = newValue;
  }

  String getQuery() {
    return query;
  }

  boolean isCustomQuerySet() {
    return (customQuery != null);
  }

  public boolean isReadOnlySession() {
    return readOnlySession;
  }

  /**
   * Connect to database using hibernate
   */
  public void establishSession() {
    SQLSourceHelper.logger.info("Opening hibernate session");
    serviceRegistry = new StandardServiceRegistryBuilder().applySettings(config.getProperties()).build();
    SQLSourceHelper.factory = config.buildSessionFactory(serviceRegistry);
    session = SQLSourceHelper.factory.openSession();
    session.setCacheMode(CacheMode.IGNORE);
    session.setDefaultReadOnly(isReadOnlySession());
  }

  /**
   * Close database connection
   */
  public void closeSession() {
    SQLSourceHelper.logger.info("Closing hibernate session");
    session.close();
    SQLSourceHelper.factory.close();
  }

  private void resetConnection() throws InterruptedException {
    closeSession();
    establishSession();
  }

  /**
   * Execute the selection query in the database
   * @return The query result. Each Object is a cell content.
   *         <p>
   *         The cell contents use database types (date,int,string...), keep in mind in case of
   *         future conversions/castings.
   * @throws InterruptedException
   */
  @SuppressWarnings("unchecked")
  public List<List<Object>> executeQuery() throws InterruptedException {
    List<List<Object>> rowsList = new ArrayList<>();
    Query query;
    if (!session.isConnected()) {
      resetConnection();
    }
    if (isCustomQuerySet()) {
      query = session.createSQLQuery(buildQuery());
    }
    else {
      query = session.createSQLQuery(getQuery()).setFirstResult(Integer.parseInt(getCurrentIndex()));
    }
    if (maxRows > 0) {
      query.setMaxResults(maxRows);
      query.setFetchSize(maxRows);
    }
    try {
      rowsList = query.setResultTransformer(Transformers.TO_LIST).list();
    }
    catch (final Exception e) {
      SQLSourceHelper.logger.error("Exception thrown, resetting connection.", e);
      resetConnection();
    }
    if (!rowsList.isEmpty()) {
      if (isCustomQuerySet()) {
        setCurrentIndex(rowsList.get(rowsList.size() - 1).get(0).toString());
      }
      else {
        setCurrentIndex(Integer.toString((Integer.parseInt(getCurrentIndex()) + rowsList.size())));
      }
    }
    return rowsList;
  }

}
