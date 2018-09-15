package com.profitera.dc.impl;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.profitera.dc.LoadingErrorList;
import com.profitera.dc.parser.LoadingQueryWriter;
import com.profitera.dc.parser.impl.LoadDefinition;

public class TableRefresher {
  private final LoadDefinition definition;
  private static final Log LOG = LogFactory.getLog(TableRefresher.class);

  public TableRefresher(LoadDefinition definition) {
    this.definition = definition;
  }

  public void refresh(SqlMapClient writerClient, final LoadingErrorList allErrors) {
    if (definition.isRefreshData()) {
      boolean alreadyError = false;
      try {
        writerClient.startTransaction();
        int refreshed = executeRefreshDataQueries(writerClient);
        LOG.debug("Refresh data query affected " + refreshed + " rows.");
        writerClient.commitTransaction();
      } catch (SQLException e) {
        LOG.error("Exception occured when trying to delete all the data for refreshing table " + definition.getDestTable(), e);
        allErrors.writeException(new Exception("Refresh data:", e), LOG);
        alreadyError = true;
        throw new RuntimeException("Error while trying to refresh data.", e);
      } finally {
        try {
        writerClient.endTransaction();
        } catch (SQLException e) {
          if (!alreadyError) {
            LOG.error("Exception occured when trying to delete all the data for refreshing table " + definition.getDestTable(), e);
            allErrors.writeException(new Exception("Refresh data:", e), LOG);
            alreadyError = true;
            throw new RuntimeException("Error while trying to refresh data.", e);
          }
        }
      }
    }
  }
  private int executeRefreshDataQueries(SqlMapClient transactionClient) throws SQLException {
    int refreshed = 0;
    
    String refreshQueryName = LoadingQueryWriter.getRefreshQueryName(definition.getDestTable());
    String refreshQuery = definition.getRefreshDataQuery();
    if (refreshQuery == null || refreshQuery.trim().toLowerCase().startsWith("delete")) {
      refreshed = transactionClient.delete(refreshQueryName, null);
    } else {
      refreshed = transactionClient.update(refreshQueryName, null);
    }
    
    return refreshed;
  }


}
