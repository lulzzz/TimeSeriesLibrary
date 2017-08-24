﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class contains a System.Data.SqlClient.SqlConnection object for a single database
    /// connection, along with objects that are specific to that connection.
    /// </summary>
    public class TSConnection
    {

        #region Public properties
        /// <summary>
        /// SqlConnection object
        /// </summary>
        public SqlConnection Connection { get; set; }
        /// <summary>
        /// Collection of wrapper objects for SqlCommand objects that have been cached using
        /// the SqlCommand.Prepare method.
        /// </summary>
        public List<TSSqlCommandContainer> PreparedSqlCommands = new List<TSSqlCommandContainer>();
        /// <summary>
        /// Dictionary of DataTable objects that are used for SqlBulkCopy operations.
        /// The Dictionary key is the name of the table that is to be written to.
        /// </summary>
        public Dictionary<String, DataTable> BulkCopyDataTables = new Dictionary<String, DataTable>();
        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connection">SqlConnection object to be contained
        /// by the new TSConnection object</param>
        public TSConnection(SqlConnection connection)
        {
            Connection = connection;
        }

        /// <summary>
        /// This method writes the data in the DataTable object to the database table with the given
        /// name.  This method is designed to be called after making several calls to method 
        /// TS.WriteTrace, which adds data to the DataTable object.  The given table name is used as
        /// a key to find the DataTable object in this TSConnection object's BulkCopyDataTables
        /// dictionary.  If the DataTable object is not found in the dictionary, then this method
        /// simply does nothing.  After this method writes the data in the DataTable object to the
        /// database, it disposes the DataTable object.
        /// </summary>
        /// <param name="tableName">the name of the database table to which the data is to be copied</param>
        public void CommitWritesToTable(String tableName)
        {
            // Get the DataTable object from the Dictionary
            DataTable dataTable;
            if (BulkCopyDataTables.TryGetValue(tableName, out dataTable) == false)
            {
                // If the DataTable object was not found in the dictionary,
                // then this method does nothing.
                return;
            }
            SqlBulkCopy copier = null;
            try
            {
                // Instantiate a SqlBulkCopy object
                copier = new SqlBulkCopy(Connection, SqlBulkCopyOptions.TableLock, null)
                {
                    BulkCopyTimeout = 0,
                    BatchSize = 0,
                    EnableStreaming = true,
                    DestinationTableName = tableName
                };
                // The DataTable is presumed to lack any column corresponding to an IDENTITY column
                // of the database table (i.e., a column whose value is autogenerated by the server).
                // Because it is presumed that the DataTable and database do not have the same number
                // of columns, the SqlBulkCopy object can not automatically map the columns of the
                // DataTable to the columns of the database table.  Therefore, we must explicitly
                // create the ColumnMappings now.  This is simple, because the columns in the DataTable
                // should have the same names as the columns in the database table.
                foreach (DataColumn column in dataTable.Columns)
                    copier.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                // Insert the data into the database table.
                copier.WriteToServer(dataTable);
            }
            finally
            {
                // Dispose the SqlBulkCopy object.
                if (copier != null)
                    ((IDisposable)copier).Dispose();
                // Dispose the DataTable object.
                dataTable.Dispose();
                // Remove the DataTable object from the dictionary.
                BulkCopyDataTables.Remove(tableName);
            }
        }
    }
}
