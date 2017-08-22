using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class contains a collection of database connection objects that can be used
    /// by the time series library.  It contains methods for maintaining the collection
    /// of database connection objects.
    /// </summary>
    public class TSConnectionManager
    {

        #region Public properties
        /// <summary>
        /// Dictionary of connections keyed to a connection number
        /// </summary>
        public Dictionary<int, SqlConnection> TSConnectionsCollection
        {
            // get returns reference to the private field.
            // there is no set.
            get
            {
                // Initialize the collection if it has not already been initialized
                if (_tSConnectionsCollection == null)
                {
                    _tSConnectionsCollection = new Dictionary<int, SqlConnection>();
                }
                return _tSConnectionsCollection;
            }
        }
        private Dictionary<int, SqlConnection> _tSConnectionsCollection;
        /// <summary>
        /// Collection of wrapper objects for SqlCommand objects that have been cached using
        /// the SqlCommand.Prepare method.
        /// </summary>
        public List<TSSqlCommandContainer> PreparedSqlCommands = new List<TSSqlCommandContainer>();
        #endregion


        #region Public Methods
        
        /// <summary>
        /// Method opens a new connection according to the given connection string.  The connection
        /// is added to the TSConnectionsCollection and its key number is returned.
        /// </summary>
        public int OpenConnection(String connxString)
        {
            SqlConnection connx;
            try
            {   // instantiate the SqlConnection object
                connx = new SqlConnection(connxString);
            }
            catch
            {   // instantiation failed -- perhaps due to bad connection string
                return 0;
            }
            try
            {   // method opens the connection
                connx.Open();
            }
            catch
            {   // connection could not be opened
                connx.Dispose();
                return 0;
            }
            // What will be the new key (i.e., the connection number)
            int key = TSConnectionsCollection.Count + 1;
            // add to the collection (dictionary)
            TSConnectionsCollection.Add(key, connx);
            
            return key;
        }

        /// <summary>
        /// Method closes the connection corresponding the the given key (i.e., connection number).
        /// The closed connection is then removed from TSConnectionsCollection.
        /// </summary>
        public void CloseConnection(int key)
        {
            try
            {   // close the connection
                SqlConnection connx = TSConnectionsCollection[key];
                connx.Close();
                connx.Dispose();
            }
            catch
            {
                return;
            }
            // if the connection was successfully closed, then remove it from the collection
            TSConnectionsCollection.Remove(key);
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="key">The integer number of the database connection</param>
        ///// <param name="tableName">The name of the table into which the trace information is to
        ///// be stored (not the name of the staging table)</param>
        //public String BeginStagingTrace(int key, String tableName)
        //{
        //    // Create a name for the temporary table.  The initial # char signals to SQL Server
        //    // that the table is temporary.  Part of a randomly generated GUID is appended to
        //    // the table's name to avoid conflict with other threads.
        //    String tempTableName = "#staging_" + tableName
        //                + "_" + Guid.NewGuid().ToString("N").Substring(0, 12);
        //    // Add the temporary table name to a Dictionary for later recall
        //    TemporaryTraceTableNames[key].Add(tableName, tempTableName);

        //    return tempTableName;

        //    //// Create the temporary table in the database.  The SQL command is designed to ensure that
        //    //// the temporary table has exactly the same columns as the actual table. The "TOP 0" clause
        //    //// of the SELECT statement ensures that no data is copied into the new table.  Note that the
        //    //// temporary table will not have any indexes of any kind when created.
        //    //String commandText = "SELECT TOP 0 * INTO " + tempTableName
        //    //                    + " FROM " + tableName;
        //    //var command = new SqlCommand(commandText, tSConnectionsCollection[key]);
        //    //command.ExecuteNonQuery();
        //    //command.Dispose();

        //    //return tempTableName;
        //}
        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="key">The integer number of the database connection</param>
        ///// <param name="tableName">The name of the table into which the trace information is to
        ///// be stored (not the name of the staging table)</param>
        //public void CommitStagedTraces(int key, String tableName)
        //{
        //    String tempTableName;
        //    // Get the name of the temporary staging table from the Dictionary
        //    if (TemporaryTraceTableNames[key].TryGetValue(tableName, out tempTableName) == false)
        //        return;

        //    //// Copy all rows from the staging table into the actual table.  We are copying values
        //    //// from all columns except the 'Id' column.  The 'Id' column would cause an error since
        //    //// it has the IDENTITY property on it.  After the INSERT command copies all records from
        //    //// the staging table, the DROP command deletes the table entirely.
        //    //String text = "INSERT INTO " + tableName + "\n"
        //    //          + "SELECT TraceNumber, TimeStepCount, EndDate, ValueBlob, Checksum, TimeSeries_Id\n"
        //    //          + "FROM " + tempTableName + "\n\n"
        //    //          + "DROP TABLE " + tempTableName;
        //    //var command = new SqlCommand(text, tSConnectionsCollection[key]) { CommandTimeout = 600 };
        //    //command.ExecuteNonQuery();
        //    //command.Dispose();

        //    // Remove the name of the temporary staging table from the Dictionary, since it will
        //    // never be used again.
        //    TemporaryTraceTableNames[key].Remove(tableName);
        //}
        
        #endregion

    }
}
