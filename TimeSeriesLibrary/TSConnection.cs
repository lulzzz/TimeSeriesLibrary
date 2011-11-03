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
    public class TSConnection
    {

        #region Private Fields
        /// <summary>
        /// Dictionary of connections keyed to a connection number
        /// </summary>
        private Dictionary<int, SqlConnection> tSConnectionsCollection;
        #endregion


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
                if (tSConnectionsCollection == null)
                {
                    tSConnectionsCollection = new Dictionary<int, SqlConnection>();
                }
                return tSConnectionsCollection;
            }
        }
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
            {   // the constructor opens the connection
                connx = new SqlConnection(connxString);
            }
            catch
            {   // The connection process failed--the connection will
                // not be added to the collection.
                return 0;
            }
            // What will be the new key (i.e., the connection number)
            int key = TSConnectionsCollection.Count + 1;
            // add to the collection (dictionary)
            tSConnectionsCollection.Add(key, connx);
            
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
            }
            catch
            {
                return;
            }
            // if the connection was successfully closed, then remove it from the collection
            tSConnectionsCollection.Remove(key);
        }
        #endregion

    }
}
