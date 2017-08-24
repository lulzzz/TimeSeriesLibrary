using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

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
        public Dictionary<int, TSConnection> TSConnectionsCollection
        {
            // get returns reference to the private field.
            // there is no set.
            get
            {
                // Initialize the collection if it has not already been initialized
                if (_tSConnectionsCollection == null)
                {
                    _tSConnectionsCollection = new Dictionary<int, TSConnection>();
                }
                return _tSConnectionsCollection;
            }
        }
        private Dictionary<int, TSConnection> _tSConnectionsCollection;
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
            TSConnectionsCollection.Add(key, new TSConnection(connx));

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
                SqlConnection connx = TSConnectionsCollection[key].Connection;
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

        #endregion

    }
}
