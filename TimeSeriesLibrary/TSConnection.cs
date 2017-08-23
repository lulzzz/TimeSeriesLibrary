using System;
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

    }
}
