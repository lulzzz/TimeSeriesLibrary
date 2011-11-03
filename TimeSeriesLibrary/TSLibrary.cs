﻿using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.IO;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// TSLibrary is the main class for the TimeSeriesLibrary.  It contains the callable
    /// functions of the library.
    /// </summary>
    public class TSLibrary
    {
        /// <summary>
        /// TSConnection object maintains a list of connections that have been opened by the library
        /// </summary>
        public TSConnection ConnxObject = new TSConnection();

        public ErrCodes.Enum ErrorCode;


        #region Public Methods for Connection
        /// <summary>
        /// Opens a new connection for the time series library to use.  The new connection 
        /// is added to a list and assigned a serial number.  The method returns the
        /// serial number of the new connection.
        /// </summary>
        /// <param name="connectionString">The connection string used to open the connection.</param>
        /// <returns>The serial number of the new connection.</returns>
        public int OpenConnection(String connectionString)
        {
            // all the logic is found in the TSConnection object.
            return ConnxObject.OpenConnection(connectionString);
        }
        /// <summary>
        /// Closes the connection identified with the given serial number.  When the
        /// connection is closed, it is removed from the list of connections available to
        /// the time series library, and the serial number no longer refers to this
        /// connection.
        /// </summary>
        /// <param name="connectionNumber">The serial number of the connection to be closed</param>
        public void CloseConnection(int connectionNumber)
        {
            // all the logic is found in the TSConnection object.
            ConnxObject.CloseConnection(connectionNumber);
        }
        #endregion


        #region Private methods for Connection
        /// <summary>
        /// Returns the SqlConnection object corresponding to the given connection number.
        /// </summary>
        /// <param name="connectionNumber">serial number of the connection within the collection</param>
        /// <returns></returns>
        private SqlConnection GetConnectionFromId(int connectionNumber)
        {
            SqlConnection connx;
            try
            {
                connx = ConnxObject.TSConnectionsCollection[connectionNumber];
            }
            catch
            {
                ErrorCode = ErrCodes.Enum.Connection_Not_Found;
                return null;
            }
            return connx;
        }
        #endregion


        #region Public methods for READING time series

        public unsafe int ReadValues(
                int connectionNumber, String tableName, Guid id,
                int nReqValues, double[] valueArray, DateTime reqStartDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);

            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            return ts.ReadValues(id, nReqValues, valueArray, reqStartDate);
        }
        
        #endregion


        #region Public methods for WRITING time series

        public unsafe int WriteValues(
                    int connectionNumber, String tableName,
                    short timeStepUnit, short timeStepQuantity,
                    int nOutValues, double[] valueArray, DateTime OutStartDate)
        {
            // Get the connection that we'll pass along.
            SqlConnection connx = GetConnectionFromId(connectionNumber);

            // Construct new TS object with SqlConnection object and table name
            TS ts = new TS(connx, tableName);

            return ts.WriteValues(timeStepUnit, timeStepQuantity, nOutValues, valueArray, OutStartDate);
        }

        #endregion


    }
}