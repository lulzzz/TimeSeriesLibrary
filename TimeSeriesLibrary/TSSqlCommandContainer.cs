using System;
using System.Data.SqlClient;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// An object of this class wraps a SqlCommand object, together with fields that
    /// allow the command to be looked up by the code that needs to reuse the command.
    /// The purpose of this container is to ensure that commands are efficiently cached
    /// and reused by the database system.
    /// </summary>
    public class TSSqlCommandContainer
    {
        public int ConnectionId;
        public SqlConnection SqlConnection;
        public String TableName;
        public String KeyString;
        public SqlCommand SqlCommand;

        /// <summary>
        /// Constructor.  The constructor will call the Prepare method on the SqlCommand
        /// object, so the caller does not need to call this method.
        /// </summary>
        public TSSqlCommandContainer(int connectionId, SqlConnection sqlConnection, 
                    String tableName, String keyString, SqlCommand sqlCommand)
        {
            ConnectionId = connectionId;
            SqlConnection = sqlConnection;
            TableName = tableName;
            KeyString = keyString;
            SqlCommand = sqlCommand;
            
            // Prepare the command to ensure that it is cached by the database system.
            // http://social.msdn.microsoft.com/Forums/en-US/adodotnetdataproviders/thread/a702d6eb-54bd-492b-9715-59bac182263d/
            SqlCommand.Prepare();
        }


    }
}
