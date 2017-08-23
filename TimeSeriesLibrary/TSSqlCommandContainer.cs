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
        /// <summary>
        /// The name of the table that the command is used for
        /// </summary>
        public String TableName;
        /// <summary>
        /// A string that distinguishes this command from others in the collection
        /// </summary>
        public String KeyString;
        /// <summary>
        /// The SqlCommand that is wrapped by this container object.
        /// </summary>
        public SqlCommand SqlCommand;

        /// <summary>
        /// Constructor.  The constructor will call the Prepare method on the SqlCommand
        /// object, so the caller does not need to call this method.
        /// </summary>
        /// <param name="tableName">The name of the table that the command is used for</param>
        /// <param name="keyString">A string that distinguishes this command from others in the collection</param>
        /// <param name="sqlCommand">The SqlCommand that is wrapped by this container object</param>
        public TSSqlCommandContainer(String tableName, String keyString, SqlCommand sqlCommand)
        {
            TableName = tableName;
            KeyString = keyString;
            SqlCommand = sqlCommand;
            
            // Prepare the command to ensure that it is cached by the database system.
            // http://social.msdn.microsoft.com/Forums/en-US/adodotnetdataproviders/thread/a702d6eb-54bd-492b-9715-59bac182263d/
            SqlCommand.Prepare();
        }


    }
}
