using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TimeSeriesLibrary;
using System.Data.SqlClient;
using System.Data;

namespace Sandbox
{
    class DataTableExperiment
    {
        public String TableName = "OutputTimeSeriesTraces";
        public SqlConnection Connx;
        const int nVals = 400, nIter = 1200;


        public void Test(Boolean inBulk, Boolean doParam)
        {
            // Create dummy time series that we can write to the database
            var valList = new List<double[]>();
            for (int i = 0; i < nIter; i++)
            {
                var iterList = new List<double>();
                for (int t = 0; t < nVals; t++)
                {
                    iterList.Add(1.5 * t + i + 0.33);
                }
                valList.Add(iterList.ToArray());
            }


            if (inBulk)
            {
                var traceObjects = new List<ITimeSeriesTrace>();
                for (int i = 0; i < nIter; i++)
                {
                    ITimeSeriesTrace traceObject = new TSTrace { TraceNumber = 1 };
                    // Convert the array of double values into a byte array...a BLOB
                    traceObject.ValueBlob = TSBlobCoder.ConvertArrayToBlobRegular
                                (nVals, valList[i], TSBlobCoder.currentCompressionCode, traceObject);
                    traceObjects.Add(traceObject);
                }
                WriteBulkTraces(traceObjects);
            }
            else if (doParam)
            {
                // reference: http://stackoverflow.com/questions/2449827/pros-and-cons-of-using-sqlcommand-prepare-in-c
                // this does appear to be faster!!

                SqlCommand cmd = new SqlCommand("INSERT INTO " + TableName
                                    + "(TimeSeries_Id, TraceNumber, ValueBlob, Checksum) "
                                    + "VALUES (@TimeSeries_Id, @TraceNumber, @ValueBlob, @Checksum)", Connx);
                cmd.Parameters.Add("@TimeSeries_Id", SqlDbType.Int);
                cmd.Parameters.Add("@TraceNumber", SqlDbType.Int);
                cmd.Parameters.Add("@ValueBlob", SqlDbType.VarBinary, -1);
                cmd.Parameters.Add("@Checksum", SqlDbType.Binary, 16);
                cmd.Prepare();

                SqlCommand selectCmd = new SqlCommand("SELECT  TraceNumber, Checksum from "
                            + TableName + " where TimeSeries_Id=@TimeSeries_Id order by TraceNumber", Connx);
                selectCmd.Parameters.Add("@TimeSeries_Id", SqlDbType.Int);
                selectCmd.Prepare();

                SqlCommand updateCmd = new SqlCommand("UPDATE " + "OutputTimeSeries"
                                        + " SET Checksum=@Checksum WHERE Id=@Id", Connx);
                updateCmd.Parameters.Add("@Checksum", SqlDbType.Binary, 16);
                updateCmd.Parameters.Add("@Id", SqlDbType.Int);
                updateCmd.Prepare();


                for (int i = 0; i < nIter; i++)
                {
                    ITimeSeriesTrace traceObject = new TSTrace { TraceNumber = 1 };
                    // Convert the array of double values into a byte array...a BLOB
                    traceObject.ValueBlob = TSBlobCoder.ConvertArrayToBlobRegular
                                (nVals, valList[i], TSBlobCoder.currentCompressionCode, traceObject);
                    WriteOneTraceParam(i + 1, traceObject, cmd);

                    // update the checksum in the parameters table
                    UpdateParametersChecksum(i + 1, selectCmd, updateCmd);
                }
            }
            else
            {
                for (int i = 0; i < nIter; i++)
                {
                    ITimeSeriesTrace traceObject = new TSTrace { TraceNumber = 1 };
                    // Convert the array of double values into a byte array...a BLOB
                    traceObject.ValueBlob = TSBlobCoder.ConvertArrayToBlobRegular
                                (nVals, valList[i], TSBlobCoder.currentCompressionCode, traceObject);
                    WriteOneTrace(traceObject);
                }
            }
        }


        private unsafe void WriteBulkTraces(List<ITimeSeriesTrace> traceObjects)
        {
            // SQL statement that gives us a resultset for the DataTable object.  Note that
            // this query is rigged so that it will always return 0 records.  This is because
            // we only want the resultset to define the fields of the DataTable object.
            String comm = BuildStringForEmptyTraceDataTable();
            // SqlDataAdapter object will use the query to fill the DataTable
            using (SqlDataAdapter adp = new SqlDataAdapter(comm, Connx))
            {
                // SqlCommandBuilder object must be instantiated in order for us to call
                // the Update method of the SqlDataAdapter.  Interestingly, we only need to
                // instantiate this object--we don't need to use it in any other way.
                using (SqlCommandBuilder bld = new SqlCommandBuilder(adp))
                {
                    DataTable dTable = new DataTable();
                    // Execute the query to fill the DataTable object
                    try
                    {
                        adp.Fill(dTable);
                    }
                    catch (Exception e)
                    {   // The query failed
                        throw new TSLibraryException(ErrCode.Enum.Could_Not_Open_Table,
                                        "Table '" + TableName + "' could not be opened using query:\n\n" + comm, e);
                    }
                    foreach (var traceObject in traceObjects)
                    {
                        // DataRow object represents the current row of the DataTable object, which in turn
                        // represents a record that we will add to the database table.
                        DataRow currentRow = dTable.NewRow();

                        // transfer all of the data into the DataRow object
                        currentRow["TimeSeries_Id"] = 1;
                        currentRow["TraceNumber"] = traceObject.TraceNumber;
                        currentRow["ValueBlob"] = traceObject.ValueBlob;
                        currentRow["Checksum"] = traceObject.Checksum;
                        dTable.Rows.Add(currentRow);
                    }
                    // Save the DataRow object to the database
                    adp.Update(dTable);
                    dTable.Dispose();
                }
            }
        }
        private unsafe void WriteOneTrace(ITimeSeriesTrace traceObject)
        {
            // SQL statement that gives us a resultset for the DataTable object.  Note that
            // this query is rigged so that it will always return 0 records.  This is because
            // we only want the resultset to define the fields of the DataTable object.
            String comm = BuildStringForEmptyTraceDataTable();
            // SqlDataAdapter object will use the query to fill the DataTable
            using (SqlDataAdapter adp = new SqlDataAdapter(comm, Connx))
            {
                // SqlCommandBuilder object must be instantiated in order for us to call
                // the Update method of the SqlDataAdapter.  Interestingly, we only need to
                // instantiate this object--we don't need to use it in any other way.
                using (SqlCommandBuilder bld = new SqlCommandBuilder(adp))
                {
                    DataTable dTable = new DataTable();
                    // Execute the query to fill the DataTable object
                    try
                    {
                        adp.Fill(dTable);
                    }
                    catch (Exception e)
                    {   // The query failed
                        throw new TSLibraryException(ErrCode.Enum.Could_Not_Open_Table,
                                        "Table '" + TableName + "' could not be opened using query:\n\n" + comm, e);
                    }
                    // DataRow object represents the current row of the DataTable object, which in turn
                    // represents a record that we will add to the database table.
                    DataRow currentRow = dTable.NewRow();

                    // transfer all of the data into the DataRow object
                    currentRow["TimeSeries_Id"] = 1;
                    currentRow["TraceNumber"] = traceObject.TraceNumber;
                    currentRow["ValueBlob"] = traceObject.ValueBlob;
                    currentRow["Checksum"] = traceObject.Checksum;
                    dTable.Rows.Add(currentRow);
                    // Save the DataRow object to the database
                    adp.Update(dTable);
                    dTable.Dispose();
                }
            }
        }
        private unsafe void WriteOneTraceParam(int id, ITimeSeriesTrace traceObject, SqlCommand cmd)
        {
            cmd.Parameters["@TimeSeries_Id"].Value = id;
            cmd.Parameters["@TraceNumber"].Value = 26;
            cmd.Parameters["@ValueBlob"].Value = traceObject.ValueBlob;
            cmd.Parameters["@Checksum"].Value = traceObject.ValueBlob;
            cmd.ExecuteNonQuery();
        }
        /// <summary>
        /// This method updates the value in the Checksum field of the parameters table.
        /// It does not modify any other fields.
        /// </summary>
        /// <param name="doWriteToDB">true if the method should actually save the timeseries to the database</param>
        /// <param name="tsImport">TSImport object into which the method will record values that it has computed.
        /// If this parameter is null, then the method will skip the recording of such paramters to an object.</param>
        private void UpdateParametersChecksum(int id, SqlCommand selectCmd, SqlCommand updateCmd)
        {
            //// This List will contain one item for each trace for this time series.  The primary
            //// purpose of the list is to store the checksum for each trace, since the checksum
            //// of the timeseries is computed from the list of checksums from each of its traces.
            //List<ITimeSeriesTrace> traceObjects = new List<ITimeSeriesTrace>();

            ////
            //// Query the Trace table to get all traces for this time series
            //selectCmd.Parameters["@TimeSeries_Id"].Value = id;

            //// SqlDataAdapter object will use the query to fill the DataTable
            //using (SqlDataAdapter adp = new SqlDataAdapter(selectCmd))
            //// SqlCommandBuilder object must be instantiated in order for us to call
            //// the Fill method of the SqlDataAdapter.  Interestingly, we only need to
            //// instantiate this object--we don't need to use it in any other way.
            //using (SqlCommandBuilder bld = new SqlCommandBuilder(adp))
            //using (DataTable dTable = new DataTable())
            //{
            //    // Execute the query to fill the DataTable object
            //    try
            //    {
            //        adp.Fill(dTable);
            //    }
            //    catch (Exception e)
            //    {   // The query failed
            //        throw new TSLibraryException(ErrCode.Enum.Could_Not_Open_Table,
            //                        "Failed to execute query:\n\n"
            //                        + selectCmd.CommandText, e);
            //    }
            //    // Loop through all traces, and record the values that will be needed to compute the
            //    // checksum of the ensemble time series.  Note that this does not include the BLOB 
            //    // of the trace.
            //    foreach (DataRow row in dTable.Rows)
            //    {
            //        ITimeSeriesTrace traceObject = new TSTrace();
            //        traceObject.TraceNumber = (int)row["TraceNumber"];
            //        traceObject.Checksum = (byte[])row["Checksum"];
            //        traceObjects.Add(traceObject);
            //    }
            //}
            //// Compute the new checksum of the ensemble
            //byte[] checksum = TSBlobCoder.ComputeChecksum(TSDateCalculator.TimeStepUnitCode.Day, 3, nVals,
            //                         DateTime.Now, DateTime.Now, traceObjects);


            // dummy!
            var random = new Random(DateTime.Now.Millisecond);
            byte[] checksum = new byte[16];
            random.NextBytes(checksum);

            // 
            // Write the new value to the parameters table
            // Supply parameter values for the SqlCommand
            updateCmd.Parameters["@Id"].Value = id;
            updateCmd.Parameters["@Checksum"].Value = checksum;
            updateCmd.ExecuteNonQuery();
        }

        
        
        
        
        
        
        
        
        
        
        /// <summary>
        /// Method returns a string for querying the database table and returning an empty result set.
        /// The subsequent query can be used to create an empty DataTable object, with the necessary
        /// columns defined.  Because the query names all required fields of the database table, the
        /// subsequent query will raise an exception if any fields are missing.
        /// </summary>
        /// <returns>The SQL command that returns an empty resultset</returns>
        String BuildStringForEmptyTraceDataTable()
        {
            // note: by including 'where 1=0', we ensure that an empty resultset will be returned.
            return String.Format("select TraceNumber, ValueBlob, TimeSeries_Id, Checksum" +
                                 "  from {0} where 1=0", TableName);
        }
    }
}
