using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Security.Cryptography;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class contains methods for turning a time series array into
    /// a BLOB (byte array) and visa-versa.  All of this class's methods
    /// are static, so the class does not need to be instantiated.
    /// </summary>
    public class TSBlobCoder
    {
        #region Method ConvertBlobToArrayRegular
        /// <summary>
        /// This method converts a BLOB (byte array) to an array of regular time step timeseries 
        /// values (double precision floats).  The caller must give parameters of the
        /// time series, such as time step size and start date.  The method will convert
        /// only a portion of the BLOB if the applyLimits parameter is true, according to
        /// the parameter values nReqValues, reqStartDate, and reqEndDate.  If the 
        /// applyLimits parameter is false, then the method converts the entire BLOB into
        /// the given array of values.  The array of values must have been allocated 
        /// large enough prior to calling this method.
        /// </summary>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, Year, or Irregular</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.</param>
        /// <param name="blobStartDate">Date of the first time step in the BLOB</param>
        /// <param name="applyLimits">If true, then the method will convert only a portion of the BLOB,
        /// according to the parameter values nReqValues, reqStartDate, and reqEndDate.  If false, the method
        /// converts the entire BLOB into a value array.</param>
        /// <param name="nReqValues">The maximum number of elements that will be converted into the array of values.
        /// If applyLimits==false, then this value is ignored.</param>
        /// <param name="reqStartDate">The earliest date in the time series that will be written to the array of values.
        /// If applyLimits==false, then this value is ignored.</param>
        /// <param name="reqEndDate">The latest date in the time series that will be written to the array of values.
        /// If applyLimits==false, then this value is ignored.</param>
        /// <param name="blobData">the BLOB that will be converted</param>
        /// <param name="valueArray">the array of time series values that is produced from the BLOB</param>
        /// <returns>The number of time steps that were actually written to valueArray</returns>
        public static unsafe int ConvertBlobToArrayRegular(
            TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
            DateTime blobStartDate, bool applyLimits,
            int nReqValues, DateTime reqStartDate, DateTime reqEndDate,
            Byte[] blobData, double[] valueArray)
        {
            // MemoryStream and BinaryReader objects enable bulk copying of data from the BLOB
            using (MemoryStream blobStream = new MemoryStream(blobData))
            using (BinaryReader blobReader = new BinaryReader(blobStream))
            {
                // How many elements of size 'double' are in the BLOB?
                int numBlobBin = (int)blobStream.Length;
                int numBlobValues = numBlobBin / sizeof(double);

                int numReadValues = numBlobValues;
                int numSkipValues = 0;
                int numTruncValues = 0;

                // Values might be skipped from the front or truncated from the end of the array,
                // but only if this flag is 'true'.
                if (applyLimits)
                {
                    // Do we skip any values at the front of the BLOB in order to fullfil the requested start date?
                    if (reqStartDate > blobStartDate)
                        numSkipValues = TSDateCalculator.CountSteps(blobStartDate, reqStartDate,
                                                                timeStepUnit, timeStepQuantity);
                    // compute the last date in the BLOB
                    DateTime blobEndDate = TSDateCalculator.IncrementDate
                                    (blobStartDate, timeStepUnit, timeStepQuantity, numBlobValues - 1);
                    // Do we truncate any values at the end of the BLOB in order to fulfill the requested end date?
                    if (reqEndDate < blobEndDate)
                        numTruncValues = TSDateCalculator.CountSteps(reqEndDate, blobEndDate,
                                                                timeStepUnit, timeStepQuantity);
                    // the number of values that can actually be read from the BLOB
                    numReadValues = Math.Min(numBlobValues - numSkipValues - numTruncValues, nReqValues);
                }

                // the number of bytes that will actually be read
                int numReadBin = numReadValues * sizeof(double);
                // the number of bytes that will be skipped
                int numSkipBin = numSkipValues * sizeof(double);

                // If we've got zero values to read, then we're done early!
                if (numReadValues <= 0)
                    return 0;

                // Transfer the entire array of data as a block
                Buffer.BlockCopy(blobReader.ReadBytes(numBlobBin), numSkipBin, valueArray, 0, numReadBin);

                return numReadValues;
            }
        } 
        #endregion


        #region Method ConvertBlobToArrayIrregular
        /// <summary>
        /// This method converts a BLOB (byte array) to an array of regular time step timeseries 
        /// values (date/value pairs stored in TSDateValueStruct).  The method will convert
        /// only a portion of the BLOB if the applyLimits parameter is true, according to
        /// the parameter values nReqValues, reqStartDate, and reqEndDate.  If the 
        /// applyLimits parameter is false, then the method converts the entire BLOB into
        /// the given array of values.  The array of values must have been allocated 
        /// large enough prior to calling this method.
        /// </summary>
        /// <param name="applyLimits">If true, then the method will convert only a portion of the BLOB,
        /// according to the parameter values nReqValues, reqStartDate, and reqEndDate.  If false, the method
        /// converts the entire BLOB into a value array.</param>
        /// <param name="nReqValues">The maximum number of elements that will be converted into the array of values.
        /// If applyLimits==false, then this value is ignored.</param>
        /// <param name="reqStartDate">The earliest date in the time series that will be written to the array of values.
        /// If applyLimits==false, then this value is ignored.</param>
        /// <param name="reqEndDate">The latest date in the time series that will be written to the array of values.
        /// If applyLimits==false, then this value is ignored.</param>
        /// <param name="blobData">the BLOB that will be converted</param>
        /// <param name="dateValueArray">the array of time series values that is produced from the BLOB</param>
        /// <returns>The number of time steps that were actually written to dateValueArray</returns>
        public static unsafe int ConvertBlobToArrayIrregular(bool applyLimits,
            int nReqValues, DateTime reqStartDate, DateTime reqEndDate,
            Byte[] blobData, TSDateValueStruct[] dateValueArray)
        {
            int numReadValues = 0;

            // MemoryStream and BinaryReader objects enable bulk copying of data from the BLOB
            using (MemoryStream blobStream = new MemoryStream(blobData))
            using (BinaryReader blobReader = new BinaryReader(blobStream))
            {
                // How many elements of 'TSDateValueStruct' are in the BLOB?
                int numBlobBin = (int)blobStream.Length;
                int numBlobValues = numBlobBin / sizeof(TSDateValueStruct);
                DateTime currDate;
                int j = 0;
                // Loop through all time steps in the BLOB
                for (int i = 0; i < numBlobValues; i++)
                {
                    // First check the date of this time step
                    currDate = DateTime.FromBinary(blobReader.ReadInt64());
                    // If date is before or after the dates requested by the caller, then
                    // we won't record the date/value info to the output array.
                    if (applyLimits)
                    {
                        if (currDate < reqStartDate)
                        {
                            blobReader.ReadDouble();
                            continue;
                        }
                        if (currDate > reqEndDate) break;
                    }
                    // Record the date and value to the output array.
                    dateValueArray[j].Date = currDate;
                    dateValueArray[j].Value = blobReader.ReadDouble();
                    j++;
                    // Don't overrun the array length specified by the caller
                    if (applyLimits) if (j >= nReqValues) break;
                }

                numReadValues = j;

                return numReadValues;
            }
        } 
        #endregion


        #region Method ConvertArrayToBlobRegular
        /// <summary>
        /// This method converts the given array of time series values (array of double precision
        /// floats) to a BLOB (byte array).
        /// </summary>
        /// <param name="TimeStepCount">The number of time steps in the given array of time series values</param>
        /// <param name="valueArray">The array of time series values to convert into a BLOB</param>
        /// <returns>The BLOB that is created from valueArray</returns>
        public static unsafe byte[] ConvertArrayToBlobRegular(
            int TimeStepCount, double[] valueArray)
        {
            // The number of bytes required for the BLOB
            int nBin = TimeStepCount * sizeof(double);
            // Allocate an array for the BLOB
            Byte[] blobData = new Byte[nBin];
            // Copy the array of doubles that was passed to the method into the byte array.  We skip
            // a bit of padding at the beginning that is used to compute the Checksum.  Thus, the
            // byte array (without the padding for Checksum) becomes the BLOB.
            Buffer.BlockCopy(valueArray, 0, blobData, 0, nBin);

            return blobData;
        } 
        #endregion


        #region Method ConvertArrayToBlobIrregular
        /// <summary>
        /// This method converts the given array of time series values (date/value pairs stored in 
        /// TSDateValueStruct) to a BLOB (byte array).
        /// </summary>
        /// <param name="TimeStepCount">The number of time steps in the given array of time series values</param>
        /// <param name="dateValueArray">The array of time series values to convert into a BLOB</param>
        /// <returns>The BLOB that is created from dateValueArray</returns>
        public static unsafe byte[] ConvertArrayToBlobIrregular(
            int TimeStepCount, TSDateValueStruct[] dateValueArray)
        {
            // The number of bytes required for the BLOB
            int nBin = TimeStepCount * sizeof(TSDateValueStruct);
            // Allocate an array for the BLOB
            Byte[] blobData = new Byte[nBin];

            // MemoryStream and BinaryWriter objects enable copying of data to the BLOB
            using (MemoryStream blobStream = new MemoryStream(blobData))
            using (BinaryWriter blobWriter = new BinaryWriter(blobStream))
            {
                // Loop through the entire array
                for (int i = 0; i < TimeStepCount; i++)
                {
                    // write the value to the BLOB as DATE followed by VALUE
                    blobWriter.Write(dateValueArray[i].Date.ToBinary());
                    blobWriter.Write(dateValueArray[i].Value);
                }
                return blobData;
            }
        } 
        #endregion


        #region ComputeChecksum() Methods
        /// <summary>
        /// Method computes an MD5 Checksum for the timeseries.  The input to the MD5 hash includes the 
        /// timeseries' BLOB of values, plus a TSParameters object that contains a short string of 
        /// numbers that TimeSeriesLibrary is responsible for keeping in accord with the BLOB.
        /// </summary>
        /// <param name="tsp"></param>
        /// <param name="blobData"></param>
        /// <returns></returns>
        public static byte[] ComputeChecksum(TSParameters tsp, List<ITimeSeriesTrace> traceList)
        {
            // simply unpack the TSParameters object and call the overload of this method
            return ComputeChecksum(tsp.TimeStepUnit, tsp.TimeStepQuantity,
                        tsp.TimeStepCount, tsp.BlobStartDate, tsp.BlobEndDate,
                        traceList);
        }

        /// <summary>
        /// This method computes an MD5 Checksum for the timeseries.  The input to the MD5 hash includes
        /// the list of parameters of the time series, and the list of checksums for each of the traces in
        /// the time series ensemble.  The list of the traces' checksums are passed to this method within 
        /// a list of ITimeSeriesTrace objects.
        /// </summary>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, Year, or Irregular</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.</param>
        /// <param name="timeStepCount">The number of time steps stored in the BLOB</param>
        /// <param name="blobStartDate">Date of the first time step in the BLOB</param>
        /// <param name="blobEndDate">Date of the last time step in the BLOB</param>
        /// <param name="traceList">a list of trace object whose checksums have already been computed.</param>
        /// <returns>the Checksum as a byte[16] array</returns>
        public static byte[] ComputeChecksum(
                    TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
                    int timeStepCount, DateTime blobStartDate, DateTime blobEndDate,
                    List<ITimeSeriesTrace> traceList)
        {
            // The MD5 Checksum will be computed from two basic parts.  The first part is
            // the list of the checksums of each trace in the time series.  The second is
            // the list of parameters that define the time series.  The Checksum will be
            // computed from these inputs expressed as byte arrays.  The first part--the
            // checksums of the individual traces--is already stored as a set of byte arrays.
            // For the second part we must take some extra measures to express the list of
            // parameters as a byte array.

            // This constant expresses the length of the byte array of parameters.  The
            // calculation of the constant must be in accord with the parameters that are 
            // actually assigned into the byte array below.
            const int LengthOfParamInputForChecksum =
                sizeof(TSDateCalculator.TimeStepUnitCode) +  // TimeStepUnit
                sizeof(short) +              // TimeStepQuantity
                sizeof(int) +                // TimeStepCount
                8 + 8;                       // StartDate and EndDate

            // Error check
            if (timeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular
                        && timeStepQuantity != 0)
            {
                throw new TSLibraryException(ErrCode.Enum.Checksum_Quantity_Nonzero,
                                "When the time step is irregular, the TimeStepQuantity must equal " +
                                "zero in order to ensure consistency in the checksum." );
            }

            // Byte array for the series of parameters that are fed into the MD5 algorithm
            byte[] binArray = new byte[LengthOfParamInputForChecksum];
            // MemoryStream and BinaryWriter objects allow us to write data into the byte array
            using (MemoryStream binStream = new MemoryStream(binArray))
            using (BinaryWriter binWriter = new BinaryWriter(binStream))
            {
                // Write relevant parameters (not including the BLOB itself) into a short byte array

                // TimeStepUnit
                binWriter.Write((short)timeStepUnit);
                // TimeStepQuantity
                binWriter.Write(timeStepQuantity);
                // TimeStepCount
                binWriter.Write(timeStepCount);
                // StartDate and EndDate
                binWriter.Write(blobStartDate.ToBinary());
                binWriter.Write(blobEndDate.ToBinary());

                // MD5CryptoServiceProvider object has methods to compute the Checksum
                using (MD5CryptoServiceProvider md5Hasher = new MD5CryptoServiceProvider())
                {
                    // make sure that we have a list of traces that is ordered by trace number
                    List<ITimeSeriesTrace> orderedTraceList = traceList.OrderBy(t => t.TraceNumber).ToList();
                    // loop through all traces
                    foreach (ITimeSeriesTrace traceObject in orderedTraceList)
                        // feed the checksum of the trace into the MD5 hash computer
                        md5Hasher.TransformBlock(traceObject.Checksum, 0, 16, null, 0);
                    // feed the short byte array of parameters into the MD5 hash computer
                    md5Hasher.TransformFinalBlock(binArray, 0, LengthOfParamInputForChecksum);
                    // return the hash (Checksum) value
                    return md5Hasher.Hash;
                }
            }
        }
        /// <summary>
        /// This method computes the checksum for an individual trace of a time series, where the time
        /// series is understood to be an ensemble of one or more traces.  The checksum of a trace is
        /// computed from the trace number and from the BLOB that contains the values for each time 
        /// step of the time series.
        /// </summary>
        /// <param name="traceObject">an ITimeSeriesTrace object that contains the trace number and the 
        /// BLOB for this trace.  The BLOB must be computed before calling this method, as the method will
        /// not compute it.</param>
        /// <returns>the Checksum as a byte[16] array</returns>
        public static byte[] ComputeTraceChecksum(ITimeSeriesTrace traceObject)
        {
            // The MD5 Checksum will be computed from two byte arrays.  The first byte array contains
            // the trace number and the second byte array is the time series array itself.

            // Byte array for the series of parameters that are fed into the MD5 algorithm first.
            byte[] binArray = new byte[sizeof(Int32)];
            // MemoryStream and BinaryWriter objects allow us to write data into the byte array
            using (MemoryStream binStream = new MemoryStream(binArray))
            using (BinaryWriter binWriter = new BinaryWriter(binStream))
            {
                // Write relevant parameters (not including the BLOB itself) into a short byte array

                // Trace Number
                binWriter.Write(traceObject.TraceNumber);

                // MD5CryptoServiceProvider object has methods to compute the Checksum
                using (MD5CryptoServiceProvider md5Hasher = new MD5CryptoServiceProvider())
                {
                    // feed the short byte array into the MD5 hash computer
                    md5Hasher.TransformBlock(binArray, 0, sizeof(Int32), binArray, 0);
                    // feed the BLOB of timeseries values into the MD5 hash computer
                    md5Hasher.TransformFinalBlock(traceObject.ValueBlob, 0, traceObject.ValueBlob.Length);
                    // return the hash (Checksum) value
                    return md5Hasher.Hash;
                }
            }
        }
        #endregion

    }
}
