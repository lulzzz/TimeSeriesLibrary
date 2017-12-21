using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Security.Cryptography;
using Oasis.Foundation.Infrastructure;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class contains methods for turning a time series array into
    /// a BLOB (byte array) and visa-versa.  All of this class's methods
    /// are static, so the class does not need to be instantiated.
    /// </summary>
    public static class TSBlobCoder
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
        /// <param name="timeStepCount">the number of time steps that are stored in the blob</param>
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
        /// <param name="compressionCode">a generation number that indicates what compression technique to use</param>
        /// <returns>The number of time steps that were actually written to valueArray</returns>
        public static unsafe int ConvertBlobToArrayRegular(
            TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
            int timeStepCount, DateTime blobStartDate, bool applyLimits,
            int nReqValues, DateTime reqStartDate, DateTime reqEndDate,
            Byte[] blobData, double[] valueArray, int compressionCode)
        {
            // The BLOB is kept in a compressed form, so our first step is to decompress it before
            // anything else can be done.
            Byte[] decompressedBlobData
                    = DecompressBlob(blobData, timeStepCount * sizeof(double), compressionCode);

            // MemoryStream and BinaryReader objects enable bulk copying of data from the BLOB
            using (MemoryStream blobStream = new MemoryStream(decompressedBlobData))
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
        /// <param name="timeStepCount">the number of time steps that are stored in the blob</param>
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
        /// <param name="compressionCode">a generation number that indicates what compression technique to use</param>
        /// <returns>The number of time steps that were actually written to dateValueArray</returns>
        public static unsafe int ConvertBlobToArrayIrregular(int timeStepCount, bool applyLimits,
            int nReqValues, DateTime reqStartDate, DateTime reqEndDate,
            Byte[] blobData, TSDateValueStruct[] dateValueArray, int compressionCode)
        {
            int numReadValues = 0;

            // The BLOB is kept in a compressed form, so our first step is to decompress it before
            // anything else can be done.
            Byte[] decompressedBlobData
                    = DecompressBlob(blobData, timeStepCount * sizeof(TSDateValueStruct), compressionCode);

            // MemoryStream and BinaryReader objects enable bulk copying of data from the BLOB
            using (MemoryStream blobStream = new MemoryStream(decompressedBlobData))
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
        /// floats) to a BLOB (byte array).  It also sets the computes the checksum from the resultant
        /// BLOB, and then sets the Checksum and ValueBlob properties of the given ITimeSeriesTrace
        /// object accordingly.
        /// </summary>
        /// <param name="valueArray">The array of time series values to convert into a BLOB</param>
        /// <param name="compressionCode">a generation number that indicates what compression technique to use</param>
        /// <param name="traceObject">object whose TraceNumber property will be used to compute the checksum,
        /// and whose properties will be assigned by this method</param>
        /// <returns>The BLOB that is created from valueArray</returns>
        public static unsafe byte[] ConvertArrayToBlobRegular(
                    double[] valueArray, int compressionCode, ITimeSeriesTrace traceObject)
        {
            // The number of bytes required for the BLOB
            int nBin = traceObject.TimeStepCount * sizeof(double);
            // Allocate an array for the BLOB
            Byte[] blobData = new Byte[nBin];
            // Copy the array of doubles that was passed to the method into the byte array.  
            // The byte array becomes the BLOB.
            Buffer.BlockCopy(valueArray, 0, blobData, 0, nBin);

            // Compute the checksum using the uncompressed BLOB.  During development, it was 
            // demonstrated that the checksum would be computed faster on the compressed BLOB.
            // However, this could make it difficult to upgrade the compression algorithm in the
            // future, because the checksum value would be dependent on the compression algorithm.
            Byte[] checksum = ComputeTraceChecksum(traceObject.TraceNumber, blobData);
            Boolean checksumChanged = (MurmurHash.ByteArraysAreEqual(traceObject.Checksum, checksum) == false);
            // If the checksum did not change, then we will not assign any properties to the traceObject.
            // The result will be that we will return the original ValueBlob.  If the checksum did change,
            // then we compute a new compressed ValueBlob and assign the new values.
            if (checksumChanged)
            {
                traceObject.Checksum = checksum;
                // the BLOB is stored in a compressed form, so our last step is to compress it
                traceObject.ValueBlob = CompressBlob(blobData, compressionCode);
            }
            return traceObject.ValueBlob;
        } 
        #endregion


        #region Method ConvertArrayToBlobIrregular
        /// <summary>
        /// This method converts the given array of time series values (date/value pairs stored in 
        /// TSDateValueStruct) to a BLOB (byte array).  It also sets the computes the checksum from the 
        /// resultant BLOB, and then sets the Checksum and ValueBlob properties of the given 
        /// ITimeSeriesTrace object accordingly.
        /// </summary>
        /// <param name="dateValueArray">The array of time series values to convert into a BLOB</param>
        /// <param name="compressionCode">a generation number that indicates what compression technique to use</param>
        /// <param name="traceObject">object whose TraceNumber property will be used to compute the checksum,
        /// and whose properties will be assigned by this method</param>
        /// <returns>The BLOB that is created from dateValueArray</returns>
        public static unsafe byte[] ConvertArrayToBlobIrregular(
                    TSDateValueStruct[] dateValueArray, int compressionCode, ITimeSeriesTrace traceObject)
        {
            // The number of bytes required for the BLOB
            int nBin = traceObject.TimeStepCount * sizeof(TSDateValueStruct);
            // Allocate an array for the BLOB
            Byte[] blobData = new Byte[nBin];

            // MemoryStream and BinaryWriter objects enable copying of data to the BLOB
            using (MemoryStream blobStream = new MemoryStream(blobData))
            using (BinaryWriter blobWriter = new BinaryWriter(blobStream))
            {
                // Loop through the entire array
                for (int i = 0; i < traceObject.TimeStepCount; i++)
                {
                    // write the value to the BLOB as DATE followed by VALUE
                    blobWriter.Write(dateValueArray[i].Date.ToBinary());
                    blobWriter.Write(dateValueArray[i].Value);
                }
            }

            // Compute the checksum using the uncompressed BLOB.  During development, it was 
            // demonstrated that the checksum would be computed faster on the compressed BLOB.
            // However, this could make it difficult to upgrade the compression algorithm in the
            // future, because the checksum value would be dependent on the compression algorithm.
            Byte[] checksum = ComputeTraceChecksum(traceObject.TraceNumber, blobData);
            Boolean checksumChanged = (MurmurHash.ByteArraysAreEqual(traceObject.Checksum, checksum) == false);
            // If the checksum did not change, then we will not assign any properties to the traceObject.
            // The result will be that we will return the original ValueBlob.  If the checksum did change,
            // then we compute a new compressed ValueBlob and assign the new values.
            if (checksumChanged)
            {
                traceObject.Checksum = checksum;
                // the BLOB is stored in a compressed form, so our last step is to compress it
                traceObject.ValueBlob = CompressBlob(blobData, compressionCode);
            }
            return traceObject.ValueBlob;
        } 
        #endregion


        #region ComputeChecksum() Methods
        /// <summary>
        /// Method computes a Checksum for the timeseries.  The input to the hash includes the 
        /// timeseries' BLOB of values, plus a TSParameters object that contains a short string of 
        /// numbers that TimeSeriesLibrary is responsible for keeping in accord with the BLOB.
        /// </summary>
        /// <param name="tsp">TSParameters object that contains the parameters of the time series</param>
        /// <param name="traceList">collection of trace objects for the time series</param>
        /// <returns>the Checksum as a byte[16] array</returns>
        public static byte[] ComputeChecksum(TSParameters tsp, List<ITimeSeriesTrace> traceList)
        {
            // simply unpack the TSParameters object and call the overload of this method
            return ComputeChecksum(tsp.TimeStepUnit, tsp.TimeStepQuantity, tsp.BlobStartDate, traceList);
        }

        /// <summary>
        /// This method computes a Checksum for the timeseries.  The input to the hash includes
        /// the list of parameters of the time series, and the list of checksums for each of the traces in
        /// the time series ensemble.  The list of the traces' checksums are passed to this method within 
        /// a list of ITimeSeriesTrace objects.
        /// </summary>
        /// <param name="timeStepUnit">TSDateCalculator.TimeStepUnitCode value for Minute,Hour,Day,Week,Month, Year, or Irregular</param>
        /// <param name="timeStepQuantity">The number of the given unit that defines the time step.
        /// For instance, if the time step is 6 hours long, then this value is 6.</param>
        /// <param name="blobStartDate">Date of the first time step in the BLOB</param>
        /// <param name="traceList">a list of trace object whose checksums have already been computed.</param>
        /// <returns>the Checksum as a byte[16] array</returns>
        public static byte[] ComputeChecksum(
                    TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
                    DateTime blobStartDate, List<ITimeSeriesTrace> traceList)
        {
            // Error check
            if (timeStepUnit == TSDateCalculator.TimeStepUnitCode.Irregular
                        && timeStepQuantity != 0)
                throw new TSLibraryException(ErrCode.Enum.Checksum_Quantity_Nonzero,
                                "When the time step is irregular, the TimeStepQuantity must equal " +
                                "zero in order to ensure consistency in the checksum.");

            byte[] binArray = new byte[sizeof(short) * 2 + sizeof(Double) + 16 * traceList.Count];
            using (MemoryStream binStream = new MemoryStream(binArray))
            using (BinaryWriter binWriter = new BinaryWriter(binStream))
            {
                binWriter.Write((short)timeStepUnit);
                binWriter.Write(timeStepQuantity);
                binWriter.Write(blobStartDate.ToBinary());
                foreach (var t in traceList.OrderBy(t => t.TraceNumber))
                    binWriter.Write(t.Checksum);

                // MurmurHash is used instead of xxHash b/c:
                //  1) It was already established
                //  2) MurmurHash produces 32-byte has, as opposed to 16-byte hash of xxHash, which
                //     should reduce the chance of collision.
                //  3) xxHash was later chosen for the traces b/c speed of the algorithm was a problem
                //     with the large input for the trace.  The input to the overall timeseries object
                //     is much smaller, and so there is much less to be gained by switching to xxHash.
                return new MurmurHash().ComputeHash(binArray);
            }
        }
        /// <summary>
        /// This method computes the checksum for an individual trace of a time series, where the time
        /// series is understood to be an ensemble of one or more traces.  The checksum of a trace is
        /// computed from the trace number and from the BLOB that contains the values for each time 
        /// step of the time series.
        /// </summary>
        /// <param name="traceNumber">the number for identifying the trace</param>
        /// <param name="valueBlob">the BLOB that contains the values for each time step
        /// <returns>the Checksum as a byte[16] array</returns>
        public static byte[] ComputeTraceChecksum(int traceNumber, byte[] valueBlob)
        {
            byte[] binArray = new byte[sizeof(Int32) + valueBlob.Length];
            using (MemoryStream binStream = new MemoryStream(binArray))
            using (BinaryWriter binWriter = new BinaryWriter(binStream))
            {
                binWriter.Write(traceNumber);
                binWriter.Write(valueBlob);
                // plan to follow the example at https://stackoverflow.com/a/5896716/2998072
                // in order to pad the end with zeros and thereby create a 32-byte array in
                // order to avoid upsetting code that was established when this checksum
                // was based on 32-byte MurmurHash.
                return BitConverter.GetBytes(LZ4.GetXXHash64(binArray));
            }
        }
        #endregion


        #region Compression Methods
        /// <summary>
        /// This constant tells us what the current value is for the generation number of
        /// compression approach.  In case the compression methods of this class ever change
        /// the previous algorithm for decompressing the data should not be deleted, but can
        /// still be used by invoking the compression code that was current at the time that
        /// an old time series was compressed.
        /// </summary>
        public const int currentCompressionCode = 2;

        /// <summary>
        /// This returns a compressed version of the given byte array
        /// </summary>
        /// <param name="uncompressedBlob">the uncompressed byte array</param>
        /// <param name="compressionCode">a generation number that indicates what compression technique to use</param>
        /// <returns>the compressed byte array</returns>
        public static unsafe Byte[] CompressBlob(Byte[] uncompressedBlob, int compressionCode)
        {
            if (compressionCode == 1)
            {
                Byte[] compressedBlob;
                // the byte-array length of the input blob
                int inputLength = uncompressedBlob.Length;
                // The byte array that will be created by the compression.
                // Note that some incompressible BLOBs will actually be made larger by LZFX
                // compression.  We have observed about 1% increase over the original BLOB,
                // so the factor of 1.05 is expected to be safe.  We add 16 since the factor
                // of 1.05 is insufficient when the BLOB is very small.
                compressedBlob = new Byte[(int)(inputLength * 1.05) + 16];

                // Compress using LZFX algorithm.
                // This method resizes the compressed byte array for us.
                LZFX.Compress(uncompressedBlob, ref compressedBlob);
                return compressedBlob;
            }
            if (compressionCode == 2)
            {
                Byte[] compressedBlob;
                // the byte-array length of the input blob
                int inputLength = uncompressedBlob.Length;
                // The byte array that will be created by the compression.
                // Note that some incompressible BLOBs might actually be made larger by LZ4
                // compression.  The GetMaxSize method calls a function built into LZ4 which
                // returns the maximum possible size of the output array. LZ4 documentation suggests
                // that speed is optimized by allocating this max size.
                compressedBlob = new Byte[(int)LZ4.GetMaxSize(inputLength)];

                // Compress using LZ4 algorithm.
                // This method resizes the compressed byte array for us.
                LZ4.Compress(uncompressedBlob, ref compressedBlob, 4);
                return compressedBlob;
            }
            else
            {   // return without doing any compression
                return uncompressedBlob;
            }

        }
        /// <summary>
        /// This method returns an uncompressed version of the given compressed byte array
        /// </summary>
        /// <param name="inputBlob">the byte array to be decompressed</param>
        /// <param name="decompressedLength">the known length of the decompressed byte array</param>
        /// <param name="compressionCode">a generation number that indicates what compression technique to use</param>
        /// <returns>the decompressed byte array</returns>
        public static unsafe Byte[] DecompressBlob(Byte[] inputBlob, int decompressedLength,
                                                    int compressionCode)
        {
            if (compressionCode == 1)
            {
                // the byte array of the output blob
                Byte[] decompressedBlob = new Byte[decompressedLength];
                // Decompress using LZFX algorithm.
                // This method will throw an exception if the decompressed data does not
                // exactly fit into the allocated array size.
                LZFX.Decompress(inputBlob, decompressedBlob);

                return decompressedBlob;
            }
            if (compressionCode == 2)
            {
                // the byte array of the output blob
                Byte[] decompressedBlob = new Byte[decompressedLength];
                // Decompress using LZFX algorithm.
                // This method will throw an exception if the decompressed data does not
                // exactly fit into the allocated array size.
                LZ4.Decompress(inputBlob, decompressedBlob);

                return decompressedBlob;
            }
            else
            {   // return without doing any compression
                return inputBlob;
            }
        }
        #endregion
    }
}
