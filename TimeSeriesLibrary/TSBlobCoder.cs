using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace TimeSeriesLibrary
{
    /// <summary>
    /// This class contains methods for turning a time series array
    /// into a BLOB (byte array) and visa-versa.
    /// </summary>
    class TSBlobCoder
    {
        public static unsafe int ConvertBlobToArrayRegular(
                    TSDateCalculator.TimeStepUnitCode timeStepUnit, short timeStepQuantity,
                    DateTime blobStartDate, DateTime blobEndDate,
                    int nReqValues, DateTime reqStartDate, DateTime reqEndDate,
                    Byte[] blobData, double[] valueArray )
        {
            int numReadValues = 0;

            // MemoryStream and BinaryReader objects enable bulk copying of data from the BLOB
            MemoryStream blobStream = new MemoryStream(blobData);
            BinaryReader blobReader = new BinaryReader(blobStream);
            // How many elements of size 'double' are in the BLOB?
            int numBlobBin = (int)blobStream.Length;
            int numBlobValues = numBlobBin / sizeof(double);
            // Do we skip any values at the front of the BLOB in order to fullfil the requested start date?
            int numSkipValues = 0;
            if (reqStartDate > blobStartDate)
                numSkipValues = TSDateCalculator.CountSteps(blobStartDate, reqStartDate, timeStepUnit, timeStepQuantity);
            // convert the number of skipped values from number of doubles to number of bytes
            int numSkipBin = numSkipValues * sizeof(double);
            // Do we truncate any values at the end of the BLOB in order to fulfill the requested end date?
            int numTruncValues = 0;
            if (reqEndDate < blobEndDate)
                numTruncValues = TSDateCalculator.CountSteps(reqEndDate, blobEndDate, timeStepUnit, timeStepQuantity);
            // the number of values that can actually be read from the BLOB
            numReadValues = Math.Min(numBlobValues - numSkipValues - numTruncValues, nReqValues);
            int numReadBin = numReadValues * sizeof(double);
            // If we've got zero values to read, then we're done early!
            if (numReadValues <= 0)
                return 0;

            // Transfer the entire array of data as a block
            Buffer.BlockCopy(blobReader.ReadBytes(numBlobBin), numSkipBin, valueArray, 0, numReadBin);
            
            return numReadValues;
        }


        public static unsafe int ConvertBlobToArrayIrregular(
                    int nReqValues, DateTime reqStartDate, DateTime reqEndDate,
                    Byte[] blobData, TimeSeriesValue[] dateValueArray )
        {
            int numReadValues = 0;

            // MemoryStream and BinaryReader objects enable bulk copying of data from the BLOB
            MemoryStream blobStream = new MemoryStream(blobData);
            BinaryReader blobReader = new BinaryReader(blobStream);
            // How many elements of 'TimeSeriesValue' are in the BLOB?
            int numBlobBin = (int)blobStream.Length;
            int numBlobValues = numBlobBin / sizeof(TimeSeriesValue);
            DateTime currDate;
            int j = 0;
            // Loop through all time steps in the BLOB
            for (int i = 0; i < numBlobValues; i++)
            {
                // First check the date of this time step
                currDate = DateTime.FromBinary(blobReader.ReadInt64());
                // If date is before or after the dates requested by the caller, then
                // we won't record the date/value info to the output array.
                if (currDate < reqStartDate) continue;
                if (currDate > reqEndDate) break;
                // Record the date and value to the output array.
                dateValueArray[j].Date = currDate;
                dateValueArray[j].Value = blobReader.ReadDouble();
                j++;
                // Don't overrun the array length specified by the caller
                if (j >= nReqValues) break;
            }

            numReadValues = j;

            return numReadValues;
        }


        public static unsafe byte[] ConvertArrayToBlobRegular(
                    int TimeStepCount, double[] valueArray )
        {
            // The number of bytes required for the BLOB
            int nBin = TimeStepCount * sizeof(double);
            // Allocate an array for the BLOB
            Byte[] blobData = new Byte[nBin];
            // Copy the array of doubles that was passed to the method into the byte array.  We skip
            // a bit of padding at the beginning that is used to compute the checksum.  Thus, the
            // byte array (without the padding for checksum) becomes the BLOB.
            Buffer.BlockCopy(valueArray, 0, blobData, 0, nBin);

            return blobData;
        }


        public static unsafe byte[] ConvertArrayToBlobIrregular(
                    int TimeStepCount, TimeSeriesValue[] dateValueArray )
        {
            // The number of bytes required for the BLOB
            int nBin = TimeStepCount * sizeof(TimeSeriesValue);
            // Allocate an array for the BLOB
            Byte[] blobData = new Byte[nBin];

            MemoryStream blobStream = new MemoryStream(blobData);
            BinaryWriter blobWriter = new BinaryWriter(blobStream);
            for (int i = 0; i < TimeStepCount; i++)
            {
                blobWriter.Write(dateValueArray[i].Date.ToBinary());
                blobWriter.Write(dateValueArray[i].Value);
            }
            return blobData;
        }

    }
}
